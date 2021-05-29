import 'dart:async';
import 'dart:convert';
import 'package:dartboot_annotation/dartboot_annotation.dart';
import 'package:dartboot_core/bootstrap/application_context.dart';
import 'package:dartboot_core/log/logger.dart';
import 'package:dartboot_core/error/custom_error.dart';
import 'package:dartboot_db/dartboot_db.dart';
import 'package:dartboot_util/dartboot_util.dart';
import 'package:dio/dio.dart';
import 'package:http/http.dart' as http;
import 'package:pool/pool.dart';

/// 测试连接的sql
const testConnectionSql = 'select 1';

/// Clickhouse的数据库客户端
///
/// @author luodongseu
@Bean(conditionOnProperty: 'database.clickhouse', name: 'chClient')
class ClickHouseClient {
  static Log logger = Log('ClickHouseDataBase');

  /// 主机IP
  String _host;

  /// 端口号
  int _port;

  /// 数据库
  String _db;

  /// 用户名
  String _username = 'default';

  /// 密码
  String _password = '';

  /// clickhouse的http接口url
  String _chHttpUrl;

  /// http客户端
  static Dio _httpClient;

  /// 全局唯一实例
  static ClickHouseClient _instance;

  /// 请求池
  Pool _requestPool;

  ClickHouseClient() {
    _instance = this;

    // 配置信息
    dynamic clickhouseConfig =
        ApplicationContext.instance['database.clickhouse'];
    _host = clickhouseConfig['host'] ?? '127.0.0.1';
    _port = int.parse('${clickhouseConfig['port'] ?? 8123}');
    _db = clickhouseConfig['db'] ?? 'default';
    _username = clickhouseConfig['username'] ?? 'default';
    _password = clickhouseConfig['password'] ?? '';
    _chHttpUrl =
        'http://$_host:$_port/?database=$_db&user=$_username&password=$_password'
        '&max_execution_time=600000&timeout_overflow_mode=throw'
        '&max_result_rows=1000&result_overflow_mode=throw';

    // 连接池大小
    final maxPoolSize = int.parse(
        '${ApplicationContext.instance['database.clickhouse.max-pool-size'] ?? 10}');
    _requestPool = Pool(maxPoolSize, timeout: Duration(seconds: 30));

    logger.info(
        'Start to connect clickhouse client to server:[$_host:$_port]...');

    _httpClient = Dio(BaseOptions(
        contentType: 'application/json;charset=UTF-8',
        connectTimeout: 30000,
        receiveTimeout: 600000,
        sendTimeout: 600000));
    _httpClient.interceptors.add(InterceptorsWrapper(onError: (e) {
      logger.error('Run sql failed [${e}].', e.response);
      return null;
    }));

    _instance.execute(testConnectionSql).then((v) {
      logger.info(
          'Clickhouse client connected to server:[$_host:$_port] success.');
    });
  }

  static ClickHouseClient get instance => _instance;

  /// 分页查找
  /// @skipEmptyAndZero 是否过滤空和0
  Future<PageImpl<R>> findPage<R>(String sql, PageRequest pageRequest) async {
    assert(null != sql, 'Sql must not be null.');
    assert(null != pageRequest, 'PageRequest must not be null.');
    assert(sql.toLowerCase().startsWith(RegExp('(\\s)*select')),
        'Only support select query.');

    String _sql = ModelUtils.formatSql(sql);
    PageImpl<dynamic> results = await execute(_sql, pageRequest);
    return R != dynamic
        ? PageImpl<R>(ModelUtils.resultsMapper<R>(results.content),
            results.page, results.pageSize, results.total)
        : results;
  }

  /// 查找单个
  Future<R> findOne<R>(String sql) async {
    assert(null != sql, 'Sql must not be null.');
    assert(sql.toLowerCase().startsWith(RegExp('(\\s)*select')),
        'Only support select query.');

    String _sql = ModelUtils.formatSql(sql);
    dynamic results =
        await execute('$_sql ${_sql.endsWith('limit 1') ? '' : ' limit 1'}');
    if (isEmpty(results)) {
      return null;
    }
    dynamic _r = results is List ? results[0] : results;
    return R != dynamic ? ModelUtils.resultMapper<R>(_r) : _r;
  }

  /// 查找全部
  Future<List<R>> findAll<R>(String sql) async {
    assert(null != sql, 'Sql must not be null.');
    assert(sql.toLowerCase().startsWith(RegExp('(\\s)*select')),
        'Only support select query.');

    String _sql = ModelUtils.formatSql(sql);
    dynamic results = await execute('$_sql');
    if (isEmpty(results)) {
      return [];
    }
    dynamic _r = results is List ? results : [results];
    return R != dynamic ? ModelUtils.resultsMapper<R>(_r) : _r;
  }

  /// 执行且不打印日志
  Future<dynamic> executeWithoutLog(String sql,
      {PageRequest pageRequest}) async {
    await _execute(sql, pageRequest: pageRequest, ifPrintLog: false);
  }

  /// 内部执行
  Future<dynamic> _execute(String sql,
      {PageRequest pageRequest, bool ifPrintLog = true}) async {
    // 记录时间
    final _sm = DateTime.now().millisecondsSinceEpoch;

    var result;

    // 过滤多余的空字符
    String _sql = ModelUtils.formatSql(sql);
    final isSelectSql = _sql.toLowerCase().startsWith(RegExp('(\\s)*select'));
    var limit = '';
    if (null != pageRequest) {
      assert(pageRequest.limit > 0, 'Page size must bigger than zero.');
      assert(pageRequest.page >= 0,
          'Page index must bigger than or equal to zero.');
      limit = 'limit ${pageRequest.offset},${pageRequest.limit}';
    }
    String finalSql = ModelUtils.formatSql('$_sql $limit');
    try {
      var requests = <Future>[
        _getResponse('$finalSql',
            acceptJson: isSelectSql, ifPrintLog: ifPrintLog),
      ];
      if (isSelectSql && null != pageRequest) {
        requests.add(countSubQuery(sql, ifPrintLog: ifPrintLog));
      }
      // 并发执行sql
      var res = await Future.wait(requests);
      dynamic _data(Response _res) {
        if (isEmpty(_res.data)) {
          return null;
        }
        if (_res.statusCode >= 400) {
          throw CustomError(
              '${_res.statusCode}: ${_res.data ?? 'DB exception'}');
        }
        return _res.data['data'];
      }

      if (res.length == 2) {
        // 分页查询
        result = PageImpl(
            _data(res[0]), pageRequest.page, pageRequest.limit, res[1]);
      } else {
        result = _data(res[0]);
      }
    } on CustomError {
      rethrow;
    } catch (e) {
      final _em = DateTime.now().millisecondsSinceEpoch;
      logger.error(
          "Ch sql [${finalSql.length > 100 ? '${finalSql.substring(0, 100)}...' : finalSql}] response error in [${_em - _sm}] mill secs.",
          e);
      throw CustomError(e);
    }

    // 打印时间和状态
    final _em = DateTime.now().millisecondsSinceEpoch;
    if (ifPrintLog) {
      logger.debug('Ch sql [$finalSql] -> response in [${_em - _sm}] millsecs');
    }
    return result;
  }

  /// 执行SQL语句
  ///
  /// [_sql] 为原始的sql语句，该方法会删除sql语句中多余的空字符:
  /// 1. 开头的所有空字符会被删除
  /// 2. 结尾的所有空字符会被删除
  /// 3. sql中间的连续2个以上的空字符会被替换只留1个空字符
  ///
  /// [pageRequest] 为分页信息
  /// 不传[pageRequest]表示不分页
  /// 传了[pageRequest]会返回Pageable对象
  Future<dynamic> execute(String sql, [PageRequest pageRequest]) async {
    return _execute(sql, pageRequest: pageRequest);
  }

  /// 执行单个sql，获取响应结果并记录时间
  Future<Response> _getResponse(String sql,
      {bool acceptJson = false, bool ifPrintLog = true}) async {
    // 请求连接池
    final _rqs = DateTime.now().millisecondsSinceEpoch;
    final resource = await _requestPool.request();
    logger.debug(
        'Sql [$sql] required request task in pool successful in ${DateTime.now().millisecondsSinceEpoch - _rqs} mills.');

    try {
      final printSql =
          '${ApplicationContext.instance['database.clickhouse.print-sql']}' ==
              'true';
      final isTestSql = sql == '$testConnectionSql format JSON';
      if (ifPrintLog) {
        if (printSql && !isTestSql) {
          logger.info('Ch start run sql -> $sql ...');
        } else {
          logger.debug('Ch start run sql -> $sql ...');
        }
      }
      final _sm = DateTime.now().millisecondsSinceEpoch;
      final httpResult = await http.post(_chHttpUrl,
          body: '$sql${acceptJson ? ' format JSON' : ''}',
          headers: {
            'Content-type': 'text/plain; charset=utf-8',
            'Accept': acceptJson
                ? 'application/json; charset=utf-8'
                : 'text/plain; charset=utf-8'
          });
      assert(httpResult != null, 'Http post response empty');
      final response = Response(
          statusCode: httpResult.statusCode,
          data: acceptJson ? json.decode(httpResult.body) : httpResult.body);
      final _em = DateTime.now().millisecondsSinceEpoch;
      if (ifPrintLog) {
        if (printSql && !isTestSql) {
          logger.info('Ch sql -> $sql finished in [${_em - _sm}] millsecs '
              'and response status: [${response.statusCode} '
              '${response.statusMessage}]');
        } else {
          logger.debug('Ch sql -> $sql finished in [${_em - _sm}] millsecs '
              'and response status: [${response.statusCode} '
              '${response.statusMessage}]');
        }
      }
      return response;
    } finally {
      // 释放连接池
      resource.release();
    }
  }

  /// 统计sql查询的总记录数
  ///
  /// 返回int -> 总数
  Future<int> countSubQuery(String fromSql, {bool ifPrintLog = true}) async {
    var countSql = 'select count(*) from ($fromSql) as _t_$uid4';
    if (ifPrintLog) logger.debug('Count sql -> $countSql');
    var countResponse = await _getResponse(countSql);
    if (ifPrintLog) {
      logger.debug('Ch count sql [$countSql] response -> $countResponse');
    }
    return int.parse('${countResponse.data ?? '0'}');
  }

  /// 统计sql查询的总记录数
  ///
  /// 返回int -> 总数
  Future<int> count(String fromSql) async {
    assert(isNotEmpty(fromSql), 'Sql must not be null');

    if (!(ModelUtils.formatSql(fromSql)
        .toUpperCase()
        .startsWith('SELECT COUNT('))) {
      return countSubQuery(fromSql);
    }

    logger.debug('Count sql -> $fromSql');
    var countResponse = await _getResponse(fromSql);
    logger.debug('Ch count sql [$fromSql] response -> $countResponse');
    return int.parse('${countResponse.data ?? '0'}');
  }
}
