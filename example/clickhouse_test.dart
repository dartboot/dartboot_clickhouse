import 'package:dartboot_annotation/dartboot_annotation.dart';
import 'package:dartboot_clickhouse/dartboot_clickhouse.dart';

/// A simple test use case for DB tools.

/// Clickhouse client example
@Bean()
class Test {

  Test() {
    print('..........');
     final client = ClickHouseClient();
     client.findOne('select * from t_page_event').then((value) => print(value));
  }
}
