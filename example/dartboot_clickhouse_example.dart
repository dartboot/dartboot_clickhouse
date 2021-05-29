import 'package:dartboot_core/dartboot_core.dart';
import 'package:dartboot_annotation/annotation.dart';
import './dartboot_clickhouse_example.g.dart';

// final client = ClickHouseClient();
// client.findOne('select * from t_page_event').then((value) => print(value));

@dba
void main() {
  DartBoot.run();
}
