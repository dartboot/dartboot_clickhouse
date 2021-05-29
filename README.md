The Clickhouse library for DartBoot cloud.

## Usage

A simple usage example:

```dart
import 'package:dartboot_clickhouse/dartboot_clickhouse.dart';

@dba
void main() {
  DartBoot.run();
}

@bean
class Test {
  Test() {
    final client = ClickHouseClient();
    client.findOne('select * from mytable').then((value) => print(value));
  }
}
```
