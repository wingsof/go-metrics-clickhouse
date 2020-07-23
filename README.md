go-metrics-clickhouse
===================

This is a utility for the [go-metrics](https://github.com/rcrowley/go-metrics) library which will send the metrics to [ClickHouse](https://clickhouse.tech/).

Initial version of source code is forked from [go-metrics-influxdb](https://github.com/vrischmann/go-metrics-influxdb) and all of the influxdb related code was removed and re-programmed for ClickHouse database.
That's the reason why the behavior of module and API signature provided is very similar to that of go-metrics-influxdb.

This module supports align to interval feature (which means modification of reporting time, not actual sampling time) and tagging as go-metrics-influxdb does.

Note
----

This module uses [Clickhouse-go](https://github.com/ClickHouse/clickhouse-go) and [sqlx](https://github.com/jmoiron/sqlx) instead of native [sql](https://golang.org/pkg/database/sql/) package. And it means that supporting ClickHouse version might be depend on that of those two packages.

Tested ClickHouse server version:
  * 20.3.4

Usage
-----

```go
import "github.com/wingsof/go-metrics-clickhouse"

go clickhouse.ClickHouseWithTags(
    metrics.DefaultRegistry,    // metrics registry
    time.Second * 10,           // interval
    hostport,                   // your ClickHouse url (host:port). port should be TCP port (not http api port) ex) host1:9000
    database,                   // your ClickHouse database
    table,                      // your ClickHouse table
    user,                       // your ClickHouse user
    pass,                       // your ClickHouse password
    tags,                       // your tag set as map[string]string
    aligntimestamps             // align the timestamps
)
```

License
-------

go-metrics-clickhouse is licensed under the MIT license. See the LICENSE file for details.
