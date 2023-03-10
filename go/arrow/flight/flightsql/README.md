# Go-FlightSQL-Driver

A FlightSQL-Driver for Go's [database/sql](https://golang.org/pkg/database/sql/)
package. This driver is a lightweight wrapper around the FlightSQL client in
pure Go. It provides all advantages of a `database/sql` driver like automatic
connection pooling, transactions combined with ease of use (see (#usage)).

---------------------------------------

* [Prerequisits](#prerequisits)
* [Usage](#usage)
* [Data Source Name (DSN)](#data-source-name-dsn)
* [Common parameters](#common-parameters)
* [Backend specific settings](#backend-specific-settings)
  * [SQLite](#sqlite)
  * [InfluxData IOx](#influxdata-iox)
* [Driver config usage](#driver-config-usage)
* [TLS setup](#tls-setup)

---------------------------------------

## Prerequisits

* Go 1.19+
* Installation via `go get -u github.com/apache/arrow/go/v12/arrow/flight/flightsql`
* Backend speaking FlightSQL

---------------------------------------

## Usage

_Go FlightQL Driver_ is an implementation of Go's `database/sql/driver`
interface to use the [`database/sql`](https://golang.org/pkg/database/sql/)
framework. The driver is registered as `flightsql` and configured using a
[data-source name (DSN)](#data-source-name-dsn).
Different backends might require specific parameters, check the
[Backends section](#backend-specific-settings) for known requirements.

A basic example using a SQLite backend looks like this

```go
import (
    "database/sql"
    "time"

    _ "github.com/apache/arrow/go/v12/arrow/flight/flightsql"
)

// Open the connection to an SQLite backend
db, err := sql.Open("flightsql", "flightsql://127.0.0.1:12345?timeout=5s")
if err != nil {
    panic(err)
}
// Make sure we close the connection to the database
defer db.Close()

// Use the connection e.g. for querying
rows, err := db.Query("SELECT * FROM mytable")
if err != nil {
    panic(err)
}
// ...
```

## Data Source Name (DSN)

A Data Source Name has the following format:

```text
flightsql://[user[:password]@]<address>[:port][?param1=value1&...&paramN=valueN]
```

The data-source-name (DSN) requires the `address` of the backend with an
optional port setting. The `user` and `password` parameters are passed to the
backend as GRPC Basic-Auth headers. If your backend requires a token based
authentication, please use a `token` parameter (see
[common parameters](#common-parameters) below).

**Please note**: All parameters are case-sensitive!

Alternatively to specifying the DSN directly you can use the `DriverConfig`
structure to generate the DSN string. See the
[Driver config usage section](#driver-config-usage) for details.

### Common parameters

The following common parameters exist

#### `token`

The `token` parameter can be used to specify the token for token-based
authentication. The value is passed on to the backend as a GRPC Bearer-Auth
header.

#### `timeout`

The `timeout` parameter can be set using a duration string e.g. `timeout=5s`
to limit the maximum time an operation can take. This prevents calls that wait
forever, e.g. if the backend is down or a query is taking very long. When
not set, the driver will use an _infinite_ timeout.

## Backend specific settings

This section describes the backend-specific part of the DSN. Some parts
might overlap between backends but might carry different meanings. Please
always check the documentation of the backend you are planning to use.

### SQLite

[SQLite] only requires the specification of the address (and port). No
additional parameters are required. However, it is recommended to specify a
`timeout`.

[SQLite]: https://www.sqlite.org/

### InfluxData IOx

[InfluxDB IOx][IOx] requires a `token` and a namespace to be specified as DSN
parameters.

* `token`: your Influx access token
* `iox-namespace-name`: IOX namespace to access

It is recommended to additionally specify a `timeout`.

[IOx]: https://github.com/influxdata/influxdb_iox

## Driver config usage

Alternatively to specifying the DSN directly you can fill the `DriverConfig`
structure and generate the DSN out of this. Here is some example code to use
for the [InfluxData IOx backend](#influxdata-iox):

```golang
package main

import (
    "database/sql"
    "log"
    "time"

    "github.com/apache/arrow/go/v12/arrow/flight/flightsql"
)

func main() {
    config := flightsql.DriverConfig{
        Address: "localhost:8082",
        Token:   "your token",
        Timeout: 10 * time.Second,
        Params: map[string]string{
            "iox-namespace-name": "company_sensors",
        },
    }
    db, err := sql.Open("flightsql", config.DSN())
    if err != nil {
        log.Fatalf("open failed: %v", err)
    }
    defer db.Close()

    ...
}
```

## TLS setup

Currently TLS is not yet supported and will be added later.
