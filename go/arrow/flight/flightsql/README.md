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
* [Backends](#backends)
  * [SQLite](#sqlite)
  * [InfluxData IOx](#influxdata-iox)

---------------------------------------

## Prerequisits

* Go 1.19+
* Installation via `go get -u github.com/apache/arrow/go/v12/arrow/flight/flightsql`
* One of the [supported backends](#backends)

---------------------------------------

## Usage

_Go FlightQL Driver_ is an implementation of Go's `database/sql/driver`
interface to use the [`database/sql`](https://golang.org/pkg/database/sql/)
framework. The driver is registered as `flightsql` and configured using a
[data-source name (DSN)](#data-source-name-dsn).
Different backends might need specific formats or options, see the
[Backends section](#backends) for details.

A basic example using a SQLite backend looks like this

```go
import (
    "database/sql"
    "time"

    _ "github.com/apache/arrow/go/v12/arrow/flight/flightsql"
)

// Open the connection to an SQLite backend
db, err := sql.Open("flightsql", "sqlite://127.0.0.1:12345?timeout=5s")
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
<backend>://<backend-specific address>[?param1=value1&...&paramN=valueN]
```

The mandatory `backend` and the `backend-specific address` settings are
explained in detail [below](#backends). Additionally, optional parameters such
as timeouts can be passed to the driver. Parameters common to all backends are
listed [here](#common-parameters).

**Please note**: All parameters are case-sensitive!

Alternatively to specifying the DSN directly, the `DriverConfig` structure can
be used to derive a DSN string.

### Common parameters

The following common parameters exist

#### `timeout`

The `timeout` parameter can be set using a duration string e.g. `timeout=5s`
to limit the maximum time an operation can take. This prevents calls that wait
forever, e.g. if the backend is down or a query is taking very long. When
not set, the driver will use an _infinite_ timeout.

## Backends

This section describes the backend-specific part of the DSN. Some parts
might overlap between backends but might carry different meanings. Please
always check the documentation of the backend you are planning to use.

### SQLite

A DSN for a [SQLite backend][SQLite] has to follow the pattern

```text
sqlite://address[:port][?param1=value1&...&paramN=valueN]
```

The `backend` is `sqlite` followed by a `host[:port]` address. This backend
does not support authentication-credentials nor specifying a database.

[SQLite]: https://www.sqlite.org/

### InfluxData IOx

A DSN for an [InfluxData IOx backend][IOx] has to follow the pattern

```text
iox[s]://[token@]address[:port]/bucket[?param1=value1&...&paramN=valueN]
```

The `backend` can be `iox` for non-TLS endpoints or `ioxs` for TLS connections.
To authenticate to IOx, specify a `token` valid to access the database. This
backend does not username/password authentication.

The `host[:port]` address of the IOx instance has to be followed by the
`bucket` name.

[IOx]: https://github.com/influxdata/influxdb_iox
