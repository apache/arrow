<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# FlightSQL driver

A FlightSQL-Driver for Go's [database/sql](https://golang.org/pkg/database/sql/)
package. This driver is a lightweight wrapper around the FlightSQL client in
pure Go. It provides all advantages of a `database/sql` driver like automatic
connection pooling, transactions combined with ease of use (see (#usage)).

---------------------------------------

* [Prerequisits](#prerequisits)
* [Usage](#usage)
* [Data Source Name (DSN)](#data-source-name-dsn)
* [Driver config usage](#driver-config-usage)
* [TLS setup](#tls-setup)

---------------------------------------

## Prerequisites

* Go 1.17+
* Installation via `go get -u github.com/apache/arrow/go/v12/arrow/flight/flightsql`
* Backend speaking FlightSQL

---------------------------------------

## Usage

_Go FlightQL Driver_ is an implementation of Go's `database/sql/driver`
interface to use the [`database/sql`](https://golang.org/pkg/database/sql/)
framework. The driver is registered as `flightsql` and configured using a
[data-source name (DSN)](#data-source-name-dsn).

A basic example using a SQLite backend looks like this

```go
import (
    "database/sql"
    "time"

    _ "github.com/apache/arrow/go/v12/arrow/flight/flightsql"
)

// Open the connection to an SQLite backend
db, err := sql.Open("flightsql", "flightsql://localhost:12345?timeout=5s")
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

#### `tls`

The `tls` parameter allows to enable and customize Transport-Layer-Security
settings. There are some special values for the parameters:

* `disabled` or `false` will disable TLS for this server connection. In this
  case all other settings are ignored.
* `enabled` or `true` will force TLS for this server connection. In this case
  the system settings for trusted CAs etc will be used.
* `skip-verify` will enable TLS for this server connection but will not verify
  the server certificate. **This is a security risk and should not be used!**

Any other value will be interpreted as the name of a custom configuration. Those
configurations must be registered either by
[creating the DSN from configuration](#driver-config-usage) or by calling
`RegisterTLSConfig()` (see [TLS setup](#tls-setup) for details).

## Driver config usage

Alternatively to specifying the DSN directly you can fill the `DriverConfig`
structure and generate the DSN out of this. Here is some example

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
        Address: "localhost:12345",
        Token:   "your token",
        Timeout: 10 * time.Second,
        Params: map[string]string{
            "my-custom-parameter": "foobar",
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

By specifying the [`tls` parameter](#tls) you can enable
Transport-Layer-Security. Using `tls=enabled` the system settings are used for
verifying the server's certificate. Custom TLS configurations, e.g. when using
self-signed certificates, are referenced by a user-selected name. The underlying
TLS configuration needs to be registered (using the same name) in two ways.

### TLS setup using `DriverConfig`

The first way is to create a `DriverConfig` with the `TLSConfig` field set to
the custom config and `TLSConfigName` set to the chosen name. For example

```golang
    ...

    config := flightsql.DriverConfig{
        Address: "localhost:12345",
        TLSEnabled:    true,
        TLSConfigName: "myconfig",
        TLSConfig: &tls.Config{
            MinVersion: tls.VersionTLS12,
        },
    }
    dsn := config.DSN()

    ...
```

will enable TLS forcing the minimum TLS version to 1.2. This custom config will
be registered with the name `myconfig` and the resulting DSN reads

```text
flightsql://localhost:12345?tls=myconfig`
```

If the `TLSConfigName` is omitted a random unique name (UUID) is generated and
referenced in the DSN. This prevents errors from using an already registered
name leading to errors.

### TLS setup using manual registration

The second alternative is the manual registration of the custom TLS
configuration. In this case you need to call `RegisterTLSConfig()` in your code

```golang
    myconfig := &tls.Config{MinVersion: tls.VersionTLS12}
    if err := flightsql.RegisterTLSConfig("myconfig", myconfig); err != nil {
        ...
    }
    dsn := "flightsql://localhost:12345?tls=myconfig"

    ...
```

This will register the custom configuration, constraining the minimim TLS
version, as `myconfig` and then references the registered configuration by
name in the DSN. You can reuse the same TLS configuration by registering once
and then reference in multiple DSNs. Registering multiple configurations with
the same name will throw an error to prevent unintended side-effects due to the
driver-global registry.
