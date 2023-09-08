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

Apache Arrow for Go
===================

[![GoDoc](https://godoc.org/github.com/apache/arrow/go/arrow?status.svg)](https://godoc.org/github.com/apache/arrow/go/arrow)

[Apache Arrow][arrow] is a cross-language development platform for in-memory
data. It specifies a standardized language-independent columnar memory format
for flat and hierarchical data, organized for efficient analytic operations on
modern hardware. It also provides computational libraries and zero-copy
streaming messaging and inter-process communication.

### A note about FlightSQL drivers

Go FlightSQL drivers live in the
[ADBC repository](https://github.com/apache/arrow-adbc/tree/main/go/adbc).
In particular, to use the Golang `database/sql` interface:
```golang
import (
    "database/sql"
    _ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
)

func main() {
	dsn := "uri=grpc://localhost:12345;username=mickeymouse;password=p@55w0RD"
    db, err := sql.Open("flightsql", dsn)
    ...
}
```

DSN option keys are expressed as `k=v`, delimited with `;`. 
Some options keys are defined in ADBC, others are defined in the FlightSQL ADBC driver.
- Arrow ADBC [developer doc](https://arrow.apache.org/adbc/main/driver/go/flight_sql.html#client-options)
- ADBC [source code](https://github.com/apache/arrow-adbc/blob/3d12fad1bae21029a8ff25604d6e65760c3f65bd/go/adbc/adbc.go#L149-L158)
- FlightSQL driver option keys [source code](https://github.com/apache/arrow-adbc/blob/3d12fad1bae21029a8ff25604d6e65760c3f65bd/go/adbc/driver/flightsql/flightsql_adbc.go#L70-L81)

Reference Counting
------------------

The library makes use of reference counting so that it can track when memory
buffers are no longer used. This allows Arrow to update resource accounting,
pool memory such and track overall memory usage as objects are created and
released. Types expose two methods to deal with this pattern. The `Retain`
method will increase the reference count by 1 and `Release` method will reduce
the count by 1. Once the reference count of an object is zero, any associated
object will be freed. `Retain` and `Release` are safe to call from multiple
goroutines.

### When to call `Retain` / `Release`?

* If you are passed an object and wish to take ownership of it, you must call
  `Retain`. You must later pair this with a call to `Release` when you no
  longer need the object.  "Taking ownership" typically means you wish to
  access the object outside the scope of the current function call.

* You own any object you create via functions whose name begins with `New` or
  `Copy` or when receiving an object over a channel. Therefore you must call
  `Release` once you no longer need the object.

* If you send an object over a channel, you must call `Retain` before sending
  it as the receiver is assumed to own the object and will later call `Release`
  when it no longer needs the object.

Performance
-----------

The arrow package makes extensive use of [c2goasm][] to leverage LLVM's
advanced optimizer and generate PLAN9 assembly functions from C/C++ code. The
arrow package can be compiled without these optimizations using the `noasm`
build tag. Alternatively, by configuring an environment variable, it is
possible to dynamically configure which architecture optimizations are used at
runtime.  See the `cpu` package [README](arrow/internal/cpu/README.md) for a
description of this environment variable.

### Example Usage

The following benchmarks demonstrate summing an array of 8192 values using
various optimizations.

Disable no architecture optimizations (thus using AVX2):

```sh
$ INTEL_DISABLE_EXT=NONE go test -bench=8192 -run=. ./math
goos: darwin
goarch: amd64
pkg: github.com/apache/arrow/go/arrow/math
BenchmarkFloat64Funcs_Sum_8192-8   	 2000000	       687 ns/op	95375.41 MB/s
BenchmarkInt64Funcs_Sum_8192-8     	 2000000	       719 ns/op	91061.06 MB/s
BenchmarkUint64Funcs_Sum_8192-8    	 2000000	       691 ns/op	94797.29 MB/s
PASS
ok  	github.com/apache/arrow/go/arrow/math	6.444s
```

**NOTE:** `NONE` is simply ignored, thus enabling optimizations for AVX2 and SSE4

----

Disable AVX2 architecture optimizations:

```sh
$ INTEL_DISABLE_EXT=AVX2 go test -bench=8192 -run=. ./math
goos: darwin
goarch: amd64
pkg: github.com/apache/arrow/go/arrow/math
BenchmarkFloat64Funcs_Sum_8192-8   	 1000000	      1912 ns/op	34263.63 MB/s
BenchmarkInt64Funcs_Sum_8192-8     	 1000000	      1392 ns/op	47065.57 MB/s
BenchmarkUint64Funcs_Sum_8192-8    	 1000000	      1405 ns/op	46636.41 MB/s
PASS
ok  	github.com/apache/arrow/go/arrow/math	4.786s
```

----

Disable ALL architecture optimizations, thus using pure Go implementation:

```sh
$ INTEL_DISABLE_EXT=ALL go test -bench=8192 -run=. ./math
goos: darwin
goarch: amd64
pkg: github.com/apache/arrow/go/arrow/math
BenchmarkFloat64Funcs_Sum_8192-8   	  200000	     10285 ns/op	6371.41 MB/s
BenchmarkInt64Funcs_Sum_8192-8     	  500000	      3892 ns/op	16837.37 MB/s
BenchmarkUint64Funcs_Sum_8192-8    	  500000	      3929 ns/op	16680.00 MB/s
PASS
ok  	github.com/apache/arrow/go/arrow/math	6.179s
```

[arrow]:    https://arrow.apache.org
[c2goasm]:  https://github.com/minio/c2goasm
