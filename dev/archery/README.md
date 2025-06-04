<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Developing with Archery

Archery is documented on the Arrow website:

* [Daily development using Archery](https://arrow.apache.org/docs/developers/continuous_integration/archery.html)
* [Using Archery and Crossbow](https://arrow.apache.org/docs/developers/continuous_integration/crossbow.html)
* [Using Archery and Docker](https://arrow.apache.org/docs/developers/continuous_integration/docker.html)

# Installing Archery

See the pages linked above for more details. As a general overview, Archery
comes in a number of subpackages, each needing to be installed if you want
to use the functionality of it:

* lint – lint (and in some cases auto-format) code in the Arrow repo
  To install: `pip install -e "arrow/dev/archery[lint]"`
* benchmark – to run Arrow benchmarks using Archery
  To install: `pip install -e "arrow/dev/archery[benchmark]"`
* docker – to run docker compose based tasks more easily
  To install: `pip install -e "arrow/dev/archery[docker]"`
* release – release related helpers
  To install: `pip install -e "arrow/dev/archery[release]"`
* crossbow – to trigger + interact with the crossbow build system
  To install: `pip install -e "arrow/dev/archery[crossbow]"`
* crossbow-upload
  To install: `pip install -e "arrow/dev/archery[crossbow-upload]"`

Additionally, if you would prefer to install everything at once,
`pip install -e "arrow/dev/archery[all]"` is an alias for all of
the above subpackages.

For some prior art on benchmarking in Arrow, see [this prototype](https://github.com/apache/arrow/tree/0409498819332fc479f8df38babe3426d707fb9e/dev/benchmarking).

# Usage
## Integration tests

Archery provides comprehensive integration testing capabilities for Apache Arrow implementations across different languages and formats. The integration tests verify compatibility between Arrow implementations by testing IPC (Inter-Process Communication), Flight, and C Data Interface protocols.

### Basic Usage

Run all integration tests with default settings:
```bash
archery integration
```

Run only specific test types:
```bash
# Run only IPC tests
archery integration --run-ipc

# Run only Flight tests  
archery integration --run-flight

# Run only C Data Interface tests
archery integration --run-c-data

# Run multiple test types
archery integration --run-ipc --run-flight
```

### Language Implementation Selection

Control which Arrow implementations are tested:

```bash
# Test only C++ and Java implementations
archery integration --with-cpp --with-java
```

### Test Filtering and Control

Filter tests by name pattern:
```bash
# Only run tests containing "primitive" in their name
archery integration --match primitive

# Run tests serially instead of in parallel
archery integration --serial

# Stop on first error
archery integration --stop-on-error
```

### Test Data and Scenarios

The integration tests use various data scenarios:

#### Generated Test Cases
- **Primitive types**: Basic Arrow types (int8, int32, float64, bool, etc.)
- **Nested types**: Lists, structs, maps, unions
- **Dictionary encoding**: Dictionary-encoded arrays
- **Decimal types**: High-precision decimal values (128-bit, 256-bit)
- **Date/Time types**: Timestamps, dates, times, intervals
- **Binary data**: Fixed-size binary, variable binary, string views
- **Extension types**: Custom extension type implementations

### Common Test Scenarios

#### Testing External Implementation
```bash
# Test new implementation against established ones
archery integration \
    --with-external-library /path/to/external/impl/ \
    --external-library-ipc-consumer \
    --external-library-c-data-schema-importer \
    --external-library-c-data-array-importer \
    --external-library-c-data-array-exporter \
    --external-library-c-data-schema-exporter \
    --external-library-supports-releasing-memory \
    --run-c-data \
    --run-ipc \
    --run-cpp
```
In this case, the external library path must contain the necessary shared libraries (e.g., `.so` or `.dll` files) for the C Data Interface tests. The name of the shared library should match the expected naming conventions for the Arrow C Data Interface: 'c_data_integration.[dll/so]'
And for executables:
- 'arrow-json-integration-test'
- 'arrow-stream-to-file'
- 'arrow-file-to-stream'
