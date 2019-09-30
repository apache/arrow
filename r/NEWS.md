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

# arrow 0.15.0

## Breaking changes

* The R6 classes that wrap the C++ classes are now documented and exported and have been renamed to be more R-friendly. Users of the high-level R interface in this package are not affected. Those who want to interact with the Arrow C++ API more directly should work with these objects and methods. As part of this change, many functions that instantiated these R6 objects have been removed in favor of `Class$create()` methods. Notably, `arrow::array()` and `arrow::table()` have been removed in favor of `Array$create()` and `Table$create()`, eliminating the package startup message about masking `base` functions. For more information, see the new `vignette("arrow")`.
* Due to a subtle change in the Arrow message format, data written by the 0.15 version libraries may not be readable by older versions. If you need to send data to a process that uses an older version of Arrow (for example, an Apache Spark server that hasn't yet updated to Arrow 0.15), you can set the environment variable `ARROW_PRE_0_15_IPC_FORMAT=1`.
* The `as_tibble` argument in the `read_*()` functions has been renamed to `as_data_frame` ([ARROW-6337](https://issues.apache.org/jira/browse/ARROW-6337), @jameslamb)
* The `arrow::Column` class has been removed, as it was removed from the C++ library

## New features

* `Table` and `RecordBatch` objects have S3 methods that enable you to work with them more like `data.frame`s. Extract columns, subset, and so on. See `?Table` and `?RecordBatch` for examples.
* Initial implementation of bindings for the C++ File System API. ([ARROW-6348](https://issues.apache.org/jira/browse/ARROW-6348))
* Compressed streams are now supported on Windows ([ARROW-6360](https://issues.apache.org/jira/browse/ARROW-6360)), and you can also specify a compression level ([ARROW-6533](https://issues.apache.org/jira/browse/ARROW-6533))

## Other upgrades

* Parquet file reading is much, much faster, thanks to improvements in the Arrow C++ library.
* `read_csv_arrow()` supports more parsing options, including `col_names`, `na`, `quoted_na`, and `skip`
* `read_parquet()` and `read_feather()` can ingest data from a `raw` vector ([ARROW-6278](https://issues.apache.org/jira/browse/ARROW-6278))
* File readers now properly handle paths that need expanding, such as `~/file.parquet` ([ARROW-6323](https://issues.apache.org/jira/browse/ARROW-6323))
* Improved support for creating types in a schema: the types' printed names (e.g. "double") are guaranteed to be valid to use in instantiating a schema (e.g. `double()`), and time types can be created with human-friendly resolution strings ("ms", "s", etc.). ([ARROW-63378](https://issues.apache.org/jira/browse/ARROW-6338), [ARROW-6364](https://issues.apache.org/jira/browse/ARROW-6364))


# arrow 0.14.1

Initial CRAN release of the `arrow` package. Key features include:

* Read and write support for various file formats, including Parquet, Feather/Arrow, CSV, and JSON.
* API bindings to the C++ library for Arrow data types and objects, as well as mapping between Arrow types and R data types.
* Tools for helping with C++ library configuration and installation.
