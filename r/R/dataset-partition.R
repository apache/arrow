# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#' Define Partitioning for a Dataset
#'
#' @description
#' Pass a `Partitioning` object to a [FileSystemDatasetFactory]'s `$create()`
#' method to indicate how the file's paths should be interpreted to define
#' partitioning.
#'
#' `DirectoryPartitioning` describes how to interpret raw path segments, in
#' order. For example, `schema(year = int16(), month = int8())` would define
#' partitions for file paths like "2019/01/file.parquet",
#' "2019/02/file.parquet", etc. In this scheme `NULL` values will be skipped. In
#' the previous example: when writing a dataset if the month was `NA` (or
#' `NULL`), the files would be placed in "2019/file.parquet". When reading, the
#' rows in "2019/file.parquet" would return an `NA` for the month column. An
#' error will be raised if an outer directory is `NULL` and an inner directory
#' is not.
#'
#' `HivePartitioning` is for Hive-style partitioning, which embeds field
#' names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". Because fields are named in the path
#' segments, order does not matter. This partitioning scheme allows `NULL`
#' values. They will be replaced by a configurable `null_fallback` which
#' defaults to the string `"__HIVE_DEFAULT_PARTITION__"` when writing. When
#' reading, the `null_fallback` string will be replaced with `NA`s as
#' appropriate.
#'
#' `PartitioningFactory` subclasses instruct the `DatasetFactory` to detect
#' partition features from the file paths.
#' @section Factory:
#' Both `DirectoryPartitioning$create()` and `HivePartitioning$create()`
#' methods take a [Schema] as a single input argument. The helper
#' function [`hive_partition(...)`][hive_partition] is shorthand for
#' `HivePartitioning$create(schema(...))`.
#'
#' With `DirectoryPartitioningFactory$create()`, you can provide just the
#' names of the path segments (in our example, `c("year", "month")`), and
#' the `DatasetFactory` will infer the data types for those partition variables.
#' `HivePartitioningFactory$create()` takes no arguments: both variable names
#' and their types can be inferred from the file paths. `hive_partition()` with
#' no arguments returns a `HivePartitioningFactory`.
#' @name Partitioning
#' @rdname Partitioning
#' @export
Partitioning <- R6Class("Partitioning", inherit = ArrowObject)
#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
DirectoryPartitioning <- R6Class("DirectoryPartitioning", inherit = Partitioning)
DirectoryPartitioning$create <- function(schm, segment_encoding = "uri") {
  dataset___DirectoryPartitioning(schm, segment_encoding = segment_encoding)
}

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
HivePartitioning <- R6Class("HivePartitioning", inherit = Partitioning)
HivePartitioning$create <- function(schm, null_fallback = NULL, segment_encoding = "uri") {
  dataset___HivePartitioning(schm,
    null_fallback = null_fallback_or_default(null_fallback),
    segment_encoding = segment_encoding
  )
}

#' Construct Hive partitioning
#'
#' Hive partitioning embeds field names and values in path segments, such as
#' "/year=2019/month=2/data.parquet".
#'
#' Because fields are named in the path segments, order of fields passed to
#' `hive_partition()` does not matter.
#' @param ... named list of [data types][data-type], passed to [schema()]
#' @param null_fallback character to be used in place of missing values (`NA` or `NULL`)
#' in partition columns. Default is `"__HIVE_DEFAULT_PARTITION__"`,
#' which is what Hive uses.
#' @param segment_encoding Decode partition segments after splitting paths.
#' Default is `"uri"` (URI-decode segments). May also be `"none"` (leave as-is).
#' @return A [HivePartitioning][Partitioning], or a `HivePartitioningFactory` if
#' calling `hive_partition()` with no arguments.
#' @examplesIf arrow_with_dataset()
#' hive_partition(year = int16(), month = int8())
#' @export
hive_partition <- function(..., null_fallback = NULL, segment_encoding = "uri") {
  schm <- schema(...)
  if (length(schm) == 0) {
    HivePartitioningFactory$create(null_fallback, segment_encoding)
  } else {
    HivePartitioning$create(schm, null_fallback, segment_encoding)
  }
}

PartitioningFactory <- R6Class("PartitioningFactory",
  inherit = ArrowObject,
  public = list(
    Inspect = function(paths) dataset___PartitioningFactory__Inspect(self, paths),
    Finish = function(schema) dataset___PartitioningFactory__Finish(self, schema)
  ),
  active = list(
    type_name = function() dataset___PartitioningFactory__type_name(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
DirectoryPartitioningFactory <- R6Class("DirectoryPartitioningFactory ", inherit = PartitioningFactory)
DirectoryPartitioningFactory$create <- function(field_names, segment_encoding = "uri") {
  dataset___DirectoryPartitioning__MakeFactory(field_names, segment_encoding)
}

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
HivePartitioningFactory <- R6Class("HivePartitioningFactory", inherit = PartitioningFactory)
HivePartitioningFactory$create <- function(null_fallback = NULL, segment_encoding = "uri") {
  dataset___HivePartitioning__MakeFactory(null_fallback_or_default(null_fallback), segment_encoding)
}

null_fallback_or_default <- function(null_fallback) {
  null_fallback %||% "__HIVE_DEFAULT_PARTITION__"
}
