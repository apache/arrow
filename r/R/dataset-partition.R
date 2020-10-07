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
#' "2019/02/file.parquet", etc.
#'
#' `HivePartitioning` is for Hive-style partitioning, which embeds field
#' names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". Because fields are named in the path
#' segments, order does not matter.
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
DirectoryPartitioning$create <- function(schema) {
  shared_ptr(DirectoryPartitioning, dataset___DirectoryPartitioning(schema))
}

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
HivePartitioning <- R6Class("HivePartitioning", inherit = Partitioning)
HivePartitioning$create <- function(schema) {
  shared_ptr(HivePartitioning, dataset___HivePartitioning(schema))
}

#' Construct Hive partitioning
#'
#' Hive partitioning embeds field names and values in path segments, such as
#' "/year=2019/month=2/data.parquet".
#'
#' Because fields are named in the path segments, order of fields passed to
#' `hive_partition()` does not matter.
#' @param ... named list of [data types][data-type], passed to [schema()]
#' @return A [HivePartitioning][Partitioning], or a `HivePartitioningFactory` if
#' calling `hive_partition()` with no arguments.
#' @examples
#' \donttest{
#' hive_partition(year = int16(), month = int8())
#' }
#' @export
hive_partition <- function(...) {
  schm <- schema(...)
  if (length(schm) == 0) {
    HivePartitioningFactory$create()
  } else {
    HivePartitioning$create(schm)
  }
}

PartitioningFactory <- R6Class("PartitioningFactory", inherit = ArrowObject)

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
DirectoryPartitioningFactory <- R6Class("DirectoryPartitioningFactory ", inherit = PartitioningFactory)
DirectoryPartitioningFactory$create <- function(x) {
  shared_ptr(DirectoryPartitioningFactory, dataset___DirectoryPartitioning__MakeFactory(x))
}

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
HivePartitioningFactory <- R6Class("HivePartitioningFactory", inherit = PartitioningFactory)
HivePartitioningFactory$create <- function() {
  shared_ptr(HivePartitioningFactory, dataset___HivePartitioning__MakeFactory())
}
