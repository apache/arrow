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

#' @include arrow-package.R

open_dataset <- function (path, schema = NULL, partition = NULL, ...) {
  dsd <- DataSourceDiscovery$create(path, ...)
  if (is.null(schema)) {
    schema <- dsd$Inspect()
  }
  if (!is.null(partition)) {
    dsd$SetPartitionScheme(partition)
  }
  Dataset$create(list(dsd$Finish()), schema)
}

#' @export
Dataset <- R6Class("Dataset", inherit = Object,
  public = list(
    NewScan = function() unique_ptr(ScannerBuilder, dataset___Dataset__NewScan(self))
  ),
  active = list(
    schema = function() shared_ptr(Schema, dataset___Dataset__schema(self))
  )
)
Dataset$create <- function(sources, schema) {
  assert_is_list_of(sources, "DataSource")
  assert_is(schema, "Schema")
  shared_ptr(Dataset, dataset___Dataset__create(sources, schema))
}

#' @export
names.Dataset <- function(x) names(x$schema)

#' @export
DataSource <- R6Class("DataSource", inherit = Object)

#' @export
DataSourceDiscovery <- R6Class("DataSourceDiscovery", inherit = Object,
  public = list(
    Finish = function() shared_ptr(DataSource, dataset___DSDiscovery__Finish(self)),
    SetPartitionScheme = function(schema) {
      assert_is(schema, "Schema")
      dataset___DSDiscovery__SetPartitionScheme(self, schema)
      self
    },
    Inspect = function() shared_ptr(Schema, dataset___DSDiscovery__Inspect(self))
  )
)
DataSourceDiscovery$create <- function(path,
                                       filesystem = c("auto", "local"),
                                       format = c("parquet"),
                                       allow_non_existent = FALSE,
                                       recursive = TRUE,
                                       ...) {
  if (!inherits(filesystem, "FileSystem")) {
    filesystem <- match.arg(filesystem)
    if (filesystem == "auto") {
      # When there are other FileSystems supported, detect e.g. S3 from path
      filesystem <- "local"
    }
    filesystem <- list(
      local = LocalFileSystem
      # We'll register other file systems here
    )[[filesystem]]$create(...)
  }
  selector <- Selector$create(path, allow_non_existent = allow_non_existent, recursive = recursive)
  # This may also require different initializers
  FileSystemDataSourceDiscovery$create(filesystem, selector, format)
}

#' @export
FileSystemDataSourceDiscovery <- R6Class("FileSystemDataSourceDiscovery", inherit = DataSourceDiscovery)
FileSystemDataSourceDiscovery$create <- function(filesystem, selector, format = "parquet") {
  assert_is(filesystem, "FileSystem")
  assert_is(selector, "Selector")
  format <- match.arg(format) # Only parquet for now
  shared_ptr(FileSystemDataSourceDiscovery, dataset___FSDSDiscovery__Make(filesystem, selector))
}

#' @export
ScannerBuilder <- R6Class("ScannerBuilder", inherit = Object,
  public = list(
    Project = function(cols) {
      assert_is(cols, "character")
      dataset___ScannerBuilder__Project(self, cols)
      self
    },
    Filter = function(expr) {
      assert_is(expr, "Expression")
      dataset___ScannerBuilder__Filter(self, expr)
      self
    },
    Finish = function() unique_ptr(Scanner, dataset___ScannerBuilder__Finish(self))
  ),
  active = list(
    schema = function() shared_ptr(Schema, dataset___ScannerBuilder__schema(self))
  )
)

#' @export
names.ScannerBuilder <- function(x) names(x$schema)

#' @export
Scanner <- R6Class("Scanner", inherit = Object,
  public = list(
    ToTable = function() shared_ptr(Table, dataset___Scanner__ToTable(self))
  )
)

ScanOptions <- R6Class("ScanOptions", inherit = Object)
ScanContext <- R6Class("ScanContext", inherit = Object)
DataFragment <- R6Class("DataFragment", inherit = Object,
  public = list(
    splittable = function() dataset___DataFragment__splittable(self),
    scan_options = function() {
      shared_ptr(ScanOptions, dataset___DataFragment__scan_options(self))
    }
  )
)
