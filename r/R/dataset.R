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

#' @export
ScanOptions <- R6Class("ScanOptions", inherit = Object,
  public = list()
)

#' @export
ScanContext <- R6Class("ScanContext", inherit = Object,
  public = list()
)

#' @export
DataFragment <- R6Class("DataFragment", inherit = Object,
  public = list(
    splittable = function() dataset___DataFragment__splittable(self),
    scan_options = function() shared_ptr(ScanOptions, dataset___DataFragment__scan_options(self))
  )
)

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
  # TODO: validate inputs
  shared_ptr(Dataset, dataset___Dataset__create(sources, schema))
}

#' @export
names.Dataset <- function(x) names(x$schema)

#' @export
DataSource <- R6Class("DataSource", inherit = Object,
  public = list()
)

FileSystemDataSourceDiscovery <- R6Class("FileSystemDataSourceDiscovery", inherit = Object,
  public = list(
    Finish = function() shared_ptr(DataSource, dataset___DSDiscovery__Finish(self)),
    Inspect = function() shared_ptr(Schema, dataset___DSDiscovery__Inspect(self))
  )
)
FileSystemDataSourceDiscovery$create <- function(fs, selector) {
  shared_ptr(FileSystemDataSourceDiscovery, dataset___FSDSDiscovery__Make(fs, selector))
}

#' @export
ScannerBuilder <- R6Class("ScannerBuilder", inherit = Object,
  public = list(
    Project = function(cols) {
      dataset___ScannerBuilder__Project(self, cols)
      self
    },
    Filter = function(expr) {
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
