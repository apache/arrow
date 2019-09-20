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
