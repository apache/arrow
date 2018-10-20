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

#' @include R6.R

`arrow::Field` <- R6Class("arrow::Field",
  inherit = `arrow::Object`,
  public = list(
    ToString = function() {
      Field__ToString(self)
    },
    name = function() {
      Field__name(self)
    },
    nullable = function() {
      Field__nullable(self)
    },
    Equals = function(other) {
      inherits(other, "arrow::Field") && Field__Equals(self, other)
    }
  )
)

#' @export
`==.arrow::Field` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

field <- function(name, type) {
  construct(`arrow::Field`, Field__initialize(name, type))
}

.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(nms, .list, field)
}
