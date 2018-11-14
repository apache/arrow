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

#' @include array.R

`arrow::compute::CastOptions` <- R6Class("arrow::compute::CastOptions", inherit = `arrow::Object`)

#' Cast options
#'
#' @param safe enforce safe conversion
#' @param allow_int_overflow allow int conversion, `!safe` by default
#' @param allow_time_truncate allow time truncate, `!safe` by default
#' @param allow_float_truncate allow float truncate, `!safe` by default
#'
#' @export
cast_options <- function(
  safe = TRUE,
  allow_int_overflow = !safe,
  allow_time_truncate = !safe,
  allow_float_truncate = !safe
){
  shared_ptr(`arrow::compute::CastOptions`,
    compute___CastOptions__initialize(allow_int_overflow, allow_time_truncate, allow_float_truncate)
  )
}
