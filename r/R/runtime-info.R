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
#'
#' @title class arrow::RuntimeInfo
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @rdname RuntimeInfo
#' @name RuntimeInfo
#' @keywords internal
RuntimeInfo <- R6Class("RuntimeInfo",
                      inherit = ArrowObject,
                      public = list(),
                      active = list(
                        simd_level = function() RuntimeInfo__simd_level(self),
                        detected_simd_level = function() RuntimeInfo__detected_simd_level(self)
                      )
)

#' Arrow's default [RuntimeInfo]
#'
#' @return the default [RuntimeInfo]
#' @export
#' @keywords internal
default_runtime_info <- function() {
  RuntimeInfo__default()
}
