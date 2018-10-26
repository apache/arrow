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

#' decimal type
#'
#' @param data decimal data, hosted in a complex vector
#' @param scale scale
#' @param x a `arrow_decimal128`
#'
#' @export
new_decimal128 <- function(data = complex(), scale = 0L) {
  stopifnot(is.complex(data))
  stopifnot(is.integer(scale), length(scale) == 1L)

  new_rcrd(list(data = data), scale = scale, class = "arrow_decimal128")
}

#' @rdname new_decimal128
#' @export
decimal128 <- function(data, scale = 0L) {
  new_decimal128(data, scale = scale)
}

#' @rdname new_decimal128
#' @export
format.arrow_decimal128 <- function(x, ...) {
  format_decimal128(vctrs::field(x, "data"), attr(x, "scale"))
}

# Coerce ------------------------------------------------------------------

#' @import vctrs
#' @rdname new_decimal128
#' @export
#' @method vec_type2 arrow_decimal128
#' @export vec_type2.arrow_decimal128
vec_type2.arrow_decimal128 <- function(x, y){
  UseMethod("vec_type2.arrow_decimal128", y)
}

#' @export
#' @method vec_type2.arrow_decimal128 arrow_decimal128
vec_type2.arrow_decimal128.arrow_decimal128 <- function(x, y){
  new_decimal128()
}

#' @export
#' @method vec_type2.integer arrow_decimal128
vec_type2.integer.arrow_decimal128 <- function(x, y){
  new_decimal128()
}

#' @export
#' @method vec_type2.arrow_decimal128 integer
vec_type2.arrow_decimal128.integer <- function(x, y){
  new_decimal128()
}

#' @export
#' @method vec_type2.integer64 arrow_decimal128
vec_type2.integer64.arrow_decimal128 <- function(x, y){
  new_decimal128()
}

#' @export
#' @method vec_type2.arrow_decimal128 integer64
vec_type2.arrow_decimal128.integer64 <- function(x, y){
  new_decimal128()
}

# Cast ------------------------------------------------------------------

#' @importFrom vctrs vec_cast
#' @export
#' @rdname new_decimal128
#' @export vec_cast.arrow_decimal128
#' @method vec_cast arrow_decimal128
vec_cast.arrow_decimal128 <- function(x, to) UseMethod("vec_cast.arrow_decimal128")

#' @export
#' @method vec_cast.arrow_decimal128 default
vec_cast.arrow_decimal128.default <- function(x, to) {
  stop_incompatible_cast(x, to)
}

#' @export
#' @method vec_cast.arrow_decimal128 arrow_decimal128
vec_cast.arrow_decimal128.arrow_decimal128 <- function(x, to) {
  x
}

#' @export
#' @method vec_cast.arrow_decimal128 integer64
vec_cast.arrow_decimal128.integer64 <- function(x, to) {
  new_decimal128(Integer64Vector_to_Decimal128(x), scale = 0L)
}

#' @export
#' @method vec_cast.arrow_decimal128 integer
vec_cast.arrow_decimal128.integer <- function(x, to) {
  new_decimal128(IntegerVector_to_Decimal128(x), scale = 0L)
}
