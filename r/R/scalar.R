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

#' @include arrow-datum.R

#' @title Arrow scalars
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @description A `Scalar` holds a single value of an Arrow type.
#'
#' @section Factory:
#' The `Scalar$create()` factory method instantiates a `Scalar` and takes the following arguments:
#' * `x`: an R vector, list, or `data.frame`
#' * `type`: an optional [data type][data-type] for `x`. If omitted, the type will be inferred from the data.
#' @section Usage:
#'
#' ```
#' a <- Scalar$create(x)
#' length(a)
#'
#' print(a)
#' a == a
#' ```
#'
#' @section Methods:
#'
#' - `$ToString()`: convert to a string
#' - `$as_vector()`: convert to an R vector
#' - `$as_array()`: convert to an Arrow `Array`
#' - `$Equals(other)`: is this Scalar equal to `other`
#' - `$ApproxEquals(other)`: is this Scalar approximately equal to `other`
#' - `$is_valid`: is this Scalar valid
#' - `$null_count`: number of invalid values - 1 or 0
#' - `$type`: Scalar type
#' - `$cast(target_type, safe = TRUE, options = cast_options(safe))`: cast value
#'     to a different type
#'
#' @name Scalar
#' @rdname Scalar
#' @examples
#' Scalar$create(pi)
#' Scalar$create(404)
#' # If you pass a vector into Scalar$create, you get a list containing your items
#' Scalar$create(c(1, 2, 3))
#'
#' # Comparisons
#' my_scalar <- Scalar$create(99)
#' my_scalar$ApproxEquals(Scalar$create(99.00001)) # FALSE
#' my_scalar$ApproxEquals(Scalar$create(99.000009)) # TRUE
#' my_scalar$Equals(Scalar$create(99.000009)) # FALSE
#' my_scalar$Equals(Scalar$create(99L)) # FALSE (types don't match)
#'
#' my_scalar$ToString()
#' @export
Scalar <- R6Class("Scalar",
  inherit = ArrowDatum,
  public = list(
    ToString = function() {
      if (self$type_id() == Type$EXTENSION) {
        format(self$as_vector())
      } else {
        Scalar__ToString(self)
      }
    },
    type_id = function() Scalar__type(self)$id,
    as_vector = function(length = 1L) self$as_array(length)$as_vector(),
    as_array = function(length = 1L) MakeArrayFromScalar(self, as.integer(length)),
    Equals = function(other, ...) {
      inherits(other, "Scalar") && Scalar__Equals(self, other)
    },
    ApproxEquals = function(other, ...) {
      inherits(other, "Scalar") && Scalar__ApproxEquals(self, other)
    }
  ),
  active = list(
    is_valid = function() Scalar__is_valid(self),
    null_count = function() sum(!self$is_valid),
    type = function() Scalar__type(self)
  )
)
Scalar$create <- function(x, type = NULL) {
  if (is.null(x)) {
    x <- vctrs::unspecified(1)
  } else if (length(x) != 1 && !is.data.frame(x)) {
    # Wrap in a list type
    x <- list(x)
  }
  Array__GetScalar(Array$create(x, type = type), 0)
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
StructScalar <- R6Class("StructScalar",
  inherit = Scalar,
  public = list(
    field = function(i) StructScalar__field(self, i),
    GetFieldByName = function(name) StructScalar__GetFieldByName(self, name)
  )
)

#' @export
length.Scalar <- function(x) 1L

#' @export
sort.Scalar <- function(x, decreasing = FALSE, ...) x
