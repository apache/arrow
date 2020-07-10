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

#' @title Arrow Arrays
#' @description An `Array` is an immutable data array with some logical type
#' and some length. Most logical types are contained in the base
#' `Array` class; there are also subclasses for `DictionaryArray`, `ListArray`,
#' and `StructArray`.
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Factory:
#' The `Array$create()` factory method instantiates an `Array` and
#' takes the following arguments:
#' * `x`: an R vector, list, or `data.frame`
#' * `type`: an optional [data type][data-type] for `x`. If omitted, the type
#'    will be inferred from the data.
#'
#' `Array$create()` will return the appropriate subclass of `Array`, such as
#' `DictionaryArray` when given an R factor.
#'
#' To compose a `DictionaryArray` directly, call `DictionaryArray$create()`,
#' which takes two arguments:
#' * `x`: an R vector or `Array` of integers for the dictionary indices
#' * `dict`: an R vector or `Array` of dictionary values (like R factor levels
#'   but not limited to strings only)
#' @section Usage:
#'
#' ```
#' a <- Array$create(x)
#' length(a)
#'
#' print(a)
#' a == a
#' ```
#'
#' @section Methods:
#'
#' - `$IsNull(i)`: Return true if value at index is null. Does not boundscheck
#' - `$IsValid(i)`: Return true if value at index is valid. Does not boundscheck
#' - `$length()`: Size in the number of elements this array contains
#' - `$offset()`: A relative position into another array's data, to enable zero-copy slicing
#' - `$null_count()`: The number of null entries in the array
#' - `$type()`: logical type of data
#' - `$type_id()`: type id
#' - `$Equals(other)` : is this array equal to `other`
#' - `$ApproxEquals(other)` :
#' - `$data()`: return the underlying [ArrayData][ArrayData]
#' - `$as_vector()`: convert to an R vector
#' - `$ToString()`: string representation of the array
#' - `$Slice(offset, length = NULL)`: Construct a zero-copy slice of the array
#'    with the indicated offset and length. If length is `NULL`, the slice goes
#'    until the end of the array.
#' - `$Take(i)`: return an `Array` with values at positions given by integers
#'    (R vector or Array Array) `i`.
#' - `$Filter(i, keep_na = TRUE)`: return an `Array` with values at positions where logical
#'    vector (or Arrow boolean Array) `i` is `TRUE`.
#' - `$RangeEquals(other, start_idx, end_idx, other_start_idx)` :
#' - `$cast(target_type, safe = TRUE, options = cast_options(safe))`: Alter the
#'    data in the array to change its type.
#' - `$View(type)`: Construct a zero-copy view of this array with the given type.
#' - `$Validate()` : Perform any validation checks to determine obvious inconsistencies
#'    within the array's internal data. This can be an expensive check, potentially `O(length)`
#'
#' @rdname array
#' @name array
#' @export
Array <- R6Class("Array",
  inherit = ArrowObject,
  public = list(
    ..dispatch = function() {
      type_id <- self$type_id()
      if (type_id == Type$DICTIONARY){
        shared_ptr(DictionaryArray, self$pointer())
      } else if (type_id == Type$STRUCT) {
        shared_ptr(StructArray, self$pointer())
      } else if (type_id == Type$LIST) {
        shared_ptr(ListArray, self$pointer())
      } else if (type_id == Type$LARGE_LIST){
        shared_ptr(LargeListArray, self$pointer())
      } else if (type_id == Type$FIXED_SIZE_LIST){
        shared_ptr(FixedSizeListArray, self$pointer())
      } else {
        self
      }
    },
    IsNull = function(i) Array__IsNull(self, i),
    IsValid = function(i) Array__IsValid(self, i),
    length = function() Array__length(self),
    type_id = function() Array__type_id(self),
    Equals = function(other, ...) {
      inherits(other, "Array") && Array__Equals(self, other)
    },
    ApproxEquals = function(other) {
      inherits(other, "Array") && Array__ApproxEquals(self, other)
    },
    data = function() shared_ptr(ArrayData, Array__data(self)),
    as_vector = function() Array__as_vector(self),
    ToString = function() {
      typ <- paste0("<", self$type$ToString(), ">")
      paste(typ, Array__ToString(self), sep = "\n")
    },
    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        Array$create(Array__Slice1(self, offset))
      } else {
        Array$create(Array__Slice2(self, offset, length))
      }
    },
    Take = function(i) {
      if (is.numeric(i)) {
        i <- as.integer(i)
      }
      if (is.integer(i)) {
        i <- Array$create(i)
      }
      # ARROW-9001: autoboxing in call_function
      result <- call_function("take", self, i)
      if (inherits(i, "ChunkedArray")) {
        return(shared_ptr(ChunkedArray, result))
      } else {
        Array$create(result)
      }
    },
    Filter = function(i, keep_na = TRUE) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      assert_is(i, "Array")
      Array$create(call_function("filter", self, i, options = list(keep_na = keep_na)))
    },
    RangeEquals = function(other, start_idx, end_idx, other_start_idx = 0L) {
      assert_is(other, "Array")
      Array__RangeEquals(self, other, start_idx, end_idx, other_start_idx)
    },
    cast = function(target_type, safe = TRUE, options = cast_options(safe)) {
      assert_is(options, "CastOptions")
      Array$create(Array__cast(self, as_type(target_type), options))
    },
    View = function(type) {
      Array$create(Array__View(self, as_type(type)))
    },
    Validate = function() Array__Validate(self)
  ),
  active = list(
    null_count = function() Array__null_count(self),
    offset = function() Array__offset(self),
    type = function() DataType$create(Array__type(self))
  )
)
Array$create <- function(x, type = NULL) {
  if (!inherits(x, "externalptr")) {
    if (!is.null(type)) {
      type <- as_type(type)
    }
    x <- Array__from_vector(x, type)
  }
  shared_ptr(Array, x)$..dispatch()
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
DictionaryArray <- R6Class("DictionaryArray", inherit = Array,
  public = list(
    indices = function() Array$create(DictionaryArray__indices(self)),
    dictionary = function() Array$create(DictionaryArray__dictionary(self))
  ),
  active = list(
    ordered = function() self$type$ordered
  )
)
DictionaryArray$create <- function(x, dict = NULL) {
  if (is.factor(x)) {
    # The simple case: converting a factor.
    # Ignoring `dict`; should probably error if dict is not NULL
    return(Array$create(x))
  }

  assert_that(!is.null(dict))
  if (!is.Array(x)) {
    x <- Array$create(x)
  }
  if (!is.Array(dict)) {
    dict <- Array$create(dict)
  }
  type <- DictionaryType$create(x$type, dict$type)
  shared_ptr(DictionaryArray, DictionaryArray__FromArrays(type, x, dict))
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
StructArray <- R6Class("StructArray", inherit = Array,
  public = list(
    field = function(i) Array$create(StructArray__field(self, i)),
    GetFieldByName = function(name) Array$create(StructArray__GetFieldByName(self, name)),
    Flatten = function() map(StructArray__Flatten(self), ~ Array$create(.x))
  )
)

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
ListArray <- R6Class("ListArray", inherit = Array,
  public = list(
    values = function() Array$create(ListArray__values(self)),
    value_length = function(i) ListArray__value_length(self, i),
    value_offset = function(i) ListArray__value_offset(self, i),
    raw_value_offsets = function() ListArray__raw_value_offsets(self)
  ),
  active = list(
    value_type = function() DataType$create(ListArray__value_type(self))
  )
)

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
LargeListArray <- R6Class("LargeListArray", inherit = Array,
  public = list(
    values = function() Array$create(LargeListArray__values(self)),
    value_length = function(i) LargeListArray__value_length(self, i),
    value_offset = function(i) LargeListArray__value_offset(self, i),
    raw_value_offsets = function() LargeListArray__raw_value_offsets(self)
  ),
  active = list(
    value_type = function() DataType$create(LargeListArray__value_type(self))
  )
)

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
FixedSizeListArray <- R6Class("FixedSizeListArray", inherit = Array,
  public = list(
    values = function() Array$create(FixedSizeListArray__values(self)),
    value_length = function(i) FixedSizeListArray__value_length(self, i),
    value_offset = function(i) FixedSizeListArray__value_offset(self, i)
  ),
  active = list(
    value_type = function() DataType$create(FixedSizeListArray__value_type(self)),
    list_size = function() self$type$list_size
  )
)

#' @export
length.Array <- function(x) x$length()

#' @export
is.na.Array <- function(x) {
  if (x$type == null()) {
    rep(TRUE, length(x))
  } else {
    !Array__Mask(x)
  }
}

#' @export
as.vector.Array <- function(x, mode) x$as_vector()

filter_rows <- function(x, i, keep_na = TRUE, ...) {
  # General purpose function for [ row subsetting with R semantics
  # Based on the input for `i`, calls x$Filter, x$Slice, or x$Take
  nrows <- x$num_rows %||% x$length() # Depends on whether Array or Table-like
  if (inherits(i, "array_expression")) {
    # Evaluate it
    i <- as.vector(i)
  }
  if (is.logical(i)) {
    if (isTRUE(i)) {
      # Shortcut without doing any work
      x
    } else {
      i <- rep_len(i, nrows) # For R recycling behavior; consider vctrs::vec_recycle()
      x$Filter(i, keep_na)
    }
  } else if (is.numeric(i)) {
    if (all(i < 0)) {
      # in R, negative i means "everything but i"
      i <- setdiff(seq_len(nrows), -1 * i)
    }
    if (is.sliceable(i)) {
      x$Slice(i[1] - 1, length(i))
    } else if (all(i > 0)) {
      x$Take(i - 1)
    } else {
      stop("Cannot mix positive and negative indices", call. = FALSE)
    }
  } else if (is.Array(i, INTEGER_TYPES)) {
    # NOTE: this doesn't do the - 1 offset
    x$Take(i)
  } else if (is.Array(i, "bool")) {
    x$Filter(i, keep_na)
  } else {
    # Unsupported cases
    if (is.Array(i)) {
      stop("Cannot extract rows with an Array of type ", i$type$ToString(), call. = FALSE)
    }
    stop("Cannot extract rows with an object of class ", class(i), call.=FALSE)
  }
}

#' @export
`[.Array` <- filter_rows

#' @importFrom utils head
#' @export
head.Array <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  len <- NROW(x)
  if (n < 0) {
    # head(x, negative) means all but the last n rows
    n <- max(len + n, 0)
  } else {
    n <- min(len, n)
  }
  if (n == len) {
    return(x)
  }
  x$Slice(0, n)
}

#' @importFrom utils tail
#' @export
tail.Array <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  len <- NROW(x)
  if (n < 0) {
    # tail(x, negative) means all but the first n rows
    n <- min(-n, len)
  } else {
    n <- max(len - n, 0)
  }
  if (n == 0) {
    return(x)
  }
  x$Slice(n)
}

is.sliceable <- function(i) {
  # Determine whether `i` can be expressed as a $Slice() command
  is.numeric(i) &&
    length(i) > 0 &&
    all(i > 0) &&
    identical(as.integer(i), i[1]:i[length(i)])
}

is.Array <- function(x, type = NULL) {
  is_it <- inherits(x, c("Array", "ChunkedArray"))
  if (is_it && !is.null(type)) {
    is_it <- x$type$ToString() %in% type
  }
  is_it
}

#' @export
as.double.Array <- function(x, ...) as.double(as.vector(x), ...)

#' @export
as.integer.Array <- function(x, ...) as.integer(as.vector(x), ...)

#' @export
as.character.Array <- function(x, ...) as.character(as.vector(x), ...)
