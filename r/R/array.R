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
#' - `$nbytes()`: Total number of bytes consumed by the elements of the array
#' - `$offset`: A relative position into another array's data, to enable zero-copy slicing
#' - `$null_count`: The number of null entries in the array
#' - `$type`: logical type of data
#' - `$type_id()`: type id
#' - `$Equals(other)` : is this array equal to `other`
#' - `$ApproxEquals(other)` :
#' - `$Diff(other)` : return a string expressing the difference between two arrays
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
#' - `$SortIndices(descending = FALSE)`: return an `Array` of integer positions that can be
#'    used to rearrange the `Array` in ascending or descending order
#' - `$RangeEquals(other, start_idx, end_idx, other_start_idx)` :
#' - `$cast(target_type, safe = TRUE, options = cast_options(safe))`: Alter the
#'    data in the array to change its type.
#' - `$View(type)`: Construct a zero-copy view of this array with the given type.
#' - `$Validate()` : Perform any validation checks to determine obvious inconsistencies
#'    within the array's internal data. This can be an expensive check, potentially `O(length)`
#'
#' @rdname array
#' @name array
#' @examples
#' my_array <- Array$create(1:10)
#' my_array$type
#' my_array$cast(int8())
#'
#' # Check if value is null; zero-indexed
#' na_array <- Array$create(c(1:5, NA))
#' na_array$IsNull(0)
#' na_array$IsNull(5)
#' na_array$IsValid(5)
#' na_array$null_count
#'
#' # zero-copy slicing; the offset of the new Array will be the same as the index passed to $Slice
#' new_array <- na_array$Slice(5)
#' new_array$offset
#'
#' # Compare 2 arrays
#' na_array2 <- na_array
#' na_array2 == na_array # element-wise comparison
#' na_array2$Equals(na_array) # overall comparison
#' @export
Array <- R6Class("Array",
  inherit = ArrowDatum,
  public = list(
    IsNull = function(i) Array__IsNull(self, i),
    IsValid = function(i) Array__IsValid(self, i),
    length = function() Array__length(self),
    type_id = function() Array__type_id(self),
    nbytes = function() Array__ReferencedBufferSize(self),
    Equals = function(other, ...) {
      inherits(other, "Array") && Array__Equals(self, other)
    },
    ApproxEquals = function(other) {
      inherits(other, "Array") && Array__ApproxEquals(self, other)
    },
    Diff = function(other) {
      if (!inherits(other, "Array")) {
        other <- Array$create(other)
      }
      Array__Diff(self, other)
    },
    data = function() Array__data(self),
    as_vector = function() Array__as_vector(self),
    ToString = function() {
      typ <- paste0("<", self$type$ToString(), ">")
      paste(typ, Array__ToString(self), sep = "\n")
    },
    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        Array__Slice1(self, offset)
      } else {
        Array__Slice2(self, offset, length)
      }
    },
    Take = function(i) {
      if (is.numeric(i)) {
        i <- as.integer(i)
      }
      if (is.integer(i)) {
        i <- Array$create(i)
      }
      call_function("take", self, i)
    },
    Filter = function(i, keep_na = TRUE) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      assert_is(i, "Array")
      call_function("filter", self, i, options = list(keep_na = keep_na))
    },
    RangeEquals = function(other, start_idx, end_idx, other_start_idx = 0L) {
      assert_is(other, "Array")
      Array__RangeEquals(self, other, start_idx, end_idx, other_start_idx)
    },
    View = function(type) {
      Array$create(Array__View(self, as_type(type)))
    },
    Same = function(other) Array__Same(self, other),
    Validate = function() Array__Validate(self),
    export_to_c = function(array_ptr, schema_ptr) ExportArray(self, array_ptr, schema_ptr)
  ),
  active = list(
    null_count = function() Array__null_count(self),
    offset = function() Array__offset(self),
    type = function() Array__type(self)
  )
)
Array$create <- function(x, type = NULL) {
  if (!is.null(type)) {
    type <- as_type(type)
  }
  if (is.null(x) && is.null(type)) {
    type <- null()
  }
  if (inherits(x, "Scalar")) {
    out <- x$as_array()
    if (!is.null(type)) {
      out <- out$cast(type)
    }
    return(out)
  }

  if (is.null(type)) {
    return(vec_to_Array(x, type))
  }

  # when a type is given, try to create a vector of the desired type. If that
  # fails, attempt to cast and if casting is successful, suggest to the user
  # to try casting manually. If the casting fails, return the original error
  # message.
  tryCatch(
    vec_to_Array(x, type),
    error = function(cnd) {
      attempt <- try(vec_to_Array(x, NULL)$cast(type), silent = TRUE)
      abort(
        c(conditionMessage(cnd),
          i = if (!inherits(attempt, "try-error")) {
            "You might want to try casting manually with `Array$create(...)$cast(...)`."
          }
        )
      )
    }
  )
}

#' @include arrowExports.R
Array$import_from_c <- ImportArray


#' Convert an object to an Arrow Array
#'
#' The `as_arrow_array()` function is identical to `Array$create()` except
#' that it is an S3 generic, which allows methods to be defined in other
#' packages to convert objects to [Array]. `Array$create()` is slightly faster
#' because it tries to convert in C++ before falling back on
#' `as_arrow_array()`.
#'
#' @param x An object to convert to an Arrow Array
#' @param ... Passed to S3 methods
#' @param type A [type][data-type] for the final Array. A value of `NULL`
#'   will default to the type guessed by [infer_type()].
#'
#' @return An [Array] with type `type`.
#' @export
#'
#' @examples
#' as_arrow_array(1:5)
#'
as_arrow_array <- function(x, ..., type = NULL) {
  UseMethod("as_arrow_array")
}

#' @export
as_arrow_array.default <- function(x, ..., type = NULL, from_vec_to_array = FALSE) {
  # If from_vec_to_array is TRUE, this is a call from C++ after
  # trying the internal C++ conversion and S3 dispatch has failed
  # failed to find a method for the object. This call happens when creating
  # Array, ChunkedArray, RecordBatch, and Table objects from data.frame
  # if the internal C++ conversion (faster and can usually be parallelized)
  # is not implemented. If the C++ call has reached this default method,
  # we error. If from_vec_to_array is FALSE, we call vec_to_Array to use the
  # internal C++ conversion.
  if (from_vec_to_array) {
    # Last ditch attempt: if vctrs::vec_is(x), we can use the vctrs
    # extension type.
    if (vctrs::vec_is(x) && is.null(type)) {
      vctrs_extension_array(x)
    } else if (vctrs::vec_is(x) && inherits(type, "VctrsExtensionType")) {
      vctrs_extension_array(
        x,
        ptype = type$ptype(),
        storage_type = type$storage_type()
      )
    } else {
      stop_cant_convert_array(x, type)
    }
  } else {
    vec_to_Array(x, type)
  }
}

#' @rdname as_arrow_array
#' @export
as_arrow_array.Array <- function(x, ..., type = NULL) {
  if (is.null(type)) {
    x
  } else {
    x$cast(type)
  }
}

#' @rdname as_arrow_array
#' @export
as_arrow_array.Scalar <- function(x, ..., type = NULL) {
  as_arrow_array(x$as_array(), ..., type = type)
}

#' @rdname as_arrow_array
#' @export
as_arrow_array.ChunkedArray <- function(x, ..., type = NULL) {
  concat_arrays(!!!x$chunks, type = type)
}

# data.frame conversion can happen in C++ when all the columns can be
# converted in C++ and when `type` is not an ExtensionType; however,
# when calling as_arrow_array(), this method will get called regardless
# of whether or not this can or can't happen.
#' @export
as_arrow_array.data.frame <- function(x, ..., type = NULL) {
  type <- type %||% infer_type(x)

  if (inherits(type, "VctrsExtensionType")) {
    storage <- as_arrow_array(x, type = type$storage_type())
    new_extension_array(storage, type)
  } else if (inherits(type, "StructType")) {
    fields <- type$fields()
    names <- map_chr(fields, "name")
    types <- map(fields, "type")
    arrays <- Map(as_arrow_array, x, type = types)
    names(arrays) <- names
    StructArray$create(!!!arrays)
  } else {
    stop_cant_convert_array(x, type)
  }
}

#' @export
as_arrow_array.vctrs_list_of <- function(x, ..., type = NULL) {
  type <- type %||% infer_type(x)
  if (!inherits(type, "ListType") && !inherits(type, "LargeListType")) {
    stop_cant_convert_array(x, type)
  }

  as_arrow_array(unclass(x), type = type)
}

#' @export
as_arrow_array.blob <- function(x, ..., type = NULL) {
  type <- type %||% infer_type(x)
  if (!type$Equals(binary()) && !type$Equals(large_binary())) {
    stop_cant_convert_array(x, type)
  }

  as_arrow_array(unclass(x), type = type)
}

stop_cant_convert_array <- function(x, type) {
  if (is.null(type)) {
    abort(
      sprintf(
        "Can't create Array from object of type %s",
        paste(class(x), collapse = " / ")
      ),
      call = caller_env()
    )
  } else {
    abort(
      sprintf(
        "Can't create Array<%s> from object of type %s",
        format(type$code()),
        paste(class(x), collapse = " / ")
      ),
      call = caller_env()
    )
  }
}

#' Concatenate zero or more Arrays
#'
#' Concatenates zero or more [Array] objects into a single
#' array. This operation will make a copy of its input; if you need
#' the behavior of a single Array but don't need a
#' single object, use [ChunkedArray].
#'
#' @param ... zero or more [Array] objects to concatenate
#' @param type An optional `type` describing the desired
#'   type for the final Array.
#'
#' @return A single [Array]
#' @export
#'
#' @examples
#' concat_arrays(Array$create(1:3), Array$create(4:5))
concat_arrays <- function(..., type = NULL) {
  dots <- lapply(list2(...), Array$create, type = type)

  if (length(dots) == 0 && is.null(type)) {
    return(Array$create(logical(), type = null()))
  } else if (length(dots) == 0) {
    return(Array$create(logical(), type = null())$cast(type))
  }

  if (!is.null(type)) {
    dots <- lapply(dots, function(array) array$cast(type))
  }

  arrow__Concatenate(dots)
}

#' @rdname concat_arrays
#' @export
c.Array <- function(...) {
  abort(c(
    "Use `concat_arrays()` or `ChunkedArray$create()` instead.",
    i = "`concat_arrays()` creates a new Array by copying data.",
    i = "`ChunkedArray$create()` uses the arrays as chunks for zero-copy concatenation."
  ))
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
DictionaryArray <- R6Class("DictionaryArray",
  inherit = Array,
  public = list(
    indices = function() DictionaryArray__indices(self),
    dictionary = function() DictionaryArray__dictionary(self)
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
  DictionaryArray__FromArrays(type, x, dict)
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
StructArray <- R6Class("StructArray",
  inherit = Array,
  public = list(
    field = function(i) StructArray__field(self, i),
    GetFieldByName = function(name) StructArray__GetFieldByName(self, name),
    Flatten = function() StructArray__Flatten(self)
  )
)

StructArray$create <- function(...) {
  data <- record_batch(...)
  StructArray__from_RecordBatch(data)
}


#' @export
`[[.StructArray` <- function(x, i, ...) {
  if (is.character(i)) {
    x$GetFieldByName(i)
  } else if (is.numeric(i)) {
    x$field(i - 1)
  } else {
    stop("'i' must be character or numeric, not ", class(i), call. = FALSE)
  }
}

#' @export
`$.StructArray` <- function(x, name, ...) {
  assert_that(is.string(name))
  if (name %in% ls(x)) {
    get(name, x)
  } else {
    x$GetFieldByName(name)
  }
}

#' @export
names.StructArray <- function(x, ...) StructType__field_names(x$type)

#' @export
dim.StructArray <- function(x, ...) c(length(x), x$type$num_fields)

#' @export
as.data.frame.StructArray <- function(x, row.names = NULL, optional = FALSE, ...) {
  as.vector(x)
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
ListArray <- R6Class("ListArray",
  inherit = Array,
  public = list(
    values = function() ListArray__values(self),
    value_length = function(i) ListArray__value_length(self, i),
    value_offset = function(i) ListArray__value_offset(self, i),
    raw_value_offsets = function() ListArray__raw_value_offsets(self)
  ),
  active = list(
    value_type = function() ListArray__value_type(self)
  )
)

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
LargeListArray <- R6Class("LargeListArray",
  inherit = Array,
  public = list(
    values = function() LargeListArray__values(self),
    value_length = function(i) LargeListArray__value_length(self, i),
    value_offset = function(i) LargeListArray__value_offset(self, i),
    raw_value_offsets = function() LargeListArray__raw_value_offsets(self)
  ),
  active = list(
    value_type = function() LargeListArray__value_type(self)
  )
)

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
FixedSizeListArray <- R6Class("FixedSizeListArray",
  inherit = Array,
  public = list(
    values = function() FixedSizeListArray__values(self),
    value_length = function(i) FixedSizeListArray__value_length(self, i),
    value_offset = function(i) FixedSizeListArray__value_offset(self, i)
  ),
  active = list(
    value_type = function() FixedSizeListArray__value_type(self),
    list_size = function() self$type$list_size
  )
)

is.Array <- function(x, type = NULL) { # nolint
  is_it <- inherits(x, c("Array", "ChunkedArray"))
  if (is_it && !is.null(type)) {
    is_it <- x$type$ToString() %in% type
  }
  is_it
}

#' @rdname array
#' @usage NULL
#' @format NULL
#' @export
MapArray <- R6Class("MapArray",
  inherit = ListArray,
  public = list(
    keys = function() MapArray__keys(self),
    items = function() MapArray__items(self),
    keys_nested = function() MapArray__keys_nested(self),
    items_nested = function() MapArray__items_nested(self)
  )
)
