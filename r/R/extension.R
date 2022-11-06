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

#' @include arrow-object.R


#' @title class arrow::ExtensionArray
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' The `ExtensionArray` class inherits from `Array`, but also provides
#' access to the underlying storage of the extension.
#'
#' - `$storage()`: Returns the underlying [Array] used to store
#'   values.
#'
#' The `ExtensionArray` is not intended to be subclassed for extension
#' types.
#'
#' @rdname ExtensionArray
#' @name ExtensionArray
#' @export
ExtensionArray <- R6Class("ExtensionArray",
  inherit = Array,
  public = list(
    storage = function() {
      ExtensionArray__storage(self)
    },
    as_vector = function() {
      self$type$as_vector(self)
    }
  )
)

ExtensionArray$create <- function(x, type) {
  assert_is(type, "ExtensionType")
  if (inherits(x, "ExtensionArray") && type$Equals(x$type)) {
    return(x)
  }

  storage <- Array$create(x, type = type$storage_type())
  type$WrapArray(storage)
}

#' @title class arrow::ExtensionType
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' The `ExtensionType` class inherits from `DataType`, but also defines
#' extra methods specific to extension types:
#'
#' - `$storage_type()`: Returns the underlying [DataType] used to store
#'   values.
#' - `$storage_id()`: Returns the [Type] identifier corresponding to the
#'   `$storage_type()`.
#' - `$extension_name()`: Returns the extension name.
#' - `$extension_metadata()`: Returns the serialized version of the extension
#'   metadata as a [raw()] vector.
#' - `$extension_metadata_utf8()`: Returns the serialized version of the
#'   extension metadata as a UTF-8 encoded string.
#' - `$WrapArray(array)`: Wraps a storage [Array] into an [ExtensionArray]
#'   with this extension type.
#'
#' In addition, subclasses may override the following methos to customize
#' the behaviour of extension classes.
#'
#' - `$deserialize_instance()`: This method is called when a new [ExtensionType]
#'   is initialized and is responsible for parsing and validating
#'   the serialized extension_metadata (a [raw()] vector)
#'   such that its contents can be inspected by fields and/or methods
#'   of the R6 ExtensionType subclass. Implementations must also check the
#'   `storage_type` to make sure it is compatible with the extension type.
#' - `$as_vector(extension_array)`: Convert an [Array] or [ChunkedArray] to an R
#'   vector. This method is called by [as.vector()] on [ExtensionArray]
#'   objects, when a [RecordBatch] containing an [ExtensionArray] is
#'   converted to a [data.frame()], or when a [ChunkedArray] (e.g., a column
#'   in a [Table]) is converted to an R vector. The default method returns the
#'   converted storage array.
#' - `$ToString()` Return a string representation that will be printed
#'   to the console when this type or an Array of this type is printed.
#'
#' @rdname ExtensionType
#' @name ExtensionType
#' @export
ExtensionType <- R6Class("ExtensionType",
  inherit = DataType,
  public = list(

    # In addition to the initialization that occurs for all
    # ArrowObject instances, we call deserialize_instance(), which can
    # be overridden to populate custom fields
    initialize = function(xp) {
      super$initialize(xp)
      self$deserialize_instance()
    },

    # Because of how C++ shared_ptr<> objects are converted to R objects,
    # the initial object that is instantiated will be of this class
    # (ExtensionType), but the R6Class object that was registered is
    # available from C++. We need this in order to produce the correct
    # R6 subclass when a shared_ptr<ExtensionType> is returned to R.
    r6_class = function() {
      ExtensionType__r6_class(self)
    },
    storage_type = function() {
      ExtensionType__storage_type(self)
    },
    storage_id = function() {
      self$storage_type()$id
    },
    extension_name = function() {
      ExtensionType__extension_name(self)
    },
    extension_metadata = function() {
      ExtensionType__Serialize(self)
    },

    # To make sure this conversion is done properly
    extension_metadata_utf8 = function() {
      metadata_utf8 <- rawToChar(self$extension_metadata())
      Encoding(metadata_utf8) <- "UTF-8"
      metadata_utf8
    },
    WrapArray = function(array) {
      assert_is(array, "Array")
      ExtensionType__MakeArray(self, array$data())
    },
    deserialize_instance = function() {
      # Do nothing by default but allow other classes to override this method
      # to populate R6 class members.
    },
    ExtensionEquals = function(other) {
      inherits(other, "ExtensionType") &&
        identical(other$extension_name(), self$extension_name()) &&
        identical(other$extension_metadata(), self$extension_metadata())
    },
    as_vector = function(extension_array) {
      if (inherits(extension_array, "ChunkedArray")) {
        # Converting one array at a time so that users don't have to remember
        # to implement two methods. Converting all the storage arrays to
        # a ChunkedArray and then converting is probably faster
        # (VctrsExtensionType does this).
        storage_vectors <- lapply(
          seq_len(extension_array$num_chunks) - 1L,
          function(i) self$as_vector(extension_array$chunk(i))
        )

        vctrs::vec_c(!!!storage_vectors)
      } else if (inherits(extension_array, "ExtensionArray")) {
        extension_array$storage()$as_vector()
      } else {
        abort(
          c(
            "`extension_array` must be a ChunkedArray or ExtensionArray",
            i = sprintf(
              "Got object of type %s",
              paste(class(extension_array), collapse = " / ")
            )
          )
        )
      }
    },
    ToString = function() {
      # metadata is probably valid UTF-8 (e.g., JSON), but might not be
      # and it's confusing to error when printing the object. This herustic
      # isn't perfect (but subclasses should override this method anyway)
      metadata_raw <- self$extension_metadata()

      if (as.raw(0x00) %in% metadata_raw) {
        if (length(metadata_raw) > 20) {
          sprintf(
            "<%s %s...>",
            class(self)[1],
            paste(format(head(metadata_raw, 20)), collapse = " ")
          )
        } else {
          sprintf(
            "<%s %s>",
            class(self)[1],
            paste(format(metadata_raw), collapse = " ")
          )
        }
      } else {
        paste0(class(self)[1], " <", self$extension_metadata_utf8(), ">")
      }
    }
  )
)

# ExtensionType$new() is what gets used by the generated wrapper code to
# create an R6 object when a shared_ptr<DataType> is returned to R and
# that object has type_id() EXTENSION_TYPE. Rather than add complexity
# to the wrapper code, we modify ExtensionType$new() to do what we need
# it to do here (which is to return an instance of a custom R6
# type whose .deserialize_instance method is called to populate custom fields).
ExtensionType$.default_new <- ExtensionType$new
ExtensionType$new <- function(xp) {
  super <- ExtensionType$.default_new(xp)
  r6_class <- super$r6_class()
  if (identical(r6_class$classname, "ExtensionType")) {
    super
  } else {
    r6_class$new(xp)
  }
}

ExtensionType$create <- function(storage_type,
                                 extension_name,
                                 extension_metadata = raw(),
                                 type_class = ExtensionType) {
  if (is.string(extension_metadata)) {
    extension_metadata <- charToRaw(enc2utf8(extension_metadata))
  }

  assert_that(is.string(extension_name), is.raw(extension_metadata))
  assert_is(storage_type, "DataType")
  assert_is(type_class, "R6ClassGenerator")

  ExtensionType__initialize(
    storage_type,
    extension_name,
    extension_metadata,
    type_class
  )
}

#' Extension types
#'
#' Extension arrays are wrappers around regular Arrow [Array] objects
#' that provide some customized behaviour and/or storage. A common use-case
#' for extension types is to define a customized conversion between an
#' an Arrow [Array] and an R object when the default conversion is slow
#' or loses metadata important to the interpretation of values in the array.
#' For most types, the built-in
#' [vctrs extension type][vctrs_extension_type] is probably sufficient.
#'
#' These functions create, register, and unregister [ExtensionType]
#' and [ExtensionArray] objects. To use an extension type you will have to:
#'
#' - Define an [R6::R6Class] that inherits from [ExtensionType] and reimplement
#'   one or more methods (e.g., `deserialize_instance()`).
#' - Make a type constructor function (e.g., `my_extension_type()`) that calls
#'   [new_extension_type()] to create an R6 instance that can be used as a
#'   [data type][data-type] elsewhere in the package.
#' - Make an array constructor function (e.g., `my_extension_array()`) that
#'   calls [new_extension_array()] to create an [Array] instance of your
#'   extension type.
#' - Register a dummy instance of your extension type created using
#'   you constructor function using [register_extension_type()].
#'
#' If defining an extension type in an R package, you will probably want to
#' use [reregister_extension_type()] in that package's [.onLoad()] hook
#' since your package will probably get reloaded in the same R session
#' during its development and [register_extension_type()] will error if
#' called twice for the same `extension_name`. For an example of an
#' extension type that uses most of these features, see
#' [vctrs_extension_type()].
#'
#' @param storage_type The [data type][data-type] of the underlying storage
#'   array.
#' @param storage_array An [Array] object of the underlying storage.
#' @param extension_type An [ExtensionType] instance.
#' @param extension_name The extension name. This should be namespaced using
#'   "dot" syntax (i.e., "some_package.some_type"). The namespace "arrow"
#'    is reserved for extension types defined by the Apache Arrow libraries.
#' @param extension_metadata A [raw()] or [character()] vector containing the
#'   serialized version of the type. Chatacter vectors must be length 1 and
#'   are converted to UTF-8 before converting to [raw()].
#' @param type_class An [R6::R6Class] whose `$new()` class method will be
#'   used to construct a new instance of the type.
#'
#' @return
#'   - `new_extension_type()` returns an [ExtensionType] instance according
#'     to the `type_class` specified.
#'   - `new_extension_array()` returns an [ExtensionArray] whose `$type`
#'     corresponds to `extension_type`.
#'   - `register_extension_type()`, `unregister_extension_type()`
#'      and `reregister_extension_type()` return `NULL`, invisibly.
#' @export
#'
#' @examples
#' # Create the R6 type whose methods control how Array objects are
#' # converted to R objects, how equality between types is computed,
#' # and how types are printed.
#' QuantizedType <- R6::R6Class(
#'   "QuantizedType",
#'   inherit = ExtensionType,
#'   public = list(
#'     # methods to access the custom metadata fields
#'     center = function() private$.center,
#'     scale = function() private$.scale,
#'
#'     # called when an Array of this type is converted to an R vector
#'     as_vector = function(extension_array) {
#'       if (inherits(extension_array, "ExtensionArray")) {
#'         unquantized_arrow <-
#'           (extension_array$storage()$cast(float64()) / private$.scale) +
#'           private$.center
#'
#'         as.vector(unquantized_arrow)
#'       } else {
#'         super$as_vector(extension_array)
#'       }
#'     },
#'
#'     # populate the custom metadata fields from the serialized metadata
#'     deserialize_instance = function() {
#'       vals <- as.numeric(strsplit(self$extension_metadata_utf8(), ";")[[1]])
#'       private$.center <- vals[1]
#'       private$.scale <- vals[2]
#'     }
#'   ),
#'   private = list(
#'     .center = NULL,
#'     .scale = NULL
#'   )
#' )
#'
#' # Create a helper type constructor that calls new_extension_type()
#' quantized <- function(center = 0, scale = 1, storage_type = int32()) {
#'   new_extension_type(
#'     storage_type = storage_type,
#'     extension_name = "arrow.example.quantized",
#'     extension_metadata = paste(center, scale, sep = ";"),
#'     type_class = QuantizedType
#'   )
#' }
#'
#' # Create a helper array constructor that calls new_extension_array()
#' quantized_array <- function(x, center = 0, scale = 1,
#'                             storage_type = int32()) {
#'   type <- quantized(center, scale, storage_type)
#'   new_extension_array(
#'     Array$create((x - center) * scale, type = storage_type),
#'     type
#'   )
#' }
#'
#' # Register the extension type so that Arrow knows what to do when
#' # it encounters this extension type
#' reregister_extension_type(quantized())
#'
#' # Create Array objects and use them!
#' (vals <- runif(5, min = 19, max = 21))
#'
#' (array <- quantized_array(
#'   vals,
#'   center = 20,
#'   scale = 2^15 - 1,
#'   storage_type = int16()
#' )
#' )
#'
#' array$type$center()
#' array$type$scale()
#'
#' as.vector(array)
new_extension_type <- function(storage_type,
                               extension_name,
                               extension_metadata = raw(),
                               type_class = ExtensionType) {
  ExtensionType$create(
    storage_type,
    extension_name,
    extension_metadata,
    type_class
  )
}

#' @rdname new_extension_type
#' @export
new_extension_array <- function(storage_array, extension_type) {
  ExtensionArray$create(storage_array, extension_type)
}

#' @rdname new_extension_type
#' @export
register_extension_type <- function(extension_type) {
  assert_is(extension_type, "ExtensionType")
  arrow__RegisterRExtensionType(extension_type)
}

#' @rdname new_extension_type
#' @export
reregister_extension_type <- function(extension_type) {
  tryCatch(
    register_extension_type(extension_type),
    error = function(e) {
      unregister_extension_type(extension_type$extension_name())
      register_extension_type(extension_type)
    }
  )
}

#' @rdname new_extension_type
#' @export
unregister_extension_type <- function(extension_name) {
  arrow__UnregisterRExtensionType(extension_name)
}

#' @importFrom utils capture.output
VctrsExtensionType <- R6Class("VctrsExtensionType",
  inherit = ExtensionType,
  public = list(
    ptype = function() private$.ptype,
    ToString = function() {
      paste0(capture.output(print(self$ptype())), collapse = "\n")
    },
    deserialize_instance = function() {
      private$.ptype <- unserialize(self$extension_metadata())
    },
    ExtensionEquals = function(other) {
      inherits(other, "VctrsExtensionType") && identical(self$ptype(), other$ptype())
    },
    as_vector = function(extension_array) {
      if (inherits(extension_array, "ChunkedArray")) {
        # rather than convert one array at a time, use more Arrow
        # machinery to convert the whole ChunkedArray at once
        storage_arrays <- lapply(
          seq_len(extension_array$num_chunks) - 1L,
          function(i) extension_array$chunk(i)$storage()
        )
        storage <- chunked_array(!!!storage_arrays, type = self$storage_type())

        vctrs::vec_restore(storage$as_vector(), self$ptype())
      } else if (inherits(extension_array, "Array")) {
        vctrs::vec_restore(
          super$as_vector(extension_array),
          self$ptype()
        )
      } else {
        super$as_vector(extension_array)
      }
    }
  ),
  private = list(
    .ptype = NULL
  )
)


#' Extension type for generic typed vectors
#'
#' Most common R vector types are converted automatically to a suitable
#' Arrow [data type][data-type] without the need for an extension type. For
#' vector types whose conversion is not suitably handled by default, you can
#' create a [vctrs_extension_array()], which passes [vctrs::vec_data()] to
#' `Array$create()` and calls [vctrs::vec_restore()] when the [Array] is
#' converted back into an R vector.
#'
#' @param x A vctr (i.e., [vctrs::vec_is()] returns `TRUE`).
#' @param ptype A [vctrs::vec_ptype()], which is usually a zero-length
#'   version of the object with the appropriate attributes set. This value
#'   will be serialized using [serialize()], so it should not refer to any
#'   R object that can't be saved/reloaded.
#' @inheritParams new_extension_type
#'
#' @return
#'   - `vctrs_extension_array()` returns an [ExtensionArray] instance with a
#'     `vctrs_extension_type()`.
#'   - `vctrs_extension_type()` returns an [ExtensionType] instance for the
#'     extension name "arrow.r.vctrs".
#' @export
#'
#' @examples
#' (array <- vctrs_extension_array(as.POSIXlt("2022-01-02 03:45", tz = "UTC")))
#' array$type
#' as.vector(array)
#'
#' temp_feather <- tempfile()
#' write_feather(arrow_table(col = array), temp_feather)
#' read_feather(temp_feather)
#' unlink(temp_feather)
vctrs_extension_array <- function(x, ptype = vctrs::vec_ptype(x),
                                  storage_type = NULL) {
  if (inherits(x, "ExtensionArray") && inherits(x$type, "VctrsExtensionType")) {
    return(x)
  }

  vctrs::vec_assert(x)
  storage <- Array$create(vctrs::vec_data(x), type = storage_type)
  type <- vctrs_extension_type(ptype, storage$type)
  new_extension_array(storage, type)
}

#' @rdname vctrs_extension_array
#' @export
vctrs_extension_type <- function(x,
                                 storage_type = infer_type(vctrs::vec_data(x))) {
  ptype <- vctrs::vec_ptype(x)

  new_extension_type(
    storage_type = storage_type,
    extension_name = "arrow.r.vctrs",
    extension_metadata = serialize(ptype, NULL),
    type_class = VctrsExtensionType
  )
}
