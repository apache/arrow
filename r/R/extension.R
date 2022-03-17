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
ExtensionArray <- R6Class("ExtensionArray",
  inherit = Array,
  public = list(
    storage = function() {
      ExtensionArray__storage(self)
    },

    as_vector = function() {
      self$type$.array_as_vector(self)
    }
  )
)

ExtensionArray$create <- function(x, type) {
  assert_is(type, "ExtensionType")
  if (inheritx(x, "ExtensionArray") && type$Equals(x$type)) {
    return(x)
  }

  storage <- Array$create(x, type = type$storage_type())
  type$WrapArray(storage)
}

#' @include arrow-package.R
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
#' - `$Serialize()`: Returns the serialized version of the extension metadata
#'   as a [raw()] vector.
#' - `$WrapArray(array)`: Wraps a storage [Array] into an [ExtensionArray]
#'   with this extension type.
#'
#' In addition, subclasses may override the following methos to customize
#' the behaviour of extension classes.
#'
#' - `$.Deserialize(storage_type, extension_name, extension_metadata)`
#'   This method is called when a new [ExtensionType]
#'   is initialized and is responsible for parsing and validating
#'   the serialized `extension_metadata` (a [raw()] vector)
#'   such that its contents can be inspected by fields and/or methods
#'   of the R6 ExtensionType subclass. Implementations must also check the
#'   `storage_type` to make sure it is compatible with the extension type.
#' - `$.array_as_vector(extension_array)`: Convert an [Array] to an R
#'   vector. This method is called by [as.vector()] on [ExtensionArray]
#'   objects or when a [RecordBatch] containing an [ExtensionArray] is
#'   converted to a [data.frame()]. The default method returns the converted
#'   storage array.
#' - `$.chunked_array_as_vector(chunked_array)`: Convert a [ChunkedArray]
#'   to an R vector. This method is called by [as.vector()] on a [ChunkedArray]
#'   whose type matches this extension type or when a [Table] containing
#'   such a column is converted to a [data.frame()]. The default method
#'   returns the converted version of the equivalent storage arrays
#'   as a [ChunkedArray].
#' - `$.ToString()` Return a string representation that will be printed
#'   to the console when this type or an Array of this type is printed.
#'
#' @rdname ExtensionType
#' @name ExtensionType
#' @export
ExtensionType <- R6Class("ExtensionType",
  inherit = DataType,
  public = list(

    # In addition to the initialization that occurs for all
    # ArrowObject instances, we call .Deserialize(), which can
    # be overridden to populate custom fields
    initialize = function(xp) {
      super$initialize(xp)
      self$.Deserialize(
        self$storage_type(),
        self$extension_name(),
        self$Serialize()
      )
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

    Serialize = function() {
      ExtensionType__Serialize(self)
    },

    MakeArray = function(data) {
      assert_is(data, "ArrayData")
      ExtensionType__MakeArray(self, data)
    },

    WrapArray = function(array) {
      assert_is(array, "Array")
      self$MakeArray(array$data())
    },

    .Deserialize = function(storage_type, extension_name, extension_metadata) {
      # Do nothing by default but allow other classes to override this method
      # to populate R6 class members.
    },

    .ExtensionEquals = function(other) {
      inherits(other, "ExtensionType") &&
        identical(other$extension_name(), self$extension_name()) &&
        identical(other$Serialize(), self$Serialize())
    },

    .array_as_vector = function(extension_array) {
      extension_array$storage()$as_vector()
    },

    .chunked_array_as_vector = function(chunked_array) {
      storage_arrays <- lapply(
        seq_len(chunked_array$num_chunks) - 1L,
        function(i) chunked_array$chunk(i)$storage()
      )
      storage <- chunked_array(!!! storage_arrays, type = self$storage_type())
      storage$as_vector()
    },

    .ToString = function() {
      # metadata is probably valid UTF-8 (e.g., JSON), but might not be
      # and it's confusing to error when printing the object. This herustic
      # isn't perfect (but subclasses should override this method anyway)
      metadata_raw <- self$Serialize()

      if (as.raw(0x00) %in% metadata_raw) {
        if (length(metadata_raw) > 20) {
          sprintf(
            "<%s %s...>",
            class(self)[1],
            paste(format(utils::head(metadata_raw, 20)), collapse = " ")
          )
        } else {
          sprintf(
            "<%s %s>",
            class(self)[1],
            paste(format(metadata_raw), collapse = " ")
          )
        }

      } else {
        metadata_utf8 <- rawToChar(self$Serialize())
        Encoding(metadata_utf8) <- "UTF-8"
        paste0(class(self)[1], " <", metadata_utf8, ">")
      }
    }
  )
)

# ExtensionType$new() is what gets used by the generated wrapper code to
# create an R6 object when a shared_ptr<DataType> is returned to R and
# that object has type_id() EXTENSION_TYPE. Rather than add complexity
# to the wrapper code, we modify ExtensionType$new() to do what we need
# it to do here (which is to return an instance of a custom R6
# type whose .Deserialize method is called to populate custom fields).
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

#' Extension types
#'
#' Extension arrays are wrappers around regular Arrow [Array] objects
#' that provide some customized behaviour and/or storage. A common use-case
#' for extension types is to define a customized conversion between an
#' an Arrow [Array] and an R object when the default conversion is slow
#' or looses metadata important to the interpretation of values in the array.
#' For most types, the built-in
#' [vctrs extension type][vctrs_extension_type] is probably sufficient.
#'
#' These functions create, register, and unregister [ExtensionType]
#' and [ExtensionArray] objects. To use an extension type you will have to:
#'
#' - Define an [R6::R6Class] that inherits from [ExtensionType] and reimplement
#'   one or more methods (e.g., `.Deserialize()`).
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
#' @param extension_metadata A [raw()] vector containing the serialized
#'   version of the type.
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
new_extension_type <- function(storage_type,
                               extension_name,
                               extension_metadata,
                               type_class = ExtensionType) {
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


VctrsExtensionType <- R6Class("VctrsExtensionType",
  inherit = ExtensionType,
  public = list(
    ptype = function() {
      private$.ptype
    },

    .ToString = function() {
      tf <- tempfile()
      sink(tf)
      on.exit({
        sink(NULL)
        unlink(tf)
      })
      print(self$ptype())
      paste0(readLines(tf), collapse = "\n")
    },

    .Deserialize = function(storage_type, extension_name, extension_metadata) {
      private$.ptype <- unserialize(extension_metadata)
    },

    .ExtensionEquals = function(other) {
      if (!inherits(other, "VctrsExtensionType")) {
        return(FALSE)
      }

      identical(self$ptype(), other$ptype())
    },

    .chunked_array_as_vector = function(chunked_array) {
      vctrs::vec_restore(
        super$.chunked_array_as_vector(chunked_array),
        self$ptype()
      )
    },

    .array_as_vector = function(extension_array) {
      vctrs::vec_restore(
        super$.array_as_vector(extension_array),
        self$ptype()
      )
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
#' @examplesIf arrow_available()
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
  type$WrapArray(storage)
}

#' @rdname vctrs_extension_array
#' @export
vctrs_extension_type <- function(ptype,
                                 storage_type = type(vctrs::vec_data(ptype))) {
  ptype <- vctrs::vec_ptype(ptype)

  new_extension_type(
    storage_type = storage_type,
    extension_name = "arrow.r.vctrs",
    extension_metadata = serialize(ptype, NULL),
    type_class = VctrsExtensionType
  )
}
