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

ExtensionType <- R6Class("ExtensionType",
  inherit = DataType,
  public = list(
    initialize = function(xp) {
      super$initialize(xp)
      self$.Deserialize(
        self$storage_type(),
        self$extension_name(),
        self$Serialize()
      )
    },

    .set_r6_constructors = function(type_class) {
      private$type_class <- type_class
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

    ToString = function() {
      metadata_utf8 <- rawToChar(self$Serialize())
      Encoding(metadata_utf8) <- "UTF-8"
      paste0(class(self)[1], " <", metadata_utf8, ">")
    },

    .Deserialize = function(storage_type, extension_name, extension_metadata) {
      # Do nothing by default but allow other classes to override this method
      # to populate R6 class members.
    },

    .ExtensionEquals = function(other) {
      # note that this must not call to C++ (because C++ might call here)
      inherits(other, "ExtensionType") &&
        identical(other$extension_name(), self$extension_name()) &&
        identical(other$Serialize(), self$Serialize())
    },

    .chunked_array_as_vector = function(chunked_array) {
      storage_arrays <- lapply(
        seq_len(chunked_array$num_chunks) - 1L,
        function(i) chunked_array$chunk(i)$storage()
      )
      storage <- chunked_array(!!! storage_arrays, type = self$storage_type())
      storage$as_vector()
    },

    .array_as_vector = function(extension_array) {
      extension_array$storage()$as_vector()
    }
  ),
  private = list(
    type_class = NULL
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
  registered_type_instance <- extension_type_registry[[super$extension_name()]]
  if (is.null(registered_type_instance)) {
    return(super)
  }

  instance <- registered_type_instance$clone()
  instance$.__enclos_env__$super <- super
  instance$initialize(xp)

  instance
}


MakeExtensionType <- function(storage_type,
                              extension_name,
                              extension_metadata,
                              type_class = ExtensionType) {
  assert_that(is.string(extension_name), is.raw(extension_metadata))
  assert_is(storage_type, "DataType")
  assert_is(type_class, "R6ClassGenerator")

  type <- ExtensionType__initialize(
    storage_type,
    extension_name,
    extension_metadata,
    type_class
  )

  type$.set_r6_constructors(type_class)
  type$.Deserialize(storage_type, extension_name, extension_metadata)
  type
}

RegisterExtensionType <- function(type) {
  assert_is(type, "ExtensionType")
  arrow__RegisterRExtensionType(type)
  extension_type_registry[[type$extension_name()]] <- type
  invisible(type)
}

ReRegisterExtensionType <- function(type) {
  extension_name <- type$extension_name()
  result <- extension_type_registry[[extension_name]]
  if (!is.null(result)) {
    UnregisterExtensionType(extension_name)
  }

  tryCatch(
    RegisterExtensionType(type),
    error = function(e) {
      UnregisterExtensionType(extension_name)
      RegisterExtensionType(type)
    }
  )

  invisible(result)
}

UnregisterExtensionType <- function(extension_name) {
  arrow__UnregisterRExtensionType(extension_name)
  result <- extension_type_registry[[extension_name]]
  if (!is.null(result)) {
    rm(list = extension_name, envir = extension_type_registry)
  }
  invisible(result)
}

extension_type_registry <- new.env(parent = emptyenv())


VctrsExtensionType <- R6Class("VctrsExtensionType",
  inherit = ExtensionType,
  public = list(
    ptype = function() {
      private$.ptype
    },

    ToString = function() {
      tf <- tempfile()
      on.exit(unlink(tf))
      sink(tf)
      print(self$ptype())
      sink(NULL)
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


vctrs_extension_type <- function(ptype,
                                 storage_type = type(vctrs::vec_data(ptype))) {
  ptype <- vctrs::vec_ptype(ptype)

  MakeExtensionType(
    storage_type = storage_type,
    extension_name = "arrow.r.vctrs",
    extension_metadata = serialize(ptype, NULL),
    type_class = VctrsExtensionType
  )
}
