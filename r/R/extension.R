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
    }
  )
)

ExtensionArray$.default_new <- ExtensionArray$new
ExtensionArray$new <- function(xp) {
  superclass <- ExtensionArray$.default_new(xp)
  registered_type <- extension_type_registry[[superclass$type$extension_name()]]
  if (is.null(registered_type)) {
    return(superclass)
  }

  class <- registered_type$.__enclos_env__$private$array_class
  if (inherits(superclass, class$classname)) {
    return(superclass)
  }

  class$new(xp)
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

    .set_r6_constructors = function(type_class, array_class) {
      private$type_class <- type_class
      private$array_class <- array_class
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
    }
  ),

  private = list(
    type_class = NULL,
    array_class = NULL
  )
)

ExtensionType$.default_new <- ExtensionType$new
ExtensionType$new <- function(xp) {
  superclass <- ExtensionType$.default_new(xp)
  registered_type <- extension_type_registry[[superclass$extension_name()]]
  if (is.null(registered_type)) {
    return(superclass)
  }

  registered_type$.__enclos_env__$private$type_class$new(xp)
}


MakeExtensionType <- function(storage_type,
                              extension_name,
                              extension_metadata,
                              type_class = ExtensionType,
                              array_class = ExtensionArray) {
  assert_that(is.string(extension_name), is.raw(extension_metadata))
  assert_is(storage_type, "DataType")
  assert_is(type_class, "R6ClassGenerator")
  assert_is(array_class, "R6ClassGenerator")

  type <- ExtensionType__initialize(
    storage_type,
    extension_name,
    extension_metadata,
    type_class,
    array_class
  )

  type$.set_r6_constructors(type_class, array_class)
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
      message("Deserialize called")
      private$.ptype <- unserialize(extension_metadata)
      message(sprintf("...with ptype class %s", paste0(class(private$.ptype), collapse = " / ")))
    },

    .ExtensionEquals = function(other) {
      if (!inherits(other, "VctrsExtensionType")) {
        return(FALSE)
      }

      identical(self$ptype(), other$ptype())
    }
  ),
  private = list(
    .ptype = NULL
  )
)

VctrsExtensionArray <- R6Class("VctrsExtensionArray",
  inherit = ExtensionArray,
  public = list(
    as_vector = function() {
      vctrs::vec_restore(self$storage()$as_vector(), self$type$ptype())
    }
  )
)

VctrsExtensionArray$create <- function(x, ptype = vctrs::vec_ptype(x),
                                       type = NULL) {
  if (inherits(x, "VctrsExtensionArray")) {
    return(x)
  }

  vctrs::vec_assert(x)
  type <- vctrs_extension_type(ptype)
  storage <- Array$create(vctrs::vec_data(x), type = type$storage_type())
  type$WrapArray(storage)
}


vctrs_extension_type <- function(ptype) {
  ptype <- vctrs::vec_ptype(ptype)

  MakeExtensionType(
    storage_type = type(vctrs::vec_data(ptype)),
    extension_name = "arrow.r.vctrs",
    extension_metadata = serialize(ptype, NULL),
    type_class = VctrsExtensionType,
    array_class = VctrsExtensionArray
  )
}
