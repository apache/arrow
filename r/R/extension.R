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
    initialize = function(xp) {
      super$initialize(xp)
    },

    storage = function() {
      ExtensionArray__storage(self)
    }
  )
)

ExtensionType <- R6Class("ExtensionType",
  inherit = DataType,
  public = list(
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

    ToString = function() {
      metadata_utf8 <- rawToChar(self$Serialize())
      Encoding(metadata_utf8) <- "UTF-8"
      paste0(class(self)[1], " <", metadata_utf8, ">")
    },

    .Deserialize = function(storage_type, extension_name, extension_metadata) {
      # Do nothing by default but allow other classes to override this method
      # to populate R6 class members.
    }
  )
)

ExtensionType$.default_new <- ExtensionType$new
ExtensionType$new <- function(xp) {
  superclass <- ExtensionType$.default_new(xp)
  registered_type <- extension_type_registry[[superclass$extension_name()]]
  if (is.null(registered_type)) {
    return(superclass)
  }

  type <- registered_type$clone()
  type[[".:xp:."]] <- xp
  type
}


MakeExtensionType <- function(storage_type,
                              extension_name, extension_metadata,
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

  type$.Deserialize(storage_type, extension_name, extension_metadata)
  type
}

RegisterExtensionType <- function(type) {
  assert_is(type, "ExtensionType")
  arrow__RegisterRExtensionType(type)
  extension_type_registry[[type$extension_name()]] <- type
  invisible(type)
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
