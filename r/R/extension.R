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
  active = list(
    type = function() {
      # C++ call
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

    ToString = function() {
      self$extension_name()
    },

    Serialize = function() {
      ExtensionType__Serialize(self)
    },

    .initialize = function(storage_type, extension_name, extension_metadata) {
      abort("Not implemented")
    }
  )
)

MakeExtensionType <- function(storage_type,
                              extension_name, extension_metadata,
                              type_class, array_class = ExtensionArray) {
  assert_is(type_class, "R6ClassGenerator")
  assert_is(array_class, "R6ClassGenerator")

  type <- ExtensionType__initialize(
    storage_type,
    extension_name,
    extension_metadata,
    type_class,
    array_class
  )

  type$.initialize(storage_type, extension_name, extension_metadata)
  type
}


SimpleExtensionType <- R6Class("SimpleExtensionType",
  inherit = ExtensionType,
  public = list(
    ToString = function() {
      paste0(self$extension_name(), " <'", self.metadata(), "'>")
    },

    metadata = function() {
      metadata_chr
    },

    .initialize = function(storage_type, extension_name, extension_metadata) {
      private$metadata_chr <- rawToChar(extension_metadata)
    }
  ),
  private = list(
    metadata_chr = NULL
  )
)

SimpleExtensionType$create <- function(storage_type, metadata = "") {
  MakeExtensionType(
    storage_type,
    "arrow_r.simple_extension",
    charToRaw(metadata),
    type_class = SimpleExtensionType
  )
}
