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

py_to_r.pyarrow.lib.Array <- function(x, ...) {
  schema_ptr <- allocate_arrow_schema()
  array_ptr <- allocate_arrow_array()
  on.exit({
    delete_arrow_schema(schema_ptr)
    delete_arrow_array(array_ptr)
  })

  x$`_export_to_c`(array_ptr, schema_ptr)
  Array$create(ImportArray(array_ptr, schema_ptr))
}

r_to_py.Array <- function(x, convert = FALSE) {
  schema_ptr <- allocate_arrow_schema()
  array_ptr <- allocate_arrow_array()
  on.exit({
    delete_arrow_schema(schema_ptr)
    delete_arrow_array(array_ptr)
  })

  pa <- reticulate::import("pyarrow", convert = convert)
  ExportArray(x, array_ptr, schema_ptr)
  pa$Array$`_import_from_c`(array_ptr, schema_ptr)
}

py_to_r.pyarrow.lib.RecordBatch <- function(x, ...) {
  schema_ptr <- allocate_arrow_schema()
  array_ptr <- allocate_arrow_array()
  on.exit({
    delete_arrow_schema(schema_ptr)
    delete_arrow_array(array_ptr)
  })

  x$`_export_to_c`(array_ptr, schema_ptr)
  shared_ptr(RecordBatch, ImportRecordBatch(array_ptr, schema_ptr))
}

r_to_py.RecordBatch <- function(x, convert = FALSE) {
  schema_ptr <- allocate_arrow_schema()
  array_ptr <- allocate_arrow_array()
  on.exit({
    delete_arrow_schema(schema_ptr)
    delete_arrow_array(array_ptr)
  })
  
  pa <- reticulate::import("pyarrow", convert = convert)
  ExportRecordBatch(x, array_ptr, schema_ptr)
  pa$RecordBatch$`_import_from_c`(array_ptr, schema_ptr)
}
