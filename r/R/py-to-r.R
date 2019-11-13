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
  ptr <- allocate_arrow_array()
  on.exit(delete_arrow_array(ptr))
  x$`_export_to_c`(ptr)
  Array$create(ImportArray(ptr))
}

r_to_py.Array <- function(x, convert = FALSE) {
  ptr <- allocate_arrow_array()
  on.exit(delete_arrow_array(ptr))
  ExportArray(x, ptr)
  pa <- reticulate::import("pyarrow", convert = convert)
  pa$Array$`_import_from_c`(ptr)
}

py_to_r.pyarrow.lib.RecordBatch <- function(x, ...) {
  ptr <- allocate_arrow_array()
  on.exit(delete_arrow_array(ptr))
  x$`_export_to_c`(ptr)
  shared_ptr(RecordBatch, ImportRecordBatch(ptr))
}

r_to_py.RecordBatch <- function(x, convert = FALSE) {
  ptr <- allocate_arrow_array()
  on.exit(delete_arrow_array(ptr))
  ExportRecordBatch(x, ptr)
  pa <- reticulate::import("pyarrow", convert = convert)
  pa$RecordBatch$`_import_from_c`(ptr)
}
