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

  # Import with convert = FALSE so that `_import_from_c` returns a Python object
  pa <- reticulate::import("pyarrow", convert = FALSE)
  ExportArray(x, array_ptr, schema_ptr)
  out <- pa$Array$`_import_from_c`(array_ptr, schema_ptr)
  # But set the convert attribute on the return object to the requested value
  assign("convert", convert, out)
  out
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

  # Import with convert = FALSE so that `_import_from_c` returns a Python object
  pa <- reticulate::import("pyarrow", convert = FALSE)
  ExportRecordBatch(x, array_ptr, schema_ptr)
  out <- pa$RecordBatch$`_import_from_c`(array_ptr, schema_ptr)
  # But set the convert attribute on the return object to the requested value
  assign("convert", convert, out)
  out
}

r_to_py.ChunkedArray <- function(x, convert = FALSE) {
  # Import with convert = FALSE so that `_import_from_c` returns a Python object
  pa <- reticulate::import("pyarrow", convert = FALSE)
  out <- pa$chunked_array(x$chunks)
  # But set the convert attribute on the return object to the requested value
  assign("convert", convert, out)
  out
}

py_to_r.pyarrow.lib.ChunkedArray <- function(x, ...) {
  ChunkedArray$create(!!!maybe_py_to_r(x$chunks))
}

r_to_py.Table <- function(x, convert = FALSE) {
  # Import with convert = FALSE so that `_import_from_c` returns a Python object
  pa <- reticulate::import("pyarrow", convert = FALSE)
  out <- pa$Table$from_arrays(x$columns, names = names(x))
  # But set the convert attribute on the return object to the requested value
  assign("convert", convert, out)
  out
}

py_to_r.pyarrow.lib.Table <- function(x, ...) {
  colnames <- maybe_py_to_r(x$column_names)
  r_cols <- maybe_py_to_r(x$columns)
  names(r_cols) <- colnames
  Table$create(!!!r_cols)
}

maybe_py_to_r <- function(x) {
  if (inherits(x, "python.builtin.object")) {
    # Depending on some auto-convert behavior, x may already be converted
    # or it may still be a Python object
    x <- reticulate::py_to_r(x)
  }
  x
}

#' Install pyarrow for use with reticulate
#'
#' `pyarrow` is the Python package for Apache Arrow. This function helps with
#' installing it for use with `reticulate`.
#'
#' @param envname The name or full path of the Python environment to install
#' into. This can be a virtualenv or conda environment created by `reticulate`.
#' See `reticulate::py_install()`.
#' @param nightly logical: Should we install a development version of the
#' package? Default is to use the official release version.
#' @param ... additional arguments passed to `reticulate::py_install()`.
#' @export
install_pyarrow <- function(envname = NULL, nightly = FALSE, ...) {
  if (nightly) {
    reticulate::py_install("pyarrow", envname = envname, ...,
      # Nightly for pip
      pip_options = "--extra-index-url https://repo.fury.io/arrow-nightlies/ --pre --upgrade",
      # Nightly for conda
      channel = "arrow-nightlies"
    )
  } else {
    reticulate::py_install("pyarrow", envname = envname, ...)
  }
}
