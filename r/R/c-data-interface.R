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

#' Allocate and free Arrow C Data Interface structs
#'
#' The [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
#' passes a column between two libraries in the same process through a pair of
#' C structs, `ArrowSchema` and `ArrowArray`, and a stream of batches through
#' `ArrowArrayStream`. These functions allocate one of those structs and return an
#' external pointer to it; the matching `delete_*()` function frees the struct.
#'
#' The pointers are what the `$export_to_c()` and `$import_from_c()` methods take:
#' `Array$export_to_c(array_ptr, schema_ptr)` fills the two structs from an
#' [Array] (likewise for [RecordBatch], and `$export_to_c(schema_ptr)` for a
#' [Schema], [DataType] or [Field]), and `Array$import_from_c(array_ptr, schema_ptr)`
#' builds an [Array] from structs another library filled. A consumer that has
#' imported a struct releases it through the struct's own release callback; call
#' `delete_*()` only to free the allocation itself, after the other side is done
#' with it.
#'
#' @return `allocate_*()` return an external pointer to a zero-initialised struct.
#'   `delete_*()` return `NULL`, invisibly.
#' @name c-data-interface
#' @rdname c-data-interface
#' @aliases allocate_arrow_schema delete_arrow_schema allocate_arrow_array
#'   delete_arrow_array allocate_arrow_array_stream delete_arrow_array_stream
#' @usage
#' allocate_arrow_schema()
#' delete_arrow_schema(ptr)
#' allocate_arrow_array()
#' delete_arrow_array(ptr)
#' allocate_arrow_array_stream()
#' delete_arrow_array_stream(ptr)
#' @param ptr an external pointer returned by the matching `allocate_*()` function
#' @examples
#' array_ptr <- allocate_arrow_array()
#' schema_ptr <- allocate_arrow_schema()
#' Array$create(c(1, 2, 3))$export_to_c(array_ptr, schema_ptr)
#' Array$import_from_c(array_ptr, schema_ptr)
#' delete_arrow_array(array_ptr)
#' delete_arrow_schema(schema_ptr)
#' @export allocate_arrow_schema
#' @export delete_arrow_schema
#' @export allocate_arrow_array
#' @export delete_arrow_array
#' @export allocate_arrow_array_stream
#' @export delete_arrow_array_stream
NULL
# The functions themselves are generated into arrowExports.R from the C++ side.
#
# A note for consumers that check buffer alignment: Array$create() on an R double or
# integer vector borrows the vector's memory rather than copying it, so the values buffer
# an export hands over starts where R's data does, 48 bytes into R's allocation. Buffers
# arrow allocates itself (a cast to another type, concat_arrays(), anything read from
# a file) are aligned by arrow's own allocator.
