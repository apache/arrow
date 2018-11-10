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

#' @include R6.R

`arrow::ArrayData` <- R6Class("arrow::ArrayData",
  inherit = `arrow::Object`,
  active = list(
    type = function() `arrow::DataType`$dispatch(ArrayData__get_type(self)),
    length = function() ArrayData__get_length(self),
    null_count = function() ArrayData__get_null_count(self),
    offset = function() ArrayData__get_offset(self),
    buffers = function() map(ArrayData__buffers(self), shared_ptr, class = `arrow::Buffer`)
  )
)
