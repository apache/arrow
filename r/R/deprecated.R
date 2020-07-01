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

#' @rdname read_ipc_stream
#' @export
read_arrow <- function(file, ...) {
  .Deprecated(msg = "Use 'read_ipc_stream' or 'read_feather' instead.")
  if (inherits(file, "raw")) {
    read_ipc_stream(file, ...)
  } else {
    read_feather(file, ...)
  }
}

#' @rdname write_ipc_stream
#' @export
write_arrow <- function(x, sink, ...) {
  .Deprecated(msg = "Use 'write_ipc_stream' or 'write_feather' instead.")
  if (inherits(sink, "raw")) {
    # HACK for sparklyr
    # Note that this returns a new R raw vector, not the one passed as `sink`
    write_to_raw(x)
  } else {
    write_feather(x, sink, ...)
  }
}
