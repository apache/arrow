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

#' @export
Ops.Array <- function(e1, e2) {
  structure(list(fun = .Generic, args = list(e1, e2)), class = "Expression")
}

#' @export
Ops.ChunkedArray <- Ops.Array

#' @export
Ops.Expression <- Ops.Array

#' @export
as.vector.Expression <- function(x, ...) {
  x$args <- lapply(x$args, as.vector)
  do.call(x$fun, x$args)
}

#' @export
print.Expression <- function(x, ...) print(as.vector(x))
