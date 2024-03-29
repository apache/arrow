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

#' Manage the global CPU thread pool in libarrow
#'
#' @export
cpu_count <- function() {
  GetCpuThreadPoolCapacity()
}

#' @rdname cpu_count
#' @param num_threads integer: New number of threads for thread pool
#' @export
set_cpu_count <- function(num_threads) {
  current_cpu_count <- cpu_count()
  SetCpuThreadPoolCapacity(as.integer(num_threads))
  invisible(current_cpu_count)
}

#' Manage the global I/O thread pool in libarrow
#'
#' @export
io_thread_count <- function() {
  GetIOThreadPoolCapacity()
}

#' @rdname io_thread_count
#' @param num_threads integer: New number of threads for thread pool. At least
#'   two threads are recommended to support all operations in the arrow
#'   package.
#' @export
set_io_thread_count <- function(num_threads) {
  current_io_thread_count <- io_thread_count()
  SetIOThreadPoolCapacity(as.integer(num_threads))

  # Warn for IO thread count < 2: Arrow C++ makes the assumption that there
  # is at least one thread available and the R package uses one thread from
  # the IO thread pool to support calling into R from C++.
  if (num_threads < 2) {
    rlang::warn(
      c(
        "`arrow::set_io_thread_count()` with num_threads < 2 may",
        "cause certain operations to hang or crash.",
        "i" = "Use num_threads >= 2 to support all operations"
      )
    )
  }

  invisible(current_io_thread_count)
}
