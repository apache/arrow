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

#' @include arrow-package.R

#' @title FileStats class
#' @description FileSystem entry stats
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @rdname FileStats
#' @name FileStats
#' @export
FileStats <- R6Class("FileStats",
  inherit = Object,
  public = list(
    base_name = function() fs___FileStats__base_name(self),
    extension = function() fs___FileStats__extension(self)
  ),
  active = list(
    type = function() fs___FileStats__type(self),
    path = function(path) {
      if (missing(path)) {
        fs___FileStats__path(self)
      } else {
        invisible(fs___FileStats__set_path(self))
      }
    },

    size = function(size) {
      if (missing(size)) {
        fs___FileStats__size(self)
      } else {
        invisible(fs___FileStats__set_size(self, size))
      }
    },

    mtime = function(time) {
      if (missing(time)) {
        fs___FileStats__mtime(self)
      } else {
        if (!inherits(time, "POSIXct") && length(time) == 1L) {
          abort("invalid time")
        }
        invisible(fs___FileStats__set_mtime(self, time))
      }
    }
  )
)

#' @title Selector class
#' @description EXPERIMENTAL: file selector
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @rdname Selector
#' @name Selector
#' @export
Selector <- R6Class("Selector",
  inherit = Object,
  active = list(
    base_dir = function() fs___Selector__base_dir(self),
    allow_non_existent = function() fs___Selector__allow_non_existent(self),
    recursive = function() fs___Selector__recursive(self)
  )
)

#' @title FileSystem class
#' @description EXPERIMENTAL: abstract file system API
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @rdname FileSystem
#' @name FileSystem
#' @export
FileSystem <- R6Class("FileSystem", inherit = Object,
    public = list(
      GetTargetStats = function(paths, selector = NULL) {
        if (is.null(selector)) {
          shared_ptr(FileStats, fs___FileSystem__GetTargetStats_1(paths))
        } else {
          shared_ptr(FileStats, fs___FileSystem__GetTargetStats_2(paths, selector))
        }
      },

      CreateDir = function(path, recursive = TRUE) {
        fs___FileSystem__CreateDir(self, path, isTRUE(recursive))
      },

      DeleteDir = function(path) {
        fs___FileSystem__DeleteDir(self, path)
      },

      DeleteDirContents = function(path) {
        fs___FileSystem__DeleteDirContents(self, path)
      },

      DeleteFile = function(path) {
        fs___FileSystem__DeleteFile(self, path)
      },

      DeleteFiles = function(path) {
        fs___FileSystem__DeleteFiles(self, paths)
      },

      Move = function(src, dest) {
        fs___FileSystem__Move(self, src, dest)
      },

      CopyFile = function(src, dest) {
        fs___FileSystem__CopyFile(self, src, dest)
      },

      OpenInputStream = function(path) {
        shared_ptr(InputStream, fs___FileSystem__OpenInputStream(self, path))
      },
      OpenInputFile = function(path) {
        shared_ptr(InputStream, fs___FileSystem__OpenInputFile(self, path))
      },
      OpenOutputStream = function(path) {
        shared_ptr(OutputStream, fs___FileSystem__OpenOutputStream(self, path))
      },
      OpenAppendStream = function(path) {
        shared_ptr(OutputStream, fs___FileSystem__OpenAppendStream(self, path))
      }

    )
)

#' @title SubTreeFileSystem class
#' @description EXPERIMENTAL: abstract file system API
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @rdname SubTreeFileSystem
#' @name SubTreeFileSystem
#' @export
SubTreeFileSystem <- R6Class("SubTreeFileSystem", inherit = FileSystem)

SubTreeFileSystem$create <- function(base_fs, base_path) {
  shared_ptr(SubTreeFileSystem, fs___SubTreeFileSystem__create(base_fs, base_path))
}
