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
#' @title FileSystem entry stats
#' @usage NULL
#' @format NULL
#'
#' @section Methods:
#'
#' - `base_name()` : The file base name (component after the last directory
#'    separator).
#' - `extension()` : The file extension
#'
#' @section Active bindings:
#'
#' - `$type`: The file type
#' - `$path`: The full file path in the filesystem
#' - `$size`: The size in bytes, if available.  Only regular files are
#'    guaranteed to have a size.
#' - `$mtime`: The time of last modification, if available.
#'
#' @rdname FileStats
#' @export
FileStats <- R6Class("FileStats",
  inherit = Object,
  public = list(
    base_name = function() fs___FileStats__base_name(self),
    extension = function() fs___FileStats__extension(self)
  ),
  active = list(
    type = function(type) {
      if (missing(type)) {
        fs___FileStats__type(self)
      } else {
        fs___FileStats__set_type(self, type)
      }
    },
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

#' @title file selector
#' @format NULL
#'
#' @section Factory:
#'
#' The `$create()` factory method instantiates a `Selector` given the 3 fields
#' described below.
#'
#' @section Fields:
#'
#' - `base_dir`: The directory in which to select files. If the path exists but
#'    doesn't point to a directory, this should be an error.
#' - `allow_non_existent`: The behavior if `base_dir` doesn't exist in the
#'    filesystem. If `FALSE`, an error is returned.  If `TRUE`, an empty
#'    selection is returned
#' - `recursive`: Whether to recurse into subdirectories.
#'
#' @rdname Selector
#' @export
Selector <- R6Class("Selector",
  inherit = Object,
  active = list(
    base_dir = function() fs___Selector__base_dir(self),
    allow_non_existent = function() fs___Selector__allow_non_existent(self),
    recursive = function() fs___Selector__recursive(self)
  )
)

Selector$create <- function(base_dir, allow_non_existent = FALSE, recursive = FALSE) {
  shared_ptr(Selector, fs___Selector__create(base_dir, allow_non_existent, recursive))
}

#' @title FileSystem classes
#' @description `FileSystem` is an abstract file system API,
#' `LocalFileSystem` is an implementation accessing files
#' on the local machine. `SubTreeFileSystem` is an implementation that delegates
#' to another implementation after prepending a fixed base path
#'
#' @section Factory:
#'
#' The `$create()` factory methods instantiate the `FileSystem` object and
#' take the following arguments, depending on the subclass:
#'
#' - no argument is needed for instantiating a `LocalFileSystem`
#' - `base_path` and `base_fs` for instantiating a `SubTreeFileSystem`
#'
#' @section Methods:
#'
#' - `$GetTargetStats(x)`: `x` may be a [Selector][Selector] or a character
#'    vector of paths. Returns a list of [FileStats][FileStats]
#' - `$CreateDir(path, recursive = TRUE)`: Create a directory and subdirectories.
#' - `$DeleteDir(path)`: Delete a directory and its contents, recursively.
#' - `$DeleteDirContents(path)`: Delete a directory's contents, recursively.
#'    Like `$DeleteDir()`,
#'    but doesn't delete the directory itself. Passing an empty path (`""`) will
#'    wipe the entire filesystem tree.
#' - `$DeleteFile(path)` : Delete a file.
#' - `$DeleteFiles(paths)` : Delete many files. The default implementation
#'    issues individual delete operations in sequence.
#' - `$Move(src, dest)`: Move / rename a file or directory. If the destination
#'    exists:
#'      if it is a non-empty directory, an error is returned
#'      otherwise, if it has the same type as the source, it is replaced
#'      otherwise, behavior is unspecified (implementation-dependent).
#' - `$CopyFile(src, dest)`: Copy a file. If the destination exists and is a
#'    directory, an error is returned. Otherwise, it is replaced.
#' - `$OpenInputStream(path)`: Open an [input stream][InputStream] for
#'    sequential reading.
#' - `$OpenInputFile(path)`: Open an [input file][RandomAccessFile] for random
#'    access reading.
#' - `$OpenOutputStream(path)`: Open an [output stream][OutputStream] for
#'    sequential writing.
#' - `$OpenAppendStream(path)`: Open an [output stream][OutputStream] for
#'    appending.
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
    GetTargetStats = function(x) {
      if (inherits(x, "Selector")) {
        map(fs___FileSystem__GetTargetStats_Selector(self, x), shared_ptr, class = FileStats)
      } else if (is.character(x)){
        map(fs___FileSystem__GetTargetStats_Paths(self, x), shared_ptr, class = FileStats)
      } else {
        abort("incompatible type for FileSystem$GetTargetStarts()")
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

    DeleteFiles = function(paths) {
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

#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @export
LocalFileSystem <- R6Class("LocalFileSystem", inherit = FileSystem)
LocalFileSystem$create <- function() {
  shared_ptr(LocalFileSystem, fs___LocalFileSystem__create())
}


#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @export
SubTreeFileSystem <- R6Class("SubTreeFileSystem", inherit = FileSystem)
SubTreeFileSystem$create <- function(base_path, base_fs) {
  xp <- fs___SubTreeFileSystem__create(base_path, base_fs)
  shared_ptr(SubTreeFileSystem, xp)
}
