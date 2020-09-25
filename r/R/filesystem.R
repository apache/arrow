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
#' @title FileSystem entry info
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
#' @rdname FileInfo
#' @export
FileInfo <- R6Class("FileInfo",
  inherit = ArrowObject,
  public = list(
    base_name = function() fs___FileInfo__base_name(self),
    extension = function() fs___FileInfo__extension(self)
  ),
  active = list(
    type = function(type) {
      if (missing(type)) {
        fs___FileInfo__type(self)
      } else {
        fs___FileInfo__set_type(self, type)
      }
    },
    path = function(path) {
      if (missing(path)) {
        fs___FileInfo__path(self)
      } else {
        invisible(fs___FileInfo__set_path(self))
      }
    },

    size = function(size) {
      if (missing(size)) {
        fs___FileInfo__size(self)
      } else {
        invisible(fs___FileInfo__set_size(self, size))
      }
    },

    mtime = function(time) {
      if (missing(time)) {
        fs___FileInfo__mtime(self)
      } else {
        if (!inherits(time, "POSIXct") && length(time) == 1L) {
          abort("invalid time")
        }
        invisible(fs___FileInfo__set_mtime(self, time))
      }
    }
  )
)

#' @title file selector
#' @format NULL
#'
#' @section Factory:
#'
#' The `$create()` factory method instantiates a `FileSelector` given the 3 fields
#' described below.
#'
#' @section Fields:
#'
#' - `base_dir`: The directory in which to select files. If the path exists but
#'    doesn't point to a directory, this should be an error.
#' - `allow_not_found`: The behavior if `base_dir` doesn't exist in the
#'    filesystem. If `FALSE`, an error is returned.  If `TRUE`, an empty
#'    selection is returned
#' - `recursive`: Whether to recurse into subdirectories.
#'
#' @rdname FileSelector
#' @export
FileSelector <- R6Class("FileSelector",
  inherit = ArrowObject,
  active = list(
    base_dir = function() fs___FileSelector__base_dir(self),
    allow_not_found = function() fs___FileSelector__allow_not_found(self),
    recursive = function() fs___FileSelector__recursive(self)
  )
)

FileSelector$create <- function(base_dir, allow_not_found = FALSE, recursive = FALSE) {
  shared_ptr(
    FileSelector,
    fs___FileSelector__create(clean_path_rel(base_dir), allow_not_found, recursive)
  )
}

#' @title FileSystem classes
#' @description `FileSystem` is an abstract file system API,
#' `LocalFileSystem` is an implementation accessing files
#' on the local machine. `SubTreeFileSystem` is an implementation that delegates
#' to another implementation after prepending a fixed base path
#'
#' @section Factory:
#'
#' `LocalFileSystem$create()` returns the object and takes no arguments.
#'
#' `SubTreeFileSystem$create()` takes the following arguments:
#'
#' - `base_path`, a string path
#' - `base_fs`, a `FileSystem` object
#'
#' `S3FileSystem$create()` optionally takes arguments:
#'
#' - `anonymous`: logical, default `FALSE`. If true, will not attempt to look up
#'    credentials using standard AWS configuration methods.
#' - `access_key`, `secret_key`: authentication credentials. If one is provided,
#'    the other must be as well. If both are provided, they will override any
#'    AWS configuration set at the environment level.
#' - `session_token`: optional string for authentication along with
#'    `access_key` and `secret_key`
#' - `role_arn`: string AWS ARN of an AccessRole. If provided instead of `access_key` and
#'    `secret_key`, temporary credentials will be fetched by assuming this role.
#' - `session_name`: optional string identifier for the assumed role session.
#' - `external_id`: optional unique string identifier that might be required
#'    when you assume a role in another account.
#' - `load_frequency`: integer, frequency (in seconds) with which temporary
#'    credentials from an assumed role session will be refreshed. Default is
#'    900 (i.e. 15 minutes)
#' - `region`: AWS region to connect to. If omitted, the AWS library will
#'    provide a sensible default based on client configuration, falling back
#'    to "us-east-1" if no other alternatives are found.
#' - `endpoint_override`: If non-empty, override region with a connect string
#'    such as "localhost:9000". This is useful for connecting to file systems
#'    that emulate S3.
#' - `scheme`: S3 connection transport (default "https")
#' - `background_writes`: logical, whether `OutputStream` writes will be issued
#'    in the background, without blocking (default `TRUE`)
#'
#' @section Methods:
#'
#' - `$GetFileInfo(x)`: `x` may be a [FileSelector][FileSelector] or a character
#'    vector of paths. Returns a list of [FileInfo][FileInfo]
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
FileSystem <- R6Class("FileSystem", inherit = ArrowObject,
  public = list(
    ..dispatch = function() {
      type_name <- self$type_name
      if (type_name == "local") {
        shared_ptr(LocalFileSystem, self$pointer())
      } else if (type_name == "s3") {
        shared_ptr(S3FileSystem, self$pointer())
      } else if (type_name == "subtree") {
        shared_ptr(SubTreeFileSystem, self$pointer())
      } else {
        self
      }
    },
    GetFileInfo = function(x) {
      if (inherits(x, "FileSelector")) {
        map(
          fs___FileSystem__GetTargetInfos_FileSelector(self, x),
          shared_ptr,
          class = FileInfo
        )
      } else if (is.character(x)){
        map(
          fs___FileSystem__GetTargetInfos_Paths(self, clean_path_rel(x)),
          shared_ptr,
          class = FileInfo
        )
      } else {
        abort("incompatible type for FileSystem$GetFileInfo()")
      }
    },

    CreateDir = function(path, recursive = TRUE) {
      fs___FileSystem__CreateDir(self, clean_path_rel(path), isTRUE(recursive))
    },

    DeleteDir = function(path) {
      fs___FileSystem__DeleteDir(self, clean_path_rel(path))
    },

    DeleteDirContents = function(path) {
      fs___FileSystem__DeleteDirContents(self, clean_path_rel(path))
    },

    DeleteFile = function(path) {
      fs___FileSystem__DeleteFile(self, clean_path_rel(path))
    },

    DeleteFiles = function(paths) {
      fs___FileSystem__DeleteFiles(self, clean_path_rel(paths))
    },

    Move = function(src, dest) {
      fs___FileSystem__Move(self, clean_path_rel(src), clean_path_rel(dest))
    },

    CopyFile = function(src, dest) {
      fs___FileSystem__CopyFile(self, clean_path_rel(src), clean_path_rel(dest))
    },

    OpenInputStream = function(path) {
      shared_ptr(InputStream, fs___FileSystem__OpenInputStream(self, clean_path_rel(path)))
    },
    OpenInputFile = function(path) {
      shared_ptr(RandomAccessFile, fs___FileSystem__OpenInputFile(self, clean_path_rel(path)))
    },
    OpenOutputStream = function(path) {
      shared_ptr(OutputStream, fs___FileSystem__OpenOutputStream(self, clean_path_rel(path)))
    },
    OpenAppendStream = function(path) {
      shared_ptr(OutputStream, fs___FileSystem__OpenAppendStream(self, clean_path_rel(path)))
    }
  ),
  active = list(
    type_name = function() fs___FileSystem__type_name(self)
  )
)
FileSystem$from_uri <- function(uri) {
  assert_that(is.string(uri))
  out <- fs___FileSystemFromUri(uri)
  out$fs <- shared_ptr(FileSystem, out$fs)$..dispatch()
  out
}

get_path_and_filesystem <- function(x, filesystem = NULL) {
  # Wrapper around FileSystem$from_uri that handles local paths
  # and an optional explicit filesystem
  assert_that(is.string(x))
  if (is_url(x)) {
    if (!is.null(filesystem)) {
      # Stop? Can't have URL (which yields a fs) and another fs
    }
    FileSystem$from_uri(x)
  } else {
    list(
      fs = filesystem %||% LocalFileSystem$create(),
      path = clean_path_abs(x)
    )
  }
}

is_url <- function(x) grepl("://", x)

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
#' @importFrom utils modifyList
#' @export
S3FileSystem <- R6Class("S3FileSystem", inherit = FileSystem)
S3FileSystem$create <- function(anonymous = FALSE, ...) {
  args <- list2(...)
  if (anonymous) {
    invalid_args <- intersect(c("access_key", "secret_key", "session_token", "role_arn", "session_name", "external_id", "load_frequency"), names(args))
    if (length(invalid_args)) {
      stop("Cannot specify ", oxford_paste(invalid_args), " when anonymous = TRUE", call. = FALSE)
    }
  } else {
    keys_present <- length(intersect(c("access_key", "secret_key"), names(args)))
    if (keys_present == 1) {
      stop("Key authentication requires both access_key and secret_key", call. = FALSE)
    }
    if ("session_token" %in% names(args) && keys_present != 2) {
      stop(
        "In order to initialize a session with temporary credentials, ",
        "both secret_key and access_key must be provided ",
        "in addition to session_token.",
        call. = FALSE
      )
    }
    arn <- "role_arn" %in% names(args)
    if (keys_present == 2 && arn) {
      stop("Cannot provide both key authentication and role_arn", call. = FALSE)
    }
    arn_extras <- intersect(c("session_name", "external_id", "load_frequency"), names(args))
    if (length(arn_extras) > 0 && !arn) {
      stop("Cannot specify ", oxford_paste(arn_extras), " without providing a role_arn string", call. = FALSE)
    }
  }
  args <- c(modifyList(default_s3_options, args), anonymous = anonymous)
  shared_ptr(S3FileSystem, exec(fs___S3FileSystem__create, !!!args))
}

default_s3_options <- list(
  access_key = "",
  secret_key = "",
  session_token = "",
  role_arn = "",
  session_name = "",
  external_id = "",
  load_frequency = 900L,
  region = "",
  endpoint_override = "",
  scheme = "",
  background_writes = TRUE
)

arrow_with_s3 <- function() {
  .Call(`_s3_available`)
}

#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @export
SubTreeFileSystem <- R6Class("SubTreeFileSystem", inherit = FileSystem)
SubTreeFileSystem$create <- function(base_path, base_fs = NULL) {
  fs_and_path <- get_path_and_filesystem(base_path, base_fs)
  shared_ptr(
    SubTreeFileSystem,
    fs___SubTreeFileSystem__create(fs_and_path$path, fs_and_path$fs)
  )
}

#' Copy files, including between FileSystems
#'
#' @param src_fs The FileSystem from which files will be copied.
#' @param src_sel A FileSelector indicating which files should be copied.
#' A string may also be passed, which is used as the base dir for recursive
#' selection.
#' @param dest_fs The FileSystem into which files will be copied.
#' @param dest_base_dir Where the copied files should be placed.
#' Directories will be created as necessary.
#' @param chunk_size The maximum size of block to read before flushing
#' to the destination file. A larger chunk_size will use more memory while
#' copying but may help accommodate high latency FileSystems.
copy_files <- function(src_fs = LocalFileSystem$create(),
                       src_sel,
                       dest_fs = LocalFileSystem$create(),
                       dest_base_dir,
                       chunk_size = 1024L * 1024L) {
  if (!inherits(src_sel, "FileSelector")) {
    src_sel <- FileSelector$create(src_sel, recursive = TRUE)
  }
  fs___CopyFiles(src_fs, src_sel, dest_fs, dest_base_dir,
                 chunk_size, option_use_threads())
}

clean_path_abs <- function(path) {
  # Make sure we have a valid, absolute, forward-slashed path for passing to Arrow
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

clean_path_rel <- function(path) {
  # Make sure all path separators are "/", not "\" as on Windows
  path_sep <- ifelse(tolower(Sys.info()[["sysname"]]) == "windows", "\\\\", "/")
  gsub(path_sep, "/", path)
}
