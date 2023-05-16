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

#' @include arrow-object.R
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
  fs___FileSelector__create(clean_path_rel(base_dir), allow_not_found, recursive)
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
#' - `allow_bucket_creation`: logical, if TRUE, the filesystem will create
#'    buckets if `$CreateDir()` is called on the bucket level (default `FALSE`).
#' - `allow_bucket_deletion`: logical, if TRUE, the filesystem will delete
#'    buckets if`$DeleteDir()` is called on the bucket level (default `FALSE`).
#' - `request_timeout`: Socket read time on Windows and MacOS in seconds. If
#'    negative, the AWS SDK default (typically 3 seconds).
#' - `connect_timeout`: Socket connection timeout in seconds. If negative, AWS
#'    SDK default is used (typically 1 second).
#'
#' `GcsFileSystem$create()` optionally takes arguments:
#'
#' - `anonymous`: logical, default `FALSE`. If true, will not attempt to look up
#'    credentials using standard GCS configuration methods.
#' - `access_token`: optional string for authentication. Should be provided along
#'   with `expiration`
#' - `expiration`: `POSIXct`. optional datetime representing point at which
#'   `access_token` will expire.
#' - `json_credentials`: optional string for authentication. Either a string
#'   containing JSON credentials or a path to their location on the filesystem.
#'   If a path to credentials is given, the file should be UTF-8 encoded.
#' - `endpoint_override`: if non-empty, will connect to provided host name / port,
#'   such as "localhost:9001", instead of default GCS ones. This is primarily useful
#'   for testing purposes.
#' - `scheme`: connection transport (default "https")
#' - `default_bucket_location`: the default location (or "region") to create new
#'   buckets in.
#' - `retry_limit_seconds`: the maximum amount of time to spend retrying if
#'   the filesystem encounters errors. Default is 15 seconds.
#' - `default_metadata`: default metadata to write in new objects.
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
#' @section Active bindings:
#'
#' - `$type_name`: string filesystem type name, such as "local", "s3", etc.
#' - `$region`: string AWS region, for `S3FileSystem` and `SubTreeFileSystem`
#'    containing a `S3FileSystem`
#' - `$base_fs`: for `SubTreeFileSystem`, the `FileSystem` it contains
#' - `$base_path`: for `SubTreeFileSystem`, the path in `$base_fs` which is considered
#'    root in this `SubTreeFileSystem`.
#'
#' @section Notes:
#'
#' On S3FileSystem, `$CreateDir()` on a top-level directory creates a new bucket.
#' When S3FileSystem creates new buckets (assuming allow_bucket_creation is TRUE),
#' it does not pass any non-default settings. In AWS S3, the bucket and all
#' objects will be not publicly visible, and will have no bucket policies
#' and no resource tags. To have more control over how buckets are created,
#' use a different API to create them.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @rdname FileSystem
#' @name FileSystem
#' @export
FileSystem <- R6Class("FileSystem",
  inherit = ArrowObject,
  public = list(
    GetFileInfo = function(x) {
      if (inherits(x, "FileSelector")) {
        fs___FileSystem__GetTargetInfos_FileSelector(self, x)
      } else if (is.character(x)) {
        fs___FileSystem__GetTargetInfos_Paths(self, clean_path_rel(x))
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
      fs___FileSystem__OpenInputStream(self, clean_path_rel(path))
    },
    OpenInputFile = function(path) {
      fs___FileSystem__OpenInputFile(self, clean_path_rel(path))
    },
    OpenOutputStream = function(path) {
      fs___FileSystem__OpenOutputStream(self, clean_path_rel(path))
    },
    OpenAppendStream = function(path) {
      fs___FileSystem__OpenAppendStream(self, clean_path_rel(path))
    },

    # Friendlier R user interface
    path = function(x) SubTreeFileSystem$create(x, self),
    cd = function(x) SubTreeFileSystem$create(x, self),
    ls = function(path = "", ...) {
      selector <- FileSelector$create(path, ...) # ... for recursive = TRUE
      infos <- self$GetFileInfo(selector)
      map_chr(infos, ~ .$path)
      # TODO: add full.names argument like base::dir() (default right now is TRUE)
      # TODO: see fs package for glob/regexp filtering
      # TODO: verbose method that shows other attributes as df
      # TODO: print methods for FileInfo, SubTreeFileSystem, S3FileSystem
    }
  ),
  active = list(
    type_name = function() fs___FileSystem__type_name(self),
    url_scheme = function() {
      fs_type_name <- self$type_name
      if (identical(fs_type_name, "subtree")) {
        # Recurse
        return(self$base_fs$url_scheme)
      }
      # Some type_names are the url scheme but others aren't
      type_map <- list(
        local = "file",
        gcs = "gs"
      )
      type_map[[fs_type_name]] %||% fs_type_name
    }
  )
)
FileSystem$from_uri <- function(uri) {
  assert_that(is.string(uri))
  fs___FileSystemFromUri(uri)
}

get_paths_and_filesystem <- function(x, filesystem = NULL) {
  # Wrapper around FileSystem$from_uri that handles local paths
  # and an optional explicit filesystem
  if (inherits(x, "SubTreeFileSystem")) {
    return(list(fs = x$base_fs, path = x$base_path))
  }
  assert_that(is.character(x))
  are_urls <- are_urls(x)
  if (any(are_urls)) {
    if (!all(are_urls)) {
      stop("Vectors of mixed paths and URIs are not supported", call. = FALSE)
    }
    if (!is.null(filesystem)) {
      # Stop? Can't have URL (which yields a fs) and another fs
    }
    x <- lapply(x, FileSystem$from_uri)
    if (length(unique(map(x, ~ class(.$fs)))) > 1) {
      stop(
        "Vectors of URIs for different file systems are not supported",
        call. = FALSE
      )
    }
    fs <- x[[1]]$fs
    path <- map_chr(x, ~ .$path) # singular name "path" used for compatibility
  } else {
    fs <- filesystem %||% LocalFileSystem$create()
    if (inherits(fs, "LocalFileSystem")) {
      path <- clean_path_abs(x)
    } else {
      path <- clean_path_rel(x)
    }
  }
  list(
    fs = fs,
    path = path
  )
}

# variant of the above function that asserts that x is either a scalar string
# or a SubTreeFileSystem
get_path_and_filesystem <- function(x, filesystem = NULL) {
  assert_that(is.string(x) || inherits(x, "SubTreeFileSystem"))
  get_paths_and_filesystem(x, filesystem)
}

is_url <- function(x) is.string(x) && grepl("://", x)
is_http_url <- function(x) is_url(x) && grepl("^http", x)
are_urls <- function(x) if (!is.character(x)) FALSE else grepl("://", x)

#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @export
LocalFileSystem <- R6Class("LocalFileSystem", inherit = FileSystem)
LocalFileSystem$create <- function() {
  fs___LocalFileSystem__create()
}

#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @importFrom utils modifyList
#' @export
S3FileSystem <- R6Class("S3FileSystem",
  inherit = FileSystem,
  active = list(
    region = function() fs___S3FileSystem__region(self)
  )
)
S3FileSystem$create <- function(anonymous = FALSE, ...) {
  args <- list2(...)
  if (anonymous) {
    invalid_args <- intersect(
      c(
        "access_key", "secret_key", "session_token", "role_arn", "session_name",
        "external_id", "load_frequency", "allow_bucket_creation", "allow_bucket_deletion"
      ),
      names(args)
    )
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
  exec(fs___S3FileSystem__create, !!!args)
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
  proxy_options = "",
  background_writes = TRUE,
  allow_bucket_creation = FALSE,
  allow_bucket_deletion = FALSE,
  connect_timeout = -1,
  request_timeout = -1
)

#' Connect to an AWS S3 bucket
#'
#' `s3_bucket()` is a convenience function to create an `S3FileSystem` object
#' that automatically detects the bucket's AWS region and holding onto the its
#' relative path.
#'
#' @param bucket string S3 bucket name or path
#' @param ... Additional connection options, passed to `S3FileSystem$create()`
#' @return A `SubTreeFileSystem` containing an `S3FileSystem` and the bucket's
#' relative path. Note that this function's success does not guarantee that you
#' are authorized to access the bucket's contents.
#' @examplesIf FALSE
#' bucket <- s3_bucket("voltrondata-labs-datasets")
#' @export
s3_bucket <- function(bucket, ...) {
  assert_that(is.string(bucket))
  args <- list2(...)

  # If user specifies args, they must specify region as arg, env var, or config
  if (length(args) == 0) {
    # Use FileSystemFromUri to detect the bucket's region
    if (!is_url(bucket)) {
      bucket <- paste0("s3://", bucket)
    }

    fs_and_path <- FileSystem$from_uri(bucket)
    fs <- fs_and_path$fs
  } else {
    # If there are no additional S3Options, we can use that filesystem
    fs <- exec(S3FileSystem$create, !!!args)
  }

  # Return a subtree pointing at that bucket path
  SubTreeFileSystem$create(bucket, fs)
}

#' Connect to a Google Cloud Storage (GCS) bucket
#'
#' `gs_bucket()` is a convenience function to create an `GcsFileSystem` object
#' that holds onto its relative path
#'
#' @param bucket string GCS bucket name or path
#' @param ... Additional connection options, passed to `GcsFileSystem$create()`
#' @return A `SubTreeFileSystem` containing an `GcsFileSystem` and the bucket's
#' relative path. Note that this function's success does not guarantee that you
#' are authorized to access the bucket's contents.
#' @examplesIf FALSE
#' bucket <- gs_bucket("voltrondata-labs-datasets")
#' @export
gs_bucket <- function(bucket, ...) {
  assert_that(is.string(bucket))
  args <- list2(...)

  fs <- exec(GcsFileSystem$create, !!!args)

  SubTreeFileSystem$create(bucket, fs)
}

#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @export
GcsFileSystem <- R6Class("GcsFileSystem",
  inherit = FileSystem,
  active = list(
    options = function() {
      out <- fs___GcsFileSystem__options(self)

      # Convert from nanoseconds to POSIXct w/ UTC tz
      if ("expiration" %in% names(out)) {
        out$expiration <- as.POSIXct(
          out$expiration / 1000000000, origin = "1970-01-01", tz = "UTC"
        )
      }

      out
    }
  )
)
GcsFileSystem$create <- function(anonymous = FALSE, retry_limit_seconds = 15, ...) {
  # The default retry limit in C++ is 15 minutes, but that is experienced as
  # hanging in an interactive context, so default is set here to 15 seconds.
  options <- list(...)

  # Validate options
  if (isTRUE(anonymous)) {
    invalid_args <- intersect(
      c("access_token", "expiration", "json_credentials"),
      names(options)
    )
    if (length(invalid_args)) {
      stop(
        "Cannot specify ",
        oxford_paste(invalid_args),
        " when anonymous = TRUE",
        call. = FALSE
      )
    }
  } else {
    token_args <- intersect(c("access_token", "expiration"), names(options))
    if (!is.null(options[["json_credentials"]]) && length(token_args) > 0) {
      stop("Cannot provide access_token with json_credentials", call. = FALSE)
    } else if (length(token_args) == 1) {
      stop("token auth requires both 'access_token' and 'expiration'", call. = FALSE)
    }
  }

  valid_opts <- c(
    "access_token", "expiration", "json_credentials", "endpoint_override",
    "scheme", "default_bucket_location", "default_metadata"
  )

  invalid_opts <- setdiff(names(options), valid_opts)
  if (length(invalid_opts)) {
    stop(
      "Invalid options for GcsFileSystem: ",
      oxford_paste(invalid_opts),
      call. = FALSE
    )
  }

  # Stop if expiration isn't a POSIXct
  if ("expiration" %in% names(options) && !inherits(options$expiration, "POSIXct")) {
    stop(
      paste(
        "Option 'expiration' must be of class POSIXct, not",
        class(options$expiration)[[1]]),
      call. = FALSE)
  }

  options$retry_limit_seconds <- retry_limit_seconds

  # Handle reading json_credentials from the filesystem
  if ("json_credentials" %in% names(options) && file.exists(options[["json_credentials"]])) {
    options[["json_credentials"]] <- paste(read_file_utf8(options[["json_credentials"]]), collapse = "\n")
  }

  fs___GcsFileSystem__Make(anonymous, options)
}

#' @usage NULL
#' @format NULL
#' @rdname FileSystem
#' @export
SubTreeFileSystem <- R6Class("SubTreeFileSystem",
  inherit = FileSystem,
  public = list(
    print = function(...) {
      cat(
        "SubTreeFileSystem: ",
        self$url_scheme, "://", self$base_path, "\n",
        sep = ""
      )
      invisible(self)
    }
  ),
  active = list(
    base_fs = function() {
      fs___SubTreeFileSystem__base_fs(self)
    },
    base_path = function() fs___SubTreeFileSystem__base_path(self)
  )
)
SubTreeFileSystem$create <- function(base_path, base_fs = NULL) {
  fs_and_path <- get_path_and_filesystem(base_path, base_fs)
  fs___SubTreeFileSystem__create(fs_and_path$path, fs_and_path$fs)
}

#' @export
`$.SubTreeFileSystem` <- function(x, name, ...) {
  # This is to allow delegating methods/properties to the base_fs
  assert_that(is.string(name))
  if (name %in% ls(envir = x)) {
    get(name, x)
  } else if (name %in% ls(envir = x$base_fs)) {
    get(name, x$base_fs)
  } else {
    NULL
  }
}

#' Copy files between FileSystems
#'
#' @param from A string path to a local directory or file, a URI, or a
#' `SubTreeFileSystem`. Files will be copied recursively from this path.
#' @param to A string path to a local directory or file, a URI, or a
#' `SubTreeFileSystem`. Directories will be created as necessary
#' @param chunk_size The maximum size of block to read before flushing
#' to the destination file. A larger chunk_size will use more memory while
#' copying but may help accommodate high latency FileSystems.
#' @return Nothing: called for side effects in the file system
#' @export
#' @examplesIf FALSE
#' # Copy an S3 bucket's files to a local directory:
#' copy_files("s3://your-bucket-name", "local-directory")
#' # Using a FileSystem object
#' copy_files(s3_bucket("your-bucket-name"), "local-directory")
#' # Or go the other way, from local to S3
#' copy_files("local-directory", s3_bucket("your-bucket-name"))
copy_files <- function(from, to, chunk_size = 1024L * 1024L) {
  from <- get_path_and_filesystem(from)
  to <- get_path_and_filesystem(to)
  invisible(fs___CopyFiles(
    from$fs,
    FileSelector$create(from$path, recursive = TRUE),
    to$fs,
    to$path,
    chunk_size,
    option_use_threads()
  ))
}

clean_path_abs <- function(path) {
  # Make sure we have a valid, absolute, forward-slashed path for passing to Arrow
  enc2utf8(normalizePath(path, winslash = "/", mustWork = FALSE))
}

clean_path_rel <- function(path) {
  # Make sure all path separators are "/", not "\" as on Windows
  path_sep <- ifelse(tolower(Sys.info()[["sysname"]]) == "windows", "\\\\", "/")
  gsub(path_sep, "/", path)
}

read_file_utf8 <- function(file) {
  res <- readBin(file, "raw", n = file.size(file))
  res <- rawToChar(res)
  Encoding(res) <- "UTF-8"
  res
}
