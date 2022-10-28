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

#' Report information on the package's capabilities
#'
#' This function summarizes a number of build-time configurations and run-time
#' settings for the Arrow package. It may be useful for diagnostics.
#' @return `arrow_info()` returns a list including version information, boolean
#' "capabilities", and statistics from Arrow's memory allocator, and also
#' Arrow's run-time information. The `_available()` functions return a logical
#' value whether or not the C++ library was built with support for them.
#' @export
#' @importFrom utils packageVersion
#' @seealso If any capabilities are `FALSE`, see the
#' \href{https://arrow.apache.org/docs/r/articles/install.html}{install guide}
#' for guidance on reinstalling the package.
arrow_info <- function() {
  opts <- options()
  pool <- default_memory_pool()
  runtimeinfo <- runtime_info()
  buildinfo <- build_info()
  compute_funcs <- list_compute_functions()

  out <- list(
    version = packageVersion("arrow"),
    options = opts[grep("^arrow\\.", names(opts))],
    capabilities = c(
      dataset = arrow_with_dataset(),
      substrait = arrow_with_substrait(),
      parquet = arrow_with_parquet(),
      json = arrow_with_json(),
      s3 = arrow_with_s3(),
      gcs = arrow_with_gcs(),
      utf8proc = "utf8_upper" %in% compute_funcs,
      re2 = "replace_substring_regex" %in% compute_funcs,
      vapply(tolower(names(CompressionType)[-1]), codec_is_available, logical(1))
    ),
    memory_pool = list(
      backend_name = pool$backend_name,
      bytes_allocated = pool$bytes_allocated,
      max_memory = pool$max_memory,
      available_backends = supported_memory_backends()
    ),
    runtime_info = list(
      simd_level = runtimeinfo[1],
      detected_simd_level = runtimeinfo[2]
    ),
    build_info = list(
      cpp_version = buildinfo[1],
      cpp_compiler = buildinfo[2],
      cpp_compiler_version = buildinfo[3],
      cpp_compiler_flags = buildinfo[4],
      # git_id is "" if not built from a git checkout
      # convert that to NULL
      git_id = if (nzchar(buildinfo[5])) buildinfo[5]
    )
  )
  structure(out, class = "arrow_info")
}

#' @rdname arrow_info
#' @export
arrow_available <- function() {
  .Deprecated(msg = "Arrow C++ is always available as of 7.0.0")
  TRUE
}

#' @rdname arrow_info
#' @export
arrow_with_dataset <- function() {
  tryCatch(.Call(`_dataset_available`), error = function(e) {
    return(FALSE)
  })
}

#' @rdname arrow_info
#' @export
arrow_with_substrait <- function() {
  tryCatch(.Call(`_substrait_available`), error = function(e) {
    return(FALSE)
  })
}

#' @rdname arrow_info
#' @export
arrow_with_parquet <- function() {
  tryCatch(.Call(`_parquet_available`), error = function(e) {
    return(FALSE)
  })
}

#' @rdname arrow_info
#' @export
arrow_with_s3 <- function() {
  tryCatch(.Call(`_s3_available`), error = function(e) {
    return(FALSE)
  })
}

#' @rdname arrow_info
#' @export
arrow_with_gcs <- function() {
  tryCatch(.Call(`_gcs_available`), error = function(e) {
    return(FALSE)
  })
}

#' @rdname arrow_info
#' @export
arrow_with_json <- function() {
  tryCatch(.Call(`_json_available`), error = function(e) {
    return(FALSE)
  })
}

some_features_are_off <- function(features) {
  # `features` is a named logical vector (as in arrow_info()$capabilities)
  # Let's exclude some less relevant ones
  blocklist <- c("lzo", "bz2", "brotli", "substrait")
  # Return TRUE if any of the other features are FALSE
  !all(features[setdiff(names(features), blocklist)])
}

#' @export
print.arrow_info <- function(x, ...) {
  print_key_values <- function(title, vals, ...) {
    # Make a key-value table for printing, no column names
    df <- data.frame(vals, stringsAsFactors = FALSE, ...)
    names(df) <- ""

    cat(title, ":\n", sep = "")
    print(df)
    cat("\n")
  }
  cat("Arrow package version: ", format(x$version), "\n\n", sep = "")
  print_key_values("Capabilities", c(
    x$capabilities,
    jemalloc = "jemalloc" %in% x$memory_pool$available_backends,
    mimalloc = "mimalloc" %in% x$memory_pool$available_backends
  ))
  if (some_features_are_off(x$capabilities) && identical(tolower(Sys.info()[["sysname"]]), "linux")) {
    # Only on linux because (e.g.) we disable certain features on purpose on rtools35
    cat(
      "To reinstall with more optional capabilities enabled, see\n",
      "  https://arrow.apache.org/docs/r/articles/install.html\n\n"
    )
  }

  if (length(x$options)) {
    print_key_values("Arrow options()", map_chr(x$options, format))
  }

  format_bytes <- function(b, units = "auto", digits = 2L, ...) {
    format(structure(b, class = "object_size"), units = units, digits = digits, ...)
  }
  print_key_values("Memory", c(
    Allocator = x$memory_pool$backend_name,
    # utils:::format.object_size is not properly vectorized
    Current = format_bytes(x$memory_pool$bytes_allocated, ...),
    Max = format_bytes(x$memory_pool$max_memory, ...)
  ))
  print_key_values("Runtime", c(
    `SIMD Level` = x$runtime_info$simd_level,
    `Detected SIMD Level` = x$runtime_info$detected_simd_level
  ))
  print_key_values("Build", c(
    `C++ Library Version` = x$build_info$cpp_version,
    `C++ Compiler` = x$build_info$cpp_compiler,
    `C++ Compiler Version` = x$build_info$cpp_compiler_version,
    `Git ID` = x$build_info$git_id
  ))

  invisible(x)
}
