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

#' @importFrom stats quantile median na.omit na.exclude na.pass na.fail
#' @importFrom R6 R6Class
#' @importFrom purrr as_mapper map map2 map_chr map2_chr map_dfr map_int map_lgl keep imap_chr
#' @importFrom assertthat assert_that is.string
#' @importFrom rlang list2 %||% is_false abort dots_n warn enquo quo_is_null enquos is_integerish quos eval_tidy new_data_mask syms env new_environment env_bind as_label set_names exec is_bare_character quo_get_expr quo_set_expr .data seq2 is_quosure enexpr enexprs expr caller_env is_character quo_name
#' @importFrom tidyselect vars_pull vars_rename vars_select eval_select
#' @useDynLib arrow, .registration = TRUE
#' @keywords internal
"_PACKAGE"

#' @importFrom vctrs s3_register vec_size vec_cast vec_unique
.onLoad <- function(...) {
  dplyr_methods <- paste0(
    "dplyr::",
    c(
      "select", "filter", "collect", "summarise", "group_by", "groups",
      "group_vars", "group_by_drop_default", "ungroup", "mutate", "transmute",
      "arrange", "rename", "pull", "relocate", "compute"
    )
  )
  for (cl in c("Dataset", "ArrowTabular", "arrow_dplyr_query")) {
    for (m in dplyr_methods) {
      s3_register(m, cl)
    }
  }
  s3_register("dplyr::tbl_vars", "arrow_dplyr_query")

  for (cl in c("Array", "RecordBatch", "ChunkedArray", "Table", "Schema",
               "Field", "DataType", "RecordBatchReader")) {
    s3_register("reticulate::py_to_r", paste0("pyarrow.lib.", cl))
    s3_register("reticulate::r_to_py", cl)
  }

  # Create these once, at package build time
  if (arrow_available()) {
    # Also include all available Arrow Compute functions,
    # namespaced as arrow_fun.
    # We can't do this at install time because list_compute_functions() may error
    all_arrow_funs <- list_compute_functions()
    arrow_funcs <- set_names(
      lapply(all_arrow_funs, function(fun) {
        force(fun)
        function(...) build_expr(fun, ...)
      }),
      paste0("arrow_", all_arrow_funs)
    )
    .cache$functions <- c(nse_funcs, arrow_funcs)
  }
  invisible()
}

.onAttach <- function(libname, pkgname) {
  if (!arrow_available()) {
    msg <- paste(
      "The Arrow C++ library is not available. To retry installation with debug output, run:",
      "    install_arrow(verbose = TRUE)",
      "See https://arrow.apache.org/docs/r/articles/install.html for more guidance and troubleshooting.",
      sep = "\n"
    )
    packageStartupMessage(msg)
  } else {
    # Just to be extra safe, let's wrap this in a try();
    # we don't a failed startup message to prevent the package from loading
    try({
      features <- arrow_info()$capabilities
      # That has all of the #ifdef features, plus the compression libs and the
      # string libraries (but not the memory allocators, they're added elsewhere)
      #
      # Let's print a message if some are off
      if (some_features_are_off(features)) {
        packageStartupMessage("See arrow_info() for available features")
      }
    })
  }
}

#' Is the C++ Arrow library available?
#'
#' You won't generally need to call these function, but they're made available
#' for diagnostic purposes.
#' @return `TRUE` or `FALSE` depending on whether the package was installed
#' with:
#' * The Arrow C++ library (check with `arrow_available()`)
#' * Arrow Dataset support enabled (check with `arrow_with_dataset()`)
#' * Parquet support enabled (check with `arrow_with_parquet()`)
#' * Amazon S3 support enabled (check with `arrow_with_s3()`)
#' @export
#' @examples
#' arrow_available()
#' arrow_with_dataset()
#' arrow_with_parquet()
#' arrow_with_s3()
#' @seealso If any of these are `FALSE`, see
#' `vignette("install", package = "arrow")` for guidance on reinstalling the
#' package.
arrow_available <- function() {
  tryCatch(.Call(`_arrow_available`), error = function(e) return(FALSE))
}

#' @rdname arrow_available
#' @export
arrow_with_dataset <- function() {
  tryCatch(.Call(`_dataset_available`), error = function(e) return(FALSE))
}

#' @rdname arrow_available
#' @export
arrow_with_parquet <- function() {
  tryCatch(.Call(`_parquet_available`), error = function(e) return(FALSE))
}

#' @rdname arrow_available
#' @export
arrow_with_s3 <- function() {
  tryCatch(.Call(`_s3_available`), error = function(e) return(FALSE))
}

option_use_threads <- function() {
  !is_false(getOption("arrow.use_threads"))
}

#' Report information on the package's capabilities
#'
#' This function summarizes a number of build-time configurations and run-time
#' settings for the Arrow package. It may be useful for diagnostics.
#' @return A list including version information, boolean "capabilities", and
#' statistics from Arrow's memory allocator, and also Arrow's run-time
#' information.
#' @export
#' @importFrom utils packageVersion
arrow_info <- function() {
  opts <- options()
  out <- list(
    version = packageVersion("arrow"),
    libarrow = arrow_available(),
    options = opts[grep("^arrow\\.", names(opts))]
  )
  if (out$libarrow) {
    pool <- default_memory_pool()
    runtimeinfo <- runtime_info()
    buildinfo <- build_info()
    compute_funcs <- list_compute_functions()
    out <- c(out, list(
      capabilities = c(
        dataset = arrow_with_dataset(),
        parquet = arrow_with_parquet(),
        s3 = arrow_with_s3(),
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
    ))
  }
  structure(out, class = "arrow_info")
}

some_features_are_off <- function(features) {
  # `features` is a named logical vector (as in arrow_info()$capabilities)
  # Let's exclude some less relevant ones
  blocklist <- c("lzo", "bz2", "brotli")
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
  if (x$libarrow) {
    print_key_values("Capabilities", c(
      x$capabilities,
      jemalloc = "jemalloc" %in% x$memory_pool$available_backends,
      mimalloc = "mimalloc" %in% x$memory_pool$available_backends
    ))
    if (some_features_are_off(x$capabilities) && identical(tolower(Sys.info()[["sysname"]]), "linux")) {
      # Only on linux because (e.g.) we disable certain features on purpose on rtools35 and solaris
      cat("To reinstall with more optional capabilities enabled, see\n  https://arrow.apache.org/docs/r/articles/install.html\n\n")
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
  } else {
    cat("Arrow C++ library not available. See https://arrow.apache.org/docs/r/articles/install.html for troubleshooting.\n")
  }
  invisible(x)
}

option_compress_metadata <- function() {
  !is_false(getOption("arrow.compress_metadata"))
}

#' @include enums.R
ArrowObject <- R6Class("ArrowObject",
  public = list(
    initialize = function(xp) self$set_pointer(xp),

    pointer = function() get(".:xp:.", envir = self),
    `.:xp:.` = NULL,
    set_pointer = function(xp) {
      if (!inherits(xp, "externalptr")) {
        stop(
          class(self)[1], "$new() requires a pointer as input: ",
          "did you mean $create() instead?",
          call. = FALSE
        )
      }
      assign(".:xp:.", xp, envir = self)
    },
    print = function(...) {
      if (!is.null(self$.class_title)) {
        # Allow subclasses to override just printing the class name first
        class_title <- self$.class_title()
      } else {
        class_title <- class(self)[[1]]
      }
      cat(class_title, "\n", sep = "")
      if (!is.null(self$ToString)) {
        cat(self$ToString(), "\n", sep = "")
      }
      invisible(self)
    },

    invalidate = function() {
      assign(".:xp:.", NULL, envir = self)
    }
  )
)

#' @export
`!=.ArrowObject` <- function(lhs, rhs) !(lhs == rhs)

#' @export
`==.ArrowObject` <- function(x, y) {
  x$Equals(y)
}

#' @export
all.equal.ArrowObject <- function(target, current, ..., check.attributes = TRUE) {
  target$Equals(current, check_metadata = check.attributes)
}
