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
#' @importFrom purrr as_mapper map map2 map_chr map2_chr map_dbl map_dfr map_int map_lgl keep imap imap_chr flatten
#' @importFrom assertthat assert_that is.string
#' @importFrom rlang list2 %||% is_false abort dots_n warn enquo quo_is_null enquos is_integerish quos
#' @importFrom rlang eval_tidy new_data_mask syms env new_environment env_bind set_names exec
#' @importFrom rlang is_bare_character quo_get_expr quo_get_env quo_set_expr .data seq2 is_interactive
#' @importFrom rlang expr caller_env is_character quo_name is_quosure enexpr enexprs as_quosure
#' @importFrom rlang is_list call2 is_empty as_function as_label arg_match is_symbol is_call call_args
#' @importFrom rlang quo_set_env quo_get_env is_formula quo_is_call
#' @importFrom tidyselect vars_pull vars_rename vars_select eval_select
#' @importFrom glue glue
#' @useDynLib arrow, .registration = TRUE
#' @keywords internal
"_PACKAGE"

#' @importFrom vctrs s3_register vec_size vec_cast vec_unique
.onLoad <- function(...) {
  # Make sure C++ knows on which thread it is safe to call the R API
  InitializeMainRThread()

  dplyr_methods <- paste0(
    "dplyr::",
    c(
      "select", "filter", "collect", "summarise", "group_by", "groups",
      "group_vars", "group_by_drop_default", "ungroup", "mutate", "transmute",
      "arrange", "rename", "pull", "relocate", "compute", "collapse",
      "distinct", "left_join", "right_join", "inner_join", "full_join",
      "semi_join", "anti_join", "count", "tally", "rename_with", "union",
      "union_all", "glimpse", "show_query", "explain"
    )
  )
  for (cl in c("Dataset", "ArrowTabular", "RecordBatchReader", "arrow_dplyr_query")) {
    for (m in dplyr_methods) {
      s3_register(m, cl)
    }
  }
  s3_register("dplyr::tbl_vars", "arrow_dplyr_query")
  s3_register("pillar::type_sum", "DataType")

  for (cl in c(
    "Array", "RecordBatch", "ChunkedArray", "Table", "Schema",
    "Field", "DataType", "RecordBatchReader"
  )) {
    s3_register("reticulate::py_to_r", paste0("pyarrow.lib.", cl))
    s3_register("reticulate::r_to_py", cl)
  }

  # Create the .cache$functions list at package load time.
  # We can't do this at package build time because list_compute_functions()
  # needs the C++ library loaded
  create_binding_cache()

  if (tolower(Sys.info()[["sysname"]]) == "windows") {
    # Disable multithreading on Windows
    # See https://issues.apache.org/jira/browse/ARROW-8379
    options(arrow.use_threads = FALSE)

    # Try to set timezone database
    configure_tzdb()
  }

  # register extension types that we use internally
  reregister_extension_type(vctrs_extension_type(vctrs::unspecified()))

  invisible()
}

configure_tzdb <- function() {
  # This is needed on Windows to support timezone-aware calculations
  if (requireNamespace("tzdb", quietly = TRUE)) {
    tzdb::tzdb_initialize()
    set_timezone_database(tzdb::tzdb_path("text"))
  } else {
    msg <- paste(
      "The tzdb package is not installed.",
      "Timezones will not be available to Arrow compute functions."
    )
    packageStartupMessage(msg)
  }
}

.onAttach <- function(libname, pkgname) {
  # Just to be extra safe, let's wrap this in a try();
  # we don't want a failed startup message to prevent the package from loading
  try({
    features <- arrow_info()$capabilities
    # That has all of the #ifdef features, plus the compression libs and the
    # string libraries (but not the memory allocators, they're added elsewhere)
    #
    # Let's print a message if some are off
    if (some_features_are_off(features)) {
      packageStartupMessage(
        paste(
          "Some features are not enabled in this build of Arrow.",
          "Run `arrow_info()` for more information."
        )
      )
    }
  })
}

on_old_windows <- function() {
  is_32bit <- .Machine$sizeof.pointer < 8
  is_old_r <- getRversion() < "4.0.0"
  is_windows <- tolower(Sys.info()[["sysname"]]) == "windows"

  is_32bit && is_old_r && is_windows
}


# True when the OS is linux + and the R version is development
# helpful for skipping on Valgrind, and the sanitizer checks (clang + gcc) on cran
on_linux_dev <- function() {
  identical(tolower(Sys.info()[["sysname"]]), "linux") &&
    grepl("devel", R.version.string)
}

option_use_threads <- function() {
  !is_false(getOption("arrow.use_threads"))
}

option_compress_metadata <- function() {
  !is_false(getOption("arrow.compress_metadata"))
}
