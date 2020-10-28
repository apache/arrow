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

#' @importFrom R6 R6Class
#' @importFrom purrr as_mapper map map2 map_chr map_dfr map_int map_lgl keep
#' @importFrom assertthat assert_that is.string
#' @importFrom rlang list2 %||% is_false abort dots_n warn enquo quo_is_null enquos is_integerish quos eval_tidy new_data_mask syms env env_bind as_label set_names exec is_bare_character
#' @importFrom tidyselect vars_select
#' @useDynLib arrow, .registration = TRUE
#' @keywords internal
"_PACKAGE"

#' @importFrom vctrs s3_register vec_size vec_cast
.onLoad <- function(...) {
  dplyr_methods <- paste0(
    "dplyr::",
    c(
      "select", "filter", "collect", "summarise", "group_by", "groups",
      "group_vars", "ungroup", "mutate", "arrange", "rename", "pull"
    )
  )
  for (cl in c("Dataset", "RecordBatch", "Table", "arrow_dplyr_query")) {
    for (m in dplyr_methods) {
      s3_register(m, cl)
    }
  }

  s3_register("dplyr::tbl_vars", "arrow_dplyr_query")
  s3_register("reticulate::py_to_r", "pyarrow.lib.Array")
  s3_register("reticulate::py_to_r", "pyarrow.lib.RecordBatch")
  s3_register("reticulate::py_to_r", "pyarrow.lib.ChunkedArray")
  s3_register("reticulate::py_to_r", "pyarrow.lib.Table")
  s3_register("reticulate::r_to_py", "Array")
  s3_register("reticulate::r_to_py", "RecordBatch")
  s3_register("reticulate::r_to_py", "ChunkedArray")
  s3_register("reticulate::r_to_py", "Table")
  invisible()
}

#' Is the C++ Arrow library available?
#'
#' You won't generally need to call these function, but they're made available
#' for diagnostic purposes.
#' @return `TRUE` or `FALSE` depending on whether the package was installed
#' with the Arrow C++ library (check with `arrow_available()`) or with S3
#' support enabled (check with `arrow_with_s3()`).
#' @export
#' @examples
#' arrow_available()
#' arrow_with_s3()
#' @seealso If either of these are `FALSE`, see
#' `vignette("install", package = "arrow")` for guidance on reinstalling the
#' package.
arrow_available <- function() {
  .Call(`_arrow_available`)
}

#' @rdname arrow_available
#' @export
arrow_with_s3 <- function() {
  .Call(`_s3_available`)
}

option_use_threads <- function() {
  !is_false(getOption("arrow.use_threads"))
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
      if (!is.null(self$ToString)){
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

