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

# for compatibility with R versions earlier than 4.0.0
if (!exists("deparse1")) {
  deparse1 <- function (expr, collapse = " ", width.cutoff = 500L, ...) {
    paste(deparse(expr, width.cutoff, ...), collapse = collapse)
  }
}

oxford_paste <- function(x, conjunction = "and", quote = TRUE) {
  if (quote && is.character(x)) {
    x <- paste0('"', x, '"')
  }
  if (length(x) < 2) {
    return(x)
  }
  x[length(x)] <- paste(conjunction, x[length(x)])
  if (length(x) > 2) {
    return(paste(x, collapse = ", "))
  } else {
    return(paste(x, collapse = " "))
  }
}

assert_is <- function(object, class) {
  msg <- paste(substitute(object), "must be a", oxford_paste(class, "or"))
  assert_that(inherits(object, class), msg = msg)
}

assert_is_list_of <- function(object, class) {
  msg <- paste(substitute(object), "must be a list of", oxford_paste(class, "or"))
  assert_that(is_list_of(object, class), msg = msg)
}

is_list_of <- function(object, class) {
  is.list(object) && all(map_lgl(object, ~inherits(., class)))
}

empty_named_list <- function() structure(list(), .Names = character(0))

r_symbolic_constants <- c(
  "pi", "TRUE", "FALSE", "NULL", "Inf", "NA", "NaN",
  "NA_integer_", "NA_real_", "NA_complex_", "NA_character_"
)

is_function <- function(expr, name) {
  if (!is.call(expr)) {
    return(FALSE)
  } else {
    if (deparse1(expr[[1]]) == name) {
      return(TRUE)
    }
    out <- lapply(expr, is_function, name)
  }
  any(vapply(out, isTRUE, TRUE))
}

all_funs <- function(expr) {
  names <- all_names(expr)
  names[vapply(names, function(name) {is_function(expr, name)}, TRUE)]
}

all_vars <- function(expr) {
  setdiff(all.vars(expr), r_symbolic_constants)
}

all_names <- function(expr) {
  setdiff(all.names(expr), r_symbolic_constants)
}

is_constant <- function(expr) {
  length(all_vars(expr)) == 0
}
