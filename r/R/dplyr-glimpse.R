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

#' @importFrom utils getFromNamespace
glimpse.ArrowTabular <- function(x,
                                 width = getOption("pillar.width", getOption("width")),
                                 ...) {
  # We use cli:: and pillar:: throughout this function. We don't need to check
  # to see if they're installed because dplyr depends on pillar, which depends
  # on cli, and we're only in this function though S3 dispatch on dplyr::glimpse
  if (!is.finite(width)) {
    abort("`width` must be finite.")
  }

  # Even though this is the ArrowTabular method, we use it for arrow_dplyr_query
  # too, so let's make some adaptations that aren't covered by S3 methods
  if (inherits(x, "arrow_dplyr_query")) {
    # TODO(ARROW-16030): encapsulate this
    schema <- implicit_schema(x)
    class_title <- paste(source_data(x)$class_title(), "(query)")
  } else {
    schema <- x$schema
    class_title <- x$class_title()
  }

  cli::cat_line(class_title)

  dims <- dim(x)
  # We need a couple of internal functions in pillar for formatting
  pretty_int <- getFromNamespace("big_mark", "pillar")
  cli::cat_line(sprintf(
    "%s rows x %s columns", pretty_int(dims[1]), pretty_int(dims[2])
  ))

  if (dims[2] == 0) {
    return(invisible(x))
  }

  var_types <- map_chr(schema$fields, ~ format(pillar::new_pillar_type(.$type)))
  # note: pillar:::tick_if_needed() is in glimplse.tbl()
  var_headings <- paste("$", center_pad(names(x), var_types))

  nrows <- as.integer(width / 3)
  df <- as.data.frame(head(x, nrows))
  formatted_data <- map_chr(df, function(.) {
    tryCatch(
      paste(pillar::format_glimpse(.), collapse = ", "),
      # This could error e.g. if you have a VctrsExtensionType and the package
      # that defines methods for the data is not loaded
      error = function(e) conditionMessage(e)
    )
  })

  data_width <- width - pillar::get_extent(var_headings)
  make_shorter <- getFromNamespace("str_trunc", "pillar")
  truncated_data <- make_shorter(formatted_data, data_width)

  cli::cat_line(var_headings, " ", truncated_data)
  if (inherits(x, "arrow_dplyr_query")) {
    cli::cat_line("Call `print()` for query details")
  } else if (any(grepl("<...>", var_types, fixed = TRUE)) || schema$HasMetadata) {
    # TODO: use crayon to style?
    # TODO(ARROW-16030): this could point to the schema method
    cli::cat_line("Call `print()` for full schema details")
  }
  invisible(x)
}

# Dataset has an efficient head() method via Scanner so this is fine
glimpse.Dataset <- glimpse.ArrowTabular

glimpse.arrow_dplyr_query <- function(x,
                                      width = getOption("pillar.width", getOption("width")),
                                      ...) {
  source <- source_data(x)
  # TODO(ARROW-XXXXX): this should check for RBRs in other source nodes too
  if (inherits(source, "RecordBatchReader")) {
    message("Cannot glimpse() data from a RecordBatchReader because it can only be read one time. Call `compute()` to evaluate the query first.")
    print(x)
  } else if (query_on_dataset(x) && (is_collapsed(x) || has_aggregation(x) || length(x$arrange_vars))) {
    # We allow queries that just select/filter/mutate because those stream,
    # no arrange/summarize/join/etc. because those require full scans.
    # TODO: tangentially related, test that head %>% head is handled correctly
    message("This query requires a full table scan, so glimpse() may be expensive. Call `compute()` to evaluate the query first.")
    print(x)
  } else {
    # Go for it
    glimpse.ArrowTabular(x, width = width, ...)
  }
}

glimpse.RecordBatchReader <- function(x,
                                      width = getOption("pillar.width", getOption("width")),
                                      ...) {
  # TODO(ARROW-YYYYY): to_arrow() on duckdb con should hold con not RBR so it can be run more than onces (like duckdb does on the other side)
  message("Cannot glimpse() data from a RecordBatchReader because it can only be read one time; call `as_arrow_table()` to consume it first")
  print(x)
}

glimpse.ArrowDatum <- function(x, width, ...) {
  cli::cat_line(gsub("[ \n]+", " ", x$ToString()))
}

type_sum.DataType <- function(x) {
  if (inherits(x, "VctrsExtensionType")) {
    # ptype() holds a vctrs type object, which pillar knows how to format
    paste0("ext<", pillar::type_sum(x$ptype()), ">")
  } else {
    # Trim long type names with <...>
    sub("<.*>", "<...>", x$ToString())
  }
}

center_pad <- function(left, right) {
  left_sizes <- pillar::get_extent(left)
  right_sizes <- pillar::get_extent(right)
  total_width <- max(left_sizes + right_sizes) + 1L
  paste0(left, strrep(" ", total_width - left_sizes - right_sizes), right)
}
