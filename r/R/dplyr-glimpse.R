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
  # This function is inspired by pillar:::glimpse.tbl(), with some adaptations

  # We use cli:: and pillar:: throughout this function. We don't need to check
  # to see if they're installed because dplyr depends on pillar, which depends
  # on cli, and we're only in this function though S3 dispatch on dplyr::glimpse
  if (!is.finite(width)) {
    abort("`width` must be finite.")
  }

  # We need a couple of internal functions in pillar for formatting
  pretty_int <- getFromNamespace("big_mark", "pillar")
  make_shorter <- getFromNamespace("str_trunc", "pillar")
  tickify <- getFromNamespace("tick_if_needed", "pillar")

  # Even though this is the ArrowTabular method, we use it for arrow_dplyr_query
  # so make some accommodations. (Others are handled by S3 method dispatch.)
  if (inherits(x, "arrow_dplyr_query")) {
    class_title <- paste(source_data(x)$class_title(), "(query)")
  } else {
    class_title <- x$class_title()
  }
  cli::cat_line(class_title)

  dims <- dim(x)
  cli::cat_line(sprintf(
    "%s rows x %s columns", pretty_int(dims[1]), pretty_int(dims[2])
  ))

  if (dims[2] == 0) {
    return(invisible(x))
  }

  nrows <- as.integer(width / 3)
  head_tab <- dplyr::compute(head(x, nrows))
  # Take the schema from this Table because if x is arrow_dplyr_query, some
  # output types could be a best guess (in implicit_schema()).
  schema <- head_tab$schema

  # Assemble the column names and types
  # We use the Arrow type names here. See type_sum.DataType() below.
  var_types <- map_chr(schema$fields, ~ format(pillar::new_pillar_type(.$type)))
  # glimpse.tbl() left-aligns the var names (pads with whitespace to the right)
  # and appends the types next to them. Because those type names are
  # aggressively truncated to all be roughly the same length, this means the
  # data glimpse that follows is also mostly aligned.
  # However, Arrow type names are longer and variable length, and we're only
  # truncating the nested type information inside of <...>. So, to keep the
  # data glimpses aligned, we "justify" align the name and type: add the padding
  # whitespace between them so that the total width is equal.
  var_headings <- paste("$", center_pad(tickify(names(x)), var_types))

  # Assemble the data glimpse
  df <- as.data.frame(head_tab)
  formatted_data <- map_chr(df, function(.) {
    tryCatch(
      paste(pillar::format_glimpse(.), collapse = ", "),
      # This could error e.g. if you have a VctrsExtensionType and the package
      # that defines methods for the data is not loaded
      error = function(e) conditionMessage(e)
    )
  })
  # Here and elsewhere in the glimpse code, you have to use pillar::get_extent()
  # instead of nchar() because get_extent knows how to deal with ANSI escapes
  # etc.--it counts how much space on the terminal will be taken when printed.
  data_width <- width - pillar::get_extent(var_headings)
  truncated_data <- make_shorter(formatted_data, data_width)

  # Print the table body (var name, type, data glimpse)
  cli::cat_line(var_headings, " ", truncated_data)

  # TODO: use crayon to style these footers?
  if (inherits(x, "arrow_dplyr_query")) {
    cli::cat_line("Call `print()` for query details")
  } else if (any(grepl("<...>", var_types, fixed = TRUE)) || schema$HasMetadata) {
    cli::cat_line("Call `print()` for full schema details")
  }
  invisible(x)
}

# Dataset has an efficient head() method via Scanner so this is fine
glimpse.Dataset <- glimpse.ArrowTabular

glimpse.arrow_dplyr_query <- function(x,
                                      width = getOption("pillar.width", getOption("width")),
                                      ...) {
  if (any(map_lgl(all_sources(x), ~ inherits(., "RecordBatchReader")))) {
    msg <- paste(
      "Cannot glimpse() data from a RecordBatchReader because it can only be",
      "read one time. Call `compute()` to evaluate the query first."
    )
    message(msg)
    print(x)
  } else if (query_on_dataset(x) && !query_can_stream(x)) {
    msg <- paste(
      "This query requires a full table scan, so glimpse() may be",
      "expensive. Call `compute()` to evaluate the query first."
    )
    message(msg)
    print(x)
  } else {
    # Go for it
    glimpse.ArrowTabular(x, width = width, ...)
  }
}

glimpse.RecordBatchReader <- function(x,
                                      width = getOption("pillar.width", getOption("width")),
                                      ...) {
  # TODO(ARROW-17038): to_arrow() on duckdb con should hold con not RBR so it
  # can be run more than once (like duckdb does on the other side)
  msg <- paste(
    "Cannot glimpse() data from a RecordBatchReader because it can only be",
    "read one time; call `as_arrow_table()` to consume it first."
  )
  message(msg)
  print(x)
}

glimpse.ArrowDatum <- function(x, width, ...) {
  cli::cat_line(gsub("[ \n]+", " ", x$ToString()))
  invisible(x)
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
