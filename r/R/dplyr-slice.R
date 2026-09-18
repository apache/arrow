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

# The following S3 methods are registered on load if dplyr is present

slice_head.arrow_dplyr_query <- function(.data, ..., n, prop, by = NULL) {
  check_not_grouped(.data, {{ by }})
  check_dots_empty()

  if (missing(n)) {
    n <- prop_to_n(.data, prop)
  }

  head(.data, n)
}
slice_head.Dataset <- slice_head.ArrowTabular <- slice_head.RecordBatchReader <- slice_head.arrow_dplyr_query

slice_tail.arrow_dplyr_query <- function(.data, ..., n, prop, by = NULL) {
  check_not_grouped(.data, {{ by }})
  check_dots_empty()

  if (missing(n)) {
    n <- prop_to_n(.data, prop)
  }

  tail(.data, n)
}
slice_tail.Dataset <- slice_tail.ArrowTabular <- slice_tail.RecordBatchReader <- slice_tail.arrow_dplyr_query

slice_min.arrow_dplyr_query <- function(.data, order_by, ..., n, prop, by = NULL, with_ties = TRUE) {
  check_not_grouped(.data, {{ by }})
  if (with_ties) {
    arrow_not_supported("with_ties = TRUE")
  }
  check_dots_empty()

  if (missing(n)) {
    n <- prop_to_n(.data, prop)
  }

  head(dplyr::arrange(.data, {{ order_by }}), n)
}
slice_min.Dataset <- slice_min.ArrowTabular <- slice_min.RecordBatchReader <- slice_min.arrow_dplyr_query

slice_max.arrow_dplyr_query <- function(.data, order_by, ..., n, prop, by = NULL, with_ties = TRUE) {
  check_not_grouped(.data, {{ by }})
  if (with_ties) {
    arrow_not_supported("with_ties = TRUE")
  }
  check_dots_empty()

  if (missing(n)) {
    n <- prop_to_n(.data, prop)
  }

  sorted <- dplyr::arrange(.data, {{ order_by }})
  # Invert the sort order of the things in ... so they're descending
  # TODO: handle possibility that .data was already sorted and we don't want
  # to invert those sorts? Does that matter? Or no because there's no promise
  # of order of which TopK elements you get if there are ties?
  sorted$arrange_desc <- !sorted$arrange_desc
  head(sorted, n)
}
slice_max.Dataset <- slice_max.ArrowTabular <- slice_max.RecordBatchReader <- slice_max.arrow_dplyr_query

slice_sample.arrow_dplyr_query <- function(.data, ..., n, prop, by = NULL, weight_by = NULL, replace = FALSE) {
  check_not_grouped(.data, {{ by }})
  if (replace) {
    arrow_not_supported("Sampling with replacement")
  }
  if (!missing(weight_by)) {
    # You could do this by multiplying the random() column * weight_by
    # but you'd need to calculate sum(weight_by) in order to normalize
    arrow_not_supported("weight_by")
  }
  check_dots_empty()

  if (missing(n) && missing(prop)) {
    # dplyr's default
    n <- 1
  } else if (!missing(n) && !missing(prop)) {
    validation_error("Must supply exactly one of `n` and `prop`")
  }
  .data <- as_adq(.data)

  if (missing(n)) {
    # Sampling a proportion: keep each row independently with probability prop.
    # This streams, but the number of rows returned is only approximately
    # prop * nrow(.data).
    validate_prop(prop)
    if (prop < 1) {
      .data <- set_filters(.data, Expression$create("random") < prop)
    }
    return(.data)
  }

  # Sampling n rows: sort by a random number and take the first n.
  # Sorting requires all of the data in memory, so when we know how many rows
  # there are, first filter down to a random subset that is almost certainly
  # bigger than n. The number of rows that pass the filter is Binomial with
  # standard deviation <= sqrt(oversample) <= sqrt(n) + 10, so the margin of
  # 10 * sqrt(n) + 100 rows is at least 10 standard deviations. Each row passes
  # the filter independently, so a uniform sample of the rows that pass is also
  # a uniform sample of the whole.
  validate_n(n)
  if (query_has_reader(.data)) {
    # Counting rows would consume the reader
    nrows <- NA_integer_
  } else {
    # For a filtered query this evaluates the filter, which is an extra pass
    # over the data, but that's cheaper than sorting all of it
    nrows <- nrow(.data)
  }
  oversample <- n + 10 * sqrt(n) + 100
  if (!is.na(nrows) && oversample < nrows) {
    .data <- set_filters(.data, Expression$create("random") < oversample / nrows)
  }
  # This sort key isn't in selected_columns so it gets projected away after
  # sorting, see ensure_arrange_vars(). Its name must not clash with a selected
  # column or with an existing sort key that will also be a temp column.
  existing <- c(names(.data), names(.data$arrange_vars))
  key <- make.unique(c(existing, "..random"))[length(existing) + 1]
  .data$arrange_vars <- c(set_names(list(Expression$create("random")), key), .data$arrange_vars)
  .data$arrange_desc <- c(FALSE, .data$arrange_desc)
  head(.data, n)
}
slice_sample.Dataset <- slice_sample.ArrowTabular <- slice_sample.RecordBatchReader <- slice_sample.arrow_dplyr_query


prop_to_n <- function(.data, prop) {
  nrows <- nrow(.data)
  if (is.na(nrows)) {
    arrow_not_supported("Slicing with `prop` when the query has joins or aggregations")
  }
  validate_prop(prop)
  nrows * prop
}

validate_n <- function(n) {
  if (!is.numeric(n) || length(n) != 1 || is.na(n) || n < 0) {
    validation_error("`n` must be a single non-negative numeric value")
  }
}

validate_prop <- function(prop) {
  if (!is.numeric(prop) || length(prop) != 1 || is.na(prop) || prop < 0 || prop > 1) {
    validation_error("`prop` must be a single numeric value between 0 and 1")
  }
}

check_not_grouped <- function(.data, by) {
  by <- enquo(by)
  if (length(dplyr::group_vars(.data)) > 0 || !quo_is_null(by)) {
    arrow_not_supported("Slicing grouped data")
  }
}
