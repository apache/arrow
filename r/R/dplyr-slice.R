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

slice_head.arrow_dplyr_query <- function(.data, ..., n, prop) {
  if (length(dplyr::group_vars(.data)) > 0) {
    arrow_not_supported("Slicing grouped data")
  }
  check_dots_empty()

  if (missing(n)) {
    n <- prop_to_n(.data, prop)
  }

  head(.data, n)
}
slice_head.Dataset <- slice_head.ArrowTabular <- slice_head.RecordBatchReader <- slice_head.arrow_dplyr_query

slice_tail.arrow_dplyr_query <- function(.data, ..., n, prop) {
  if (length(dplyr::group_vars(.data)) > 0) {
    arrow_not_supported("Slicing grouped data")
  }
  check_dots_empty()

  if (missing(n)) {
    n <- prop_to_n(.data, prop)
  }

  tail(.data, n)
}
slice_tail.Dataset <- slice_tail.ArrowTabular <- slice_tail.RecordBatchReader <- slice_tail.arrow_dplyr_query

slice_min.arrow_dplyr_query <- function(.data, order_by, ..., n, prop, with_ties = TRUE) {
  if (length(dplyr::group_vars(.data)) > 0) {
    arrow_not_supported("Slicing grouped data")
  }
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

slice_max.arrow_dplyr_query <- function(.data, order_by, ..., n, prop, with_ties = TRUE) {
  if (length(dplyr::group_vars(.data)) > 0) {
    arrow_not_supported("Slicing grouped data")
  }
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

#' @importFrom stats runif
slice_sample.arrow_dplyr_query <- function(.data,
                                           ...,
                                           n,
                                           prop,
                                           weight_by = NULL,
                                           replace = FALSE) {
  if (length(dplyr::group_vars(.data)) > 0) {
    arrow_not_supported("Slicing grouped data")
  }
  if (replace) {
    arrow_not_supported("Sampling with replacement")
  }
  if (!missing(weight_by)) {
    # You could do this by multiplying the random() column * weight_by
    # but you'd need to calculate sum(weight_by) in order to normalize
    arrow_not_supported("weight_by")
  }
  check_dots_empty()

  # If we want n rows sampled, we have to convert n to prop, oversample some
  # just to make sure we get enough, then head(n)
  sampling_n <- missing(prop)
  if (sampling_n) {
    prop <- min(n_to_prop(.data, n) + .05, 1)
  }
  validate_prop(prop)

  if (prop < 1) {
    .data <- as_adq(.data)
    # TODO(ARROW-17974): use Expression$create("random") instead of UDF hack
    # HACK: use a UDF to generate random. It needs an input column because
    # nullary functions don't work, and that column has to be typed. We've
    # chosen boolean() type because it's compact and can always be created:
    # pick any column and do is.na, that will be boolean.
    if (is.null(.cache$functions[["_random_along"]])) {
      register_scalar_function(
        "_random_along",
        function(context, x) {
          Array$create(runif(length(x)))
        },
        in_type = schema(x = boolean()),
        out_type = float64(),
        auto_convert = FALSE
      )
    }
    # TODO: get an actual FieldRef because the first col could be derived
    ref <- Expression$create("is_null", .data$selected_columns[[1]])
    expr <- Expression$create("_random_along", ref) < prop
    .data <- set_filters(.data, expr)
  }
  if (sampling_n) {
    .data <- head(.data, n)
  }

  .data
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

validate_prop <- function(prop) {
  if (!is.numeric(prop) || length(prop) != 1 || is.na(prop) || prop < 0 || prop > 1) {
    stop("`prop` must be a single numeric value between 0 and 1", call. = FALSE)
  }
}

n_to_prop <- function(.data, n) {
  nrows <- nrow(.data)
  if (is.na(nrows)) {
    arrow_not_supported("slice_sample() with `n` when the query has joins or aggregations")
  }
  n / nrows
}
