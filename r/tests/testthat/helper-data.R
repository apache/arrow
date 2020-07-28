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

example_data <- tibble::tibble(
  int = c(1:3, NA_integer_, 5:10),
  dbl = c(1:8, NA, 10) + .1,
  lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  false = logical(10),
  chr = letters[c(1:5, NA, 7:10)],
  fct = factor(letters[c(1:4, NA, NA, 7:10)])
)

example_with_metadata <- tibble::tibble(
  a = structure("one", class = "special_string"),
  b = 2,
  c = tibble::tibble(
    c1 = structure("inner", extra_attr = "something"),
    c2 = 4,
    c3 = 50
  ),
  d = "four"
)
# TODO: collect top-level dataset metadata
# https://issues.apache.org/jira/browse/ARROW-9271
# attr(example_with_metadata, "top_level") <- list(
#   field_one = 12,
#   field_two = "more stuff"
# )

haven_data <- tibble::tibble(
  num = structure(c(5.1, 4.9),
    format.spss = "F8.2"
  ),
  cat_int = structure(c(3, 1),
    format.spss = "F8.0",
    labels = c(first = 1, second = 2, third = 3),
    class = c("haven_labelled", "vctrs_vctr", "double")
  ),
  cat_chr = structure(c("B", "B"),
    labels = c(Alpha = "A", Beta = "B"),
    class = c("haven_labelled", "vctrs_vctr", "character")
  )
)

example_with_times <- tibble::tibble(
  date = Sys.Date() + 1:10,
  posixct = lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10,
  posixct_tz = lubridate::ymd_hms("2018-10-07 19:04:05", tz = "US/Eastern") + 1:10,
  posixlt = as.POSIXlt(lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10),
  posixlt_tz = as.POSIXlt(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "US/Eastern") + 1:10)
)

make_big_string <- function() {
  # This creates a character vector that would exceed the capacity of BinaryArray
  rep(purrr::map_chr(2047:2050, ~paste(sample(letters, ., replace = TRUE), collapse = "")), 2^18)
}
