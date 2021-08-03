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

collect.arrow_dplyr_query <- function(x, as_data_frame = TRUE, ...) {
  x <- ensure_group_vars(x)
  x <- ensure_arrange_vars(x) # this sets x$temp_columns
  # Pull only the selected rows and cols into R
  # See dataset.R for Dataset and Scanner(Builder) classes
  tab <- Scanner$create(x)$ToTable()
  # Arrange rows
  if (length(x$arrange_vars) > 0) {
    tab <- tab[
      tab$SortIndices(names(x$arrange_vars), x$arrange_desc),
      names(x$selected_columns), # this omits x$temp_columns from the result
      drop = FALSE
    ]
  }
  if (as_data_frame) {
    df <- as.data.frame(tab)
    tab$invalidate()
    restore_dplyr_features(df, x)
  } else {
    restore_dplyr_features(tab, x)
  }
}
collect.ArrowTabular <- function(x, as_data_frame = TRUE, ...) {
  if (as_data_frame) {
    as.data.frame(x, ...)
  } else {
    x
  }
}
collect.Dataset <- function(x, ...) dplyr::collect(arrow_dplyr_query(x), ...)

compute.arrow_dplyr_query <- function(x, ...) dplyr::collect(x, as_data_frame = FALSE)
compute.ArrowTabular <- function(x, ...) x
compute.Dataset <- compute.arrow_dplyr_query

pull.arrow_dplyr_query <- function(.data, var = -1) {
  .data <- arrow_dplyr_query(.data)
  var <- vars_pull(names(.data), !!enquo(var))
  .data$selected_columns <- set_names(.data$selected_columns[var], var)
  dplyr::collect(.data)[[1]]
}
pull.Dataset <- pull.ArrowTabular <- pull.arrow_dplyr_query
