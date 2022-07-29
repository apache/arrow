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

union.arrow_dplyr_query <- function(x, y, ...) {
  x <- as_adq(x)
  y <- as_adq(y)

  dplyr::distinct(dplyr::union_all(x, y))
}

union.Dataset <- union.ArrowTabular <- union.RecordBatchReader <- union.arrow_dplyr_query

union_all.arrow_dplyr_query <- function(x, y, ...) {
  x <- as_adq(x)
  y <- as_adq(y)

  x$union_all <- list(right_data = y)
  collapse.arrow_dplyr_query(x)
}

union_all.Dataset <- union_all.ArrowTabular <- union_all.RecordBatchReader <- union_all.arrow_dplyr_query
