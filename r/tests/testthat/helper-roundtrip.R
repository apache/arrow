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

expect_array_roundtrip <- function(x, type, as = NULL) {
  a <- Array$create(x, type = as)
  expect_type_equal(a$type, type)
  expect_identical(length(a), length(x))
  if (!inherits(type, c("ListType", "LargeListType"))) {
    # TODO: revisit how missingness works with ListArrays
    # R list objects don't handle missingness the same way as other vectors.
    # Is there some vctrs thing we should do on the roundtrip back to R?
    expect_equal(as.vector(is.na(a)), is.na(x))
  }
  expect_equivalent(as.vector(a), x)
  # Make sure the storage mode is the same on roundtrip (esp. integer vs. numeric)
  expect_identical(typeof(as.vector(a)), typeof(x))

  if (length(x)) {
    a_sliced <- a$Slice(1)
    x_sliced <- x[-1]
    expect_type_equal(a_sliced$type, type)
    expect_identical(length(a_sliced), length(x_sliced))
    if (!inherits(type, c("ListType", "LargeListType"))) {
      expect_equal(as.vector(is.na(a_sliced)), is.na(x_sliced))
    }
    expect_equivalent(as.vector(a_sliced), x_sliced)
  }
  invisible(a)
}
