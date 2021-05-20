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

context("Expressions")

test_that("C++ expressions", {
  skip_if_not_available("dataset")
  f <- Expression$field_ref("f")
  expect_identical(f$field_name, "f")
  g <- Expression$field_ref("g")
  date <- Expression$scalar(as.Date("2020-01-15"))
  ts <- Expression$scalar(as.POSIXct("2020-01-17 11:11:11"))
  i64 <- Expression$scalar(bit64::as.integer64(42))
  time <- Expression$scalar(hms::hms(56, 34, 12))

  expect_r6_class(f == g, "Expression")
  expect_r6_class(f == 4, "Expression")
  expect_r6_class(f == "", "Expression")
  expect_r6_class(f == NULL, "Expression")
  expect_r6_class(f == date, "Expression")
  expect_r6_class(f == i64, "Expression")
  expect_r6_class(f == time, "Expression")
  # can't seem to make this work right now because of R Ops.method dispatch
  # expect_r6_class(f == as.Date("2020-01-15"), "Expression")
  expect_r6_class(f == ts, "Expression")
  expect_r6_class(f <= 2L, "Expression")
  expect_r6_class(f != FALSE, "Expression")
  expect_r6_class(f > 4, "Expression")
  expect_r6_class(f < 4 & f > 2, "Expression")
  expect_r6_class(f < 4 | f > 2, "Expression")
  expect_r6_class(!(f < 4), "Expression")
  expect_output(
    print(f > 4),
    'Expression\n(f > 4)',
    fixed = TRUE
  )
  expect_type_equal(
    f$type(schema(f = float64())),
    float64()
  )
  expect_type_equal(
    (f > 4)$type(schema(f = float64())),
    bool()
  )
  # Interprets that as a list type
  expect_r6_class(f == c(1L, 2L), "Expression")
})
