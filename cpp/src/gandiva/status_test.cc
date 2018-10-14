// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gandiva/status.h"

#include <sstream>

#include <gtest/gtest.h>

namespace gandiva {

TEST(StatusTest, TestCodeAndMessage) {
  Status ok = Status::OK();
  ASSERT_EQ(StatusCode::OK, ok.code());
  Status code_gen_error = Status::CodeGenError("input invalid.");
  ASSERT_EQ(StatusCode::CodeGenError, code_gen_error.code());
  ASSERT_EQ("input invalid.", code_gen_error.message());
}

TEST(StatusTest, TestToString) {
  Status code_gen_error = Status::CodeGenError("input invalid.");
  ASSERT_EQ("CodeGenError: input invalid.", code_gen_error.ToString());

  std::stringstream ss;
  ss << code_gen_error;
  ASSERT_EQ(code_gen_error.ToString(), ss.str());
}

TEST(StatusTest, AndStatus) {
  Status a = Status::OK();
  Status b = Status::OK();
  Status c = Status::CodeGenError("invalid value");

  Status res;
  res = a & b;
  ASSERT_TRUE(res.ok());
  res = a & c;
  ASSERT_TRUE(res.IsCodeGenError());

  res = Status::OK();
  res &= c;
  ASSERT_TRUE(res.IsCodeGenError());

  // With rvalues
  res = Status::OK() & Status::CodeGenError("foo");
  ASSERT_TRUE(res.IsCodeGenError());
  res = Status::CodeGenError("foo") & Status::OK();
  ASSERT_TRUE(res.IsCodeGenError());

  res = Status::OK();
  res &= Status::OK();
  ASSERT_TRUE(res.ok());
  res &= Status::CodeGenError("foo");
  ASSERT_TRUE(res.IsCodeGenError());
}

}  // namespace gandiva
