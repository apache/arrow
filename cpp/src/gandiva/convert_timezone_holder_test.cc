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

#include "gandiva/convert_timezone_holder.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace gandiva{
    class TestConvertTimezone : public ::testing::Test {
    public:
        FunctionNode BuildConvert(std::string srcTz, std::string dstTz) {
          auto field = std::make_shared<FieldNode>(arrow::field("times", arrow::int64()));
          auto srcTz_node =
            std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(srcTz), false);
          auto dstTz_node =
            std::make_shared<LiteralNode>(arrow::utf8(), LiteralHolder(dstTz), false);
          return FunctionNode("convert_timezone", {field, srcTz_node, dstTz_node}, arrow::int64());
        }
    };
}  // namespace gandiva
