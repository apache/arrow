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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/plan_internal.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/test_plan_builder.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {

namespace engine {

struct RelationTestCase {
  Id function_id;
  std::vector<std::string> arguments;
  std::vector<std::shared_ptr<DataType>> data_types;
  // For a test case that should fail just use the empty string
  std::string expected_output;
  std::shared_ptr<DataType> expected_output_type;
};

}  // namespace engine

}  // namespace arrow
