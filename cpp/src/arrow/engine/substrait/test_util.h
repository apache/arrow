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

// These utilities are for internal / unit test use only.
// They allow for the construction of simple Substrait plans
// programmatically without first requiring the construction
// of an ExecPlan

// These utilities have to be here, and not in a test_util.cc
// file (or in a unit test) because only one .so is allowed
// to include each .pb.h file or else protobuf will encounter
// global namespace conflicts.

#include <gtest/gtest.h>
#include "arrow/dataset/file_base.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/table.h"

namespace arrow {

namespace engine {

class NullSinkNodeConsumer : public compute::SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<Schema>&, compute::BackpressureControl*) override {
    return Status::OK();
  }
  Status Consume(compute::ExecBatch exec_batch) override { return Status::OK(); }
  Future<> Finish() override { return Status::OK(); }

 public:
  static std::shared_ptr<NullSinkNodeConsumer> Make() {
    return std::make_shared<NullSinkNodeConsumer>();
  }
};

const auto kNullConsumer = std::make_shared<NullSinkNodeConsumer>();

const std::shared_ptr<Schema> kBoringSchema = schema({
    field("bool", boolean()),
    field("i8", int8()),
    field("i32", int32()),
    field("i32_req", int32(), /*nullable=*/false),
    field("u32", uint32()),
    field("i64", int64()),
    field("f32", float32()),
    field("f32_req", float32(), /*nullable=*/false),
    field("f64", float64()),
    field("date64", date64()),
    field("str", utf8()),
    field("list_i32", list(int32())),
    field("struct", struct_({
                        field("i32", int32()),
                        field("str", utf8()),
                        field("struct_i32_str",
                              struct_({field("i32", int32()), field("str", utf8())})),
                    })),
    field("list_struct", list(struct_({
                             field("i32", int32()),
                             field("str", utf8()),
                             field("struct_i32_str", struct_({field("i32", int32()),
                                                              field("str", utf8())})),
                         }))),
    field("dict_str", dictionary(int32(), utf8())),
    field("dict_i32", dictionary(int32(), int32())),
    field("ts_ns", timestamp(TimeUnit::NANO)),
});

std::shared_ptr<DataType> StripFieldNames(std::shared_ptr<DataType> type);

inline compute::Expression UseBoringRefs(const compute::Expression& expr) {
  if (expr.literal()) return expr;

  if (auto ref = expr.field_ref()) {
    return compute::field_ref(*ref->FindOne(*kBoringSchema));
  }

  auto modified_call = *CallNotNull(expr);
  for (auto& arg : modified_call.arguments) {
    arg = UseBoringRefs(arg);
  }
  return compute::Expression{std::move(modified_call)};
}

void WriteIpcData(const std::string& path,
                  const std::shared_ptr<fs::FileSystem> file_system,
                  const std::shared_ptr<Table> input);

Result<std::shared_ptr<Table>> GetTableFromPlan(
    compute::Declaration& other_declrs, compute::ExecContext& exec_context,
    const std::shared_ptr<Schema>& output_schema);

void CheckRoundTripResult(const std::shared_ptr<Schema> output_schema,
                          const std::shared_ptr<Table> expected_table,
                          compute::ExecContext& exec_context,
                          std::shared_ptr<Buffer>& buf,
                          const std::vector<int>& include_columns = {},
                          const ConversionOptions& conversion_options = {});

}  // namespace engine
}  // namespace arrow
