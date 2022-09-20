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

#include "arrow/engine/substrait/test_util.h"
#include "arrow/ipc/writer.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

namespace engine {

std::shared_ptr<DataType> StripFieldNames(std::shared_ptr<DataType> type) {
  if (type->id() == Type::STRUCT) {
    FieldVector fields(type->num_fields());
    for (int i = 0; i < type->num_fields(); ++i) {
      fields[i] = type->field(i)->WithName("");
    }
    return struct_(std::move(fields));
  }

  if (type->id() == Type::LIST) {
    return list(type->field(0)->WithName(""));
  }

  return type;
}

void WriteIpcData(const std::string& path,
                  const std::shared_ptr<fs::FileSystem> file_system,
                  const std::shared_ptr<Table> input) {
  EXPECT_OK_AND_ASSIGN(auto mmap, file_system->OpenOutputStream(path));
  ASSERT_OK_AND_ASSIGN(
      auto file_writer,
      MakeFileWriter(mmap, input->schema(), ipc::IpcWriteOptions::Defaults()));
  TableBatchReader reader(input);
  std::shared_ptr<RecordBatch> batch;
  while (true) {
    ASSERT_OK(reader.ReadNext(&batch));
    if (batch == nullptr) {
      break;
    }
    ASSERT_OK(file_writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(file_writer->Close());
}

Result<std::shared_ptr<Table>> GetTableFromPlan(
    compute::Declaration& other_declrs, compute::ExecContext& exec_context,
    const std::shared_ptr<Schema>& output_schema) {
  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make(&exec_context));

  arrow::AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;
  auto sink_node_options = compute::SinkNodeOptions{&sink_gen};
  auto sink_declaration = compute::Declaration({"sink", sink_node_options, "e"});
  auto declarations = compute::Declaration::Sequence({other_declrs, sink_declaration});

  ARROW_ASSIGN_OR_RAISE(auto decl, declarations.AddToPlan(plan.get()));

  RETURN_NOT_OK(decl->Validate());

  std::shared_ptr<arrow::RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
      output_schema, std::move(sink_gen), exec_context.memory_pool());

  RETURN_NOT_OK(plan->Validate());
  RETURN_NOT_OK(plan->StartProducing());
  return arrow::Table::FromRecordBatchReader(sink_reader.get());
}

void CheckRoundTripResult(const std::shared_ptr<Schema> output_schema,
                          const std::shared_ptr<Table> expected_table,
                          compute::ExecContext& exec_context,
                          std::shared_ptr<Buffer>& buf,
                          const std::vector<int>& include_columns,
                          const ConversionOptions& conversion_options) {
  std::shared_ptr<ExtensionIdRegistry> sp_ext_id_reg = MakeExtensionIdRegistry();
  ExtensionIdRegistry* ext_id_reg = sp_ext_id_reg.get();
  ExtensionSet ext_set(ext_id_reg);
  ASSERT_OK_AND_ASSIGN(auto sink_decls, DeserializePlans(
                                            *buf, [] { return kNullConsumer; },
                                            ext_id_reg, &ext_set, conversion_options));
  // decl = std::get_if<compute::Declaration>(&decl->inputs[0]);                                          
  auto other_declrs = std::get_if<compute::Declaration>(&sink_decls[0].inputs[0]);

  ASSERT_OK_AND_ASSIGN(auto output_table,
                       GetTableFromPlan(*other_declrs, exec_context, output_schema));
  if (!include_columns.empty()) {
    ASSERT_OK_AND_ASSIGN(output_table, output_table->SelectColumns(include_columns));
  }
  ASSERT_OK_AND_ASSIGN(output_table, output_table->CombineChunks());
  EXPECT_TRUE(expected_table->Equals(*output_table));
}

}  // namespace engine
}  // namespace arrow
