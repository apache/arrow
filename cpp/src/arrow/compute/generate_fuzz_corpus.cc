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

// A command line executable that generates seed corpus files for the
// arrow-compute-fuzz target: small IPC streams, each prefixed with the
// 8-byte compute-fuzz header (marker 0xFA + function/argument selectors).

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {
namespace {

constexpr uint8_t kFuzzHeaderMarker = 0xFA;

Result<std::shared_ptr<RecordBatch>> MakeNumericBatch() {
  // int64 with nulls and negatives, float64 with fractions
  Int64Builder i64_builder;
  DoubleBuilder f64_builder;
  const std::vector<std::optional<int64_t>> ints = {0, -3, 1000, std::nullopt, 42,
                                                    -1000000, 7, 25};
  const std::vector<std::optional<double>> doubles = {
      0.0, -1.5, 3.14159, std::nullopt, 1e10, -0.001, 2.5, 4.0};
  for (const auto& v : ints) {
    RETURN_NOT_OK(v.has_value() ? i64_builder.Append(*v) : i64_builder.AppendNull());
  }
  for (const auto& v : doubles) {
    RETURN_NOT_OK(v.has_value() ? f64_builder.Append(*v) : f64_builder.AppendNull());
  }
  ARROW_ASSIGN_OR_RAISE(auto i64_array, i64_builder.Finish());
  ARROW_ASSIGN_OR_RAISE(auto f64_array, f64_builder.Finish());
  auto schema = ::arrow::schema({field("i", int64()), field("d", float64())});
  return RecordBatch::Make(schema, i64_array->length(), {i64_array, f64_array});
}

Result<std::shared_ptr<RecordBatch>> MakeStringBatch() {
  StringBuilder builder;
  const std::vector<std::optional<std::string>> strings = {
      std::string(""), std::string("hello"), std::nullopt, std::string("wörld"),
      std::string("a longer string value for the kernels to chew on")};
  for (const auto& v : strings) {
    RETURN_NOT_OK(v.has_value() ? builder.Append(*v) : builder.AppendNull());
  }
  ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
  auto schema = ::arrow::schema({field("s", utf8())});
  return RecordBatch::Make(schema, array->length(), {array});
}

Result<std::shared_ptr<RecordBatch>> MakeSmallIntBatch() {
  Int32Builder i32_builder;
  const std::vector<std::optional<int32_t>> ints = {1, std::nullopt, 3, 4};
  for (const auto& v : ints) {
    RETURN_NOT_OK(v.has_value() ? i32_builder.Append(*v) : i32_builder.AppendNull());
  }
  ARROW_ASSIGN_OR_RAISE(auto array, i32_builder.Finish());
  auto schema = ::arrow::schema({field("i", int32())});
  return RecordBatch::Make(schema, array->length(), {array});
}

Result<std::shared_ptr<Buffer>> SerializeBatch(const RecordBatch& batch) {
  ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create(1024));
  ARROW_ASSIGN_OR_RAISE(auto writer, ipc::MakeStreamWriter(sink, batch.schema()));
  RETURN_NOT_OK(writer->WriteRecordBatch(batch));
  RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

Status WriteSeed(const std::string& dir, int num, const uint8_t* header,
                 int64_t header_size, const std::shared_ptr<Buffer>& payload) {
  auto name = dir + "/seed-" + std::to_string(num);
  ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(name));
  if (header_size > 0) {
    RETURN_NOT_OK(file->Write(header, header_size));
  }
  RETURN_NOT_OK(file->Write(payload));
  RETURN_NOT_OK(file->Close());
  std::cerr << name << std::endl;
  return Status::OK();
}

Status DoMain(const std::string& out_dir) {
  // The oss-fuzz build script removes the corpus directory before running
  // this generator; re-create it before writing seeds.
  std::error_code ec;
  std::filesystem::create_directories(out_dir, ec);
  if (ec) {
    return Status::IOError("Failed to create corpus directory '", out_dir,
                           "': ", ec.message());
  }
  std::vector<std::shared_ptr<RecordBatch>> batches;
  ARROW_ASSIGN_OR_RAISE(auto numeric, MakeNumericBatch());
  batches.push_back(numeric);
  ARROW_ASSIGN_OR_RAISE(auto strings, MakeStringBatch());
  batches.push_back(strings);
  ARROW_ASSIGN_OR_RAISE(auto small, MakeSmallIntBatch());
  batches.push_back(small);

  int seed_num = 0;
  // Legacy (header-less) seeds
  for (const auto& batch : batches) {
    ARROW_ASSIGN_OR_RAISE(auto payload, SerializeBatch(*batch));
    RETURN_NOT_OK(WriteSeed(out_dir, seed_num++, nullptr, 0, payload));
  }
  // Header-ful seeds covering unary/vector/aggregate/binary functions
  // (selector indices match FuzzFunctions() in fuzz.cc).
  const uint8_t selectors[] = {0, 5, 8, 13, 16};
  for (const auto& batch : batches) {
    ARROW_ASSIGN_OR_RAISE(auto payload, SerializeBatch(*batch));
    for (uint8_t selector : selectors) {
      for (uint8_t arg_mode = 0; arg_mode < 3; ++arg_mode) {
        uint8_t header[8] = {kFuzzHeaderMarker, selector, arg_mode, 7,
                             0, 0, 0, 0};
        RETURN_NOT_OK(WriteSeed(out_dir, seed_num++, header, sizeof(header), payload));
      }
    }
  }
  return Status::OK();
}

}  // namespace

Status GenerateFuzzCorpusMain(const std::string& out_dir) { return DoMain(out_dir); }

}  // namespace compute
}  // namespace arrow

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: arrow-compute-generate-fuzz-corpus <output directory>"
              << std::endl;
    return 2;
  }
  arrow::Status st = arrow::compute::GenerateFuzzCorpusMain(argv[1]);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}