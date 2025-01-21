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

#include "arrow/integration/c_data_integration_internal.h"

#include <sstream>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/integration/json_integration.h"
#include "arrow/io/file.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"

namespace arrow::internal::integration {
namespace {

template <typename Func>
const char* StatusToErrorString(Func&& func) {
  static std::string error;

  Status st = func();
  if (st.ok()) {
    return nullptr;
  }
  error = st.ToString();
  ARROW_CHECK_GT(error.length(), 0);
  return error.c_str();
}

Result<std::shared_ptr<Schema>> ReadSchemaFromJson(const std::string& json_path,
                                                   MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(json_path, pool));
  ARROW_ASSIGN_OR_RAISE(auto reader, IntegrationJsonReader::Open(pool, file));
  return reader->schema();
}

Result<std::shared_ptr<RecordBatch>> ReadBatchFromJson(const std::string& json_path,
                                                       int num_batch, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(json_path, pool));
  ARROW_ASSIGN_OR_RAISE(auto reader, IntegrationJsonReader::Open(pool, file));
  return reader->ReadRecordBatch(num_batch);
}

// XXX ideally, we should allow use of a custom memory pool in the C bridge API,
// but that requires non-trivial refactor

Status ExportSchemaFromJson(std::string json_path, ArrowSchema* out) {
  auto pool = default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto schema, ReadSchemaFromJson(json_path, pool));
  return ExportSchema(*schema, out);
}

Status ImportSchemaAndCompareToJson(std::string json_path, ArrowSchema* c_schema) {
  auto pool = default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto json_schema, ReadSchemaFromJson(json_path, pool));
  ARROW_ASSIGN_OR_RAISE(auto imported_schema, ImportSchema(c_schema));
  if (!imported_schema->Equals(json_schema, /*check_metadata=*/true)) {
    return Status::Invalid("Schemas are different:", "\n- Json Schema: ", *json_schema,
                           "\n- Imported Schema: ", *imported_schema);
  }
  return Status::OK();
}

Status ExportBatchFromJson(std::string json_path, int num_batch, ArrowArray* out) {
  auto pool = default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto batch, ReadBatchFromJson(json_path, num_batch, pool));
  return ExportRecordBatch(*batch, out);
}

Status ImportBatchAndCompareToJson(std::string json_path, int num_batch,
                                   ArrowArray* c_batch) {
  auto pool = default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto batch, ReadBatchFromJson(json_path, num_batch, pool));
  ARROW_ASSIGN_OR_RAISE(auto imported_batch, ImportRecordBatch(c_batch, batch->schema()));
  RETURN_NOT_OK(imported_batch->ValidateFull());
  if (!imported_batch->Equals(*batch, /*check_metadata=*/true)) {
    std::stringstream pp_expected;
    std::stringstream pp_actual;
    PrettyPrintOptions options(/*indent=*/2);
    options.window = 50;
    ARROW_CHECK_OK(PrettyPrint(*batch, options, &pp_expected));
    ARROW_CHECK_OK(PrettyPrint(*imported_batch, options, &pp_actual));
    return Status::Invalid("Record Batches are different:", "\n- Json Batch: ",
                           pp_expected.str(), "\n- Imported Batch: ", pp_actual.str());
  }
  return Status::OK();
}

}  // namespace
}  // namespace arrow::internal::integration

const char* ArrowCpp_CDataIntegration_ExportSchemaFromJson(const char* json_path,
                                                           ArrowSchema* out) {
  using namespace arrow::internal::integration;  // NOLINT(build/namespaces)
  return StatusToErrorString([=]() { return ExportSchemaFromJson(json_path, out); });
}

const char* ArrowCpp_CDataIntegration_ImportSchemaAndCompareToJson(const char* json_path,
                                                                   ArrowSchema* schema) {
  using namespace arrow::internal::integration;  // NOLINT(build/namespaces)
  return StatusToErrorString(
      [=]() { return ImportSchemaAndCompareToJson(json_path, schema); });
}

const char* ArrowCpp_CDataIntegration_ExportBatchFromJson(const char* json_path,
                                                          int num_batch,
                                                          ArrowArray* out) {
  using namespace arrow::internal::integration;  // NOLINT(build/namespaces)
  return StatusToErrorString(
      [=]() { return ExportBatchFromJson(json_path, num_batch, out); });
}

const char* ArrowCpp_CDataIntegration_ImportBatchAndCompareToJson(const char* json_path,
                                                                  int num_batch,
                                                                  ArrowArray* batch) {
  using namespace arrow::internal::integration;  // NOLINT(build/namespaces)
  return StatusToErrorString(
      [=]() { return ImportBatchAndCompareToJson(json_path, num_batch, batch); });
}

int64_t ArrowCpp_BytesAllocated() {
  auto pool = arrow::default_memory_pool();
  return pool->bytes_allocated();
}
