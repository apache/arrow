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

#include <cstdint>

#include "benchmark/benchmark.h"

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

std::shared_ptr<Schema> ExampleSchema() {
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", timestamp(TimeUnit::MICRO, "UTC"));
  auto f2 = field("f2", int64());
  auto f3 = field("f3", int16());
  auto f4 = field("f4", int16());
  auto f5 = field("f5", float32());
  auto f6 = field("f6", float32());
  auto f7 = field("f7", float32());
  auto f8 = field("f8", decimal(19, 10));
  return schema({f0, f1, f2, f3, f4, f5, f6, f7, f8});
}

std::shared_ptr<RecordBatch> ExampleRecordBatch() {
  // We don't care about the actual data, since it's exported as raw buffer pointers
  auto schema = ExampleSchema();
  int64_t length = 1000;
  std::vector<std::shared_ptr<Array>> columns;
  for (const auto& field : schema->fields()) {
    auto array = *MakeArrayOfNull(field->type(), length);
    columns.push_back(array);
  }
  return RecordBatch::Make(schema, length, columns);
}

static void ExportType(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowSchema c_export;
  auto type = utf8();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportType(*type, &c_export));
    ArrowSchemaRelease(&c_export);
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportSchema(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowSchema c_export;
  auto schema = ExampleSchema();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportSchema(*schema, &c_export));
    ArrowSchemaRelease(&c_export);
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportArray(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowArray c_export;
  auto array = ArrayFromJSON(utf8(), R"(["foo", "bar", null])");

  for (auto _ : state) {
    ABORT_NOT_OK(ExportArray(*array, &c_export));
    ArrowArrayRelease(&c_export);
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportRecordBatch(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowArray c_export;
  auto batch = ExampleRecordBatch();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportRecordBatch(*batch, &c_export));
    ArrowArrayRelease(&c_export);
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportImportType(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowSchema c_export;
  auto type = utf8();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportType(*type, &c_export));
    ImportType(&c_export).ValueOrDie();
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportImportSchema(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowSchema c_export;
  auto schema = ExampleSchema();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportSchema(*schema, &c_export));
    ImportSchema(&c_export).ValueOrDie();
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportImportArray(benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowArray c_export;
  auto array = ArrayFromJSON(utf8(), R"(["foo", "bar", null])");
  auto type = array->type();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportArray(*array, &c_export));
    ImportArray(&c_export, type).ValueOrDie();
  }
  state.SetItemsProcessed(state.iterations());
}

static void ExportImportRecordBatch(
    benchmark::State& state) {  // NOLINT non-const reference
  struct ArrowArray c_export;
  auto batch = ExampleRecordBatch();
  auto schema = batch->schema();

  for (auto _ : state) {
    ABORT_NOT_OK(ExportRecordBatch(*batch, &c_export));
    ImportRecordBatch(&c_export, schema).ValueOrDie();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(ExportType);
BENCHMARK(ExportSchema);
BENCHMARK(ExportArray);
BENCHMARK(ExportRecordBatch);

BENCHMARK(ExportImportType);
BENCHMARK(ExportImportSchema);
BENCHMARK(ExportImportArray);
BENCHMARK(ExportImportRecordBatch);

}  // namespace arrow
