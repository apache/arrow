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

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <opentelemetry/nostd/variant.h>
#include <opentelemetry/sdk/common/attribute_utils.h>
#include <opentelemetry/sdk/common/exporter_utils.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/provider.h>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"

namespace parquet {
namespace arrow {

namespace {

namespace otel = opentelemetry;
namespace sdktrace = opentelemetry::sdk::trace;

struct CapturedSpan {
  std::string name;
  std::unordered_map<std::string, otel::sdk::common::OwnedAttributeValue> attributes;
};

class CapturedSpanStorage {
 public:
  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    spans_.clear();
  }

  void Append(const sdktrace::SpanData& span) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto name = span.GetName();
    spans_.push_back({std::string(name.data(), name.size()), span.GetAttributes()});
  }

  std::vector<CapturedSpan> ReadColumnSpans() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<CapturedSpan> result;
    for (const auto& span : spans_) {
      if (span.name == "parquet::arrow::read_column") {
        result.push_back(span);
      }
    }
    return result;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<CapturedSpan> spans_;
};

class CapturingSpanExporter : public sdktrace::SpanExporter {
 public:
  explicit CapturingSpanExporter(std::shared_ptr<CapturedSpanStorage> storage)
      : storage_(std::move(storage)) {}

  std::unique_ptr<sdktrace::Recordable> MakeRecordable() noexcept override {
    return std::make_unique<sdktrace::SpanData>();
  }

  otel::sdk::common::ExportResult Export(
      const otel::nostd::span<std::unique_ptr<sdktrace::Recordable>>& spans) noexcept
      override {
    try {
      for (const auto& recordable : spans) {
        auto* span = dynamic_cast<sdktrace::SpanData*>(recordable.get());
        if (span == nullptr) {
          return otel::sdk::common::ExportResult::kFailure;
        }
        storage_->Append(*span);
      }
      return otel::sdk::common::ExportResult::kSuccess;
    } catch (...) {
      return otel::sdk::common::ExportResult::kFailure;
    }
  }

  bool ForceFlush(std::chrono::microseconds) noexcept override { return true; }

  bool Shutdown(std::chrono::microseconds) noexcept override { return true; }

 private:
  std::shared_ptr<CapturedSpanStorage> storage_;
};

const auto kSpanStorage = std::make_shared<CapturedSpanStorage>();

class OtelEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    auto exporter = std::make_unique<CapturingSpanExporter>(kSpanStorage);
    auto processor = std::make_unique<sdktrace::SimpleSpanProcessor>(std::move(exporter));
    auto provider = otel::nostd::shared_ptr<sdktrace::TracerProvider>(
        new sdktrace::TracerProvider(std::move(processor)));
    otel::trace::Provider::SetTracerProvider(std::move(provider));
  }
};

[[maybe_unused]] static ::testing::Environment* kOtelEnvironment =
    ::testing::AddGlobalTestEnvironment(new OtelEnvironment);

::arrow::Result<std::shared_ptr<::arrow::Buffer>> WriteToBuffer(
    const std::shared_ptr<::arrow::Table>& table) {
  ARROW_ASSIGN_OR_RAISE(auto sink, ::arrow::io::BufferOutputStream::Create());
  RETURN_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), sink, table->num_rows()));
  return sink->Finish();
}

::arrow::Result<std::unique_ptr<FileReader>> OpenReader(
    const std::shared_ptr<::arrow::Table>& table) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, WriteToBuffer(table));
  auto parquet_reader =
      ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
  ARROW_ASSIGN_OR_RAISE(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                      std::move(parquet_reader)));
  reader->set_use_threads(false);
  return reader;
}

std::shared_ptr<::arrow::Table> NestedTable() {
  auto table_schema = ::arrow::schema(
      {::arrow::field("first", ::arrow::int32()),
       ::arrow::field("group",
                      ::arrow::struct_({::arrow::field("left", ::arrow::int32()),
                                        ::arrow::field("right", ::arrow::utf8())})),
       ::arrow::field("last", ::arrow::int64())});
  return ::arrow::TableFromJSON(table_schema,
                                {R"([{"first": 1, "group": {"left": 10, "right": "a"},
                            "last": 100},
                           {"first": 2, "group": {"left": 20, "right": "b"},
                            "last": 200}])"});
}

template <typename T>
const T* GetAttribute(const CapturedSpan& span, const std::string& name) {
  auto it = span.attributes.find(name);
  if (it == span.attributes.end()) {
    return nullptr;
  }
  return otel::nostd::get_if<T>(&it->second);
}

void AssertColumnAttributes(const CapturedSpan& span, int32_t field_index,
                            const std::string& field_name,
                            const std::string& physical_type) {
  const auto* actual_index = GetAttribute<int32_t>(span, "parquet.arrow.columnindex");
  ASSERT_NE(actual_index, nullptr);
  EXPECT_EQ(*actual_index, field_index);

  const auto* actual_name = GetAttribute<std::string>(span, "parquet.arrow.columnname");
  ASSERT_NE(actual_name, nullptr);
  EXPECT_EQ(*actual_name, field_name);

  const auto* actual_type = GetAttribute<std::string>(span, "parquet.arrow.physicaltype");
  ASSERT_NE(actual_type, nullptr);
  EXPECT_EQ(*actual_type, physical_type);
}

TEST(ReadColumnTracing, FlatColumnSubset) {
  auto table_schema = ::arrow::schema({::arrow::field("first", ::arrow::int32()),
                                       ::arrow::field("target", ::arrow::utf8())});
  auto table = ::arrow::TableFromJSON(
      table_schema, {R"([{"first": 1, "target": "a"}, {"first": 2, "target": "b"}])"});
  ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(table));

  kSpanStorage->Clear();
  ASSERT_OK_AND_ASSIGN(auto result, reader->ReadTable({1}));
  ASSERT_EQ(result->num_columns(), 1);
  ASSERT_TRUE(result->column(0)->Equals(table->column(1)));

  auto spans = kSpanStorage->ReadColumnSpans();
  ASSERT_EQ(spans.size(), 1);
  AssertColumnAttributes(spans[0], 1, "target", "BYTE_ARRAY");
}

TEST(ReadColumnTracing, NestedColumnSubset) {
  ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(NestedTable()));

  kSpanStorage->Clear();
  ASSERT_OK_AND_ASSIGN(auto result, reader->ReadTable({2, 3}));
  ASSERT_EQ(result->num_columns(), 2);
  ASSERT_EQ(result->schema()->field(0)->name(), "group");
  ASSERT_EQ(result->schema()->field(1)->name(), "last");

  auto spans = kSpanStorage->ReadColumnSpans();
  ASSERT_EQ(spans.size(), 2);
  AssertColumnAttributes(spans[0], 1, "group", "");
  AssertColumnAttributes(spans[1], 2, "last", "INT64");
}

TEST(ReadColumnTracing, ReorderedColumnSubset) {
  ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(NestedTable()));

  kSpanStorage->Clear();
  ASSERT_OK_AND_ASSIGN(auto result, reader->ReadTable({3, 0, 2}));
  ASSERT_EQ(result->num_columns(), 3);

  auto spans = kSpanStorage->ReadColumnSpans();
  ASSERT_EQ(spans.size(), 3);
  AssertColumnAttributes(spans[0], 2, "last", "INT64");
  AssertColumnAttributes(spans[1], 0, "first", "INT32");
  AssertColumnAttributes(spans[2], 1, "group", "");
}

TEST(ReadColumnTracing, FullNestedSchema) {
  ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(NestedTable()));

  kSpanStorage->Clear();
  ASSERT_OK_AND_ASSIGN(auto result, reader->ReadTable());
  ASSERT_EQ(result->num_columns(), 3);

  auto spans = kSpanStorage->ReadColumnSpans();
  ASSERT_EQ(spans.size(), 3);
  AssertColumnAttributes(spans[0], 0, "first", "INT32");
  AssertColumnAttributes(spans[1], 1, "group", "");
  AssertColumnAttributes(spans[2], 2, "last", "INT64");
}

TEST(ReadColumnTracing, DirectFileTopLevelFieldRead) {
  auto table = NestedTable();
  ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(table));

  kSpanStorage->Clear();
  std::shared_ptr<::arrow::ChunkedArray> result;
  ASSERT_OK(reader->ReadColumn(2, &result));
  ASSERT_TRUE(result->Equals(table->column(2)));

  auto spans = kSpanStorage->ReadColumnSpans();
  ASSERT_EQ(spans.size(), 1);
  AssertColumnAttributes(spans[0], 2, "last", "INT64");
}

TEST(ReadColumnTracing, DirectRowGroupTopLevelFieldRead) {
  auto table = NestedTable();
  ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(table));

  kSpanStorage->Clear();
  std::shared_ptr<::arrow::ChunkedArray> result;
  ASSERT_OK(reader->RowGroup(0)->Column(1)->Read(&result));
  ASSERT_TRUE(result->Equals(table->column(1)));

  auto spans = kSpanStorage->ReadColumnSpans();
  ASSERT_EQ(spans.size(), 1);
  AssertColumnAttributes(spans[0], 1, "group", "");
}

}  // namespace

}  // namespace arrow
}  // namespace parquet
