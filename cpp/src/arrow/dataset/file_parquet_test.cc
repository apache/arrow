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

#include "arrow/dataset/file_parquet.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/filter.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "parquet/arrow/writer.h"

namespace arrow {
namespace dataset {

constexpr int64_t kBatchSize = 1UL << 15;
constexpr int64_t kBatchRepetitions = 1 << 10;
constexpr int64_t kNumRows = kBatchSize * kBatchRepetitions;

using parquet::ArrowWriterProperties;
using parquet::default_arrow_writer_properties;

using parquet::default_writer_properties;
using parquet::WriterProperties;

using parquet::CreateOutputStream;
using parquet::arrow::FileWriter;
using parquet::arrow::WriteTable;

using testing::Pointee;

Status WriteRecordBatch(const RecordBatch& batch, FileWriter* writer) {
  auto schema = batch.schema();
  auto size = batch.num_rows();

  if (!schema->Equals(*writer->schema(), false)) {
    return Status::Invalid("RecordBatch schema does not match this writer's. batch:'",
                           schema->ToString(), "' this:'", writer->schema()->ToString(),
                           "'");
  }

  RETURN_NOT_OK(writer->NewRowGroup(size));
  for (int i = 0; i < batch.num_columns(); i++) {
    RETURN_NOT_OK(writer->WriteColumnChunk(*batch.column(i)));
  }

  return Status::OK();
}

Status WriteRecordBatchReader(RecordBatchReader* reader, FileWriter* writer) {
  auto schema = reader->schema();

  if (!schema->Equals(*writer->schema(), false)) {
    return Status::Invalid("RecordBatch schema does not match this writer's. batch:'",
                           schema->ToString(), "' this:'", writer->schema()->ToString(),
                           "'");
  }

  return MakePointerIterator(reader).Visit(
      [&](std::shared_ptr<RecordBatch> batch) -> Status {
        return WriteRecordBatch(*batch, writer);
      });
}

Status WriteRecordBatchReader(
    RecordBatchReader* reader, MemoryPool* pool,
    const std::shared_ptr<io::OutputStream>& sink,
    const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
    const std::shared_ptr<ArrowWriterProperties>& arrow_properties =
        default_arrow_writer_properties()) {
  std::unique_ptr<FileWriter> writer;
  RETURN_NOT_OK(FileWriter::Open(*reader->schema(), pool, sink, properties,
                                 arrow_properties, &writer));
  RETURN_NOT_OK(WriteRecordBatchReader(reader, writer.get()));
  return writer->Close();
}

class ArrowParquetWriterMixin : public ::testing::Test {
 public:
  std::shared_ptr<Buffer> Write(RecordBatchReader* reader) {
    auto pool = ::arrow::default_memory_pool();

    std::shared_ptr<Buffer> out;
    auto sink = CreateOutputStream(pool);

    ARROW_EXPECT_OK(WriteRecordBatchReader(reader, pool, sink));
    ARROW_EXPECT_OK(sink->Finish(&out));

    return out;
  }

  std::shared_ptr<Buffer> Write(const Table& table) {
    auto pool = ::arrow::default_memory_pool();

    std::shared_ptr<Buffer> out;
    auto sink = CreateOutputStream(pool);

    ARROW_EXPECT_OK(WriteTable(table, pool, sink, 1U << 16));
    ARROW_EXPECT_OK(sink->Finish(&out));

    return out;
  }
};

class ParquetBufferFixtureMixin : public ArrowParquetWriterMixin {
 public:
  std::unique_ptr<FileSource> GetFileSource(RecordBatchReader* reader) {
    auto buffer = Write(reader);
    return internal::make_unique<FileSource>(std::move(buffer));
  }

  std::unique_ptr<RecordBatchReader> GetRecordBatchReader() {
    auto batch = ConstantArrayGenerator::Zeroes(kBatchSize, schema_);
    int64_t i = 0;
    return MakeGeneratedRecordBatch(
        batch->schema(), [batch, i](std::shared_ptr<RecordBatch>* out) mutable {
          *out = i++ < kBatchRepetitions ? batch : nullptr;
          return Status::OK();
        });
  }

 protected:
  std::shared_ptr<Schema> schema_ = schema({field("f64", float64())});
};

class TestParquetFileFormat : public ParquetBufferFixtureMixin {
 public:
  TestParquetFileFormat() : ctx_(std::make_shared<ScanContext>()) {}

 protected:
  std::shared_ptr<ScanOptions> opts_ = ScanOptions::Defaults();
  std::shared_ptr<ScanContext> ctx_;
};

TEST_F(TestParquetFileFormat, ScanRecordBatchReader) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  auto fragment = std::make_shared<ParquetFragment>(*source, opts_);

  ScanTaskIterator it;
  ASSERT_OK(fragment->Scan(ctx_, &it));
  int64_t row_count = 0;

  ASSERT_OK(it.Visit([&row_count](std::unique_ptr<ScanTask> task) -> Status {
    auto batch_it = task->Scan();
    return batch_it.Visit([&row_count](std::shared_ptr<RecordBatch> batch) -> Status {
      row_count += batch->num_rows();
      return Status::OK();
    });
  }));

  ASSERT_EQ(row_count, kNumRows);
}

TEST_F(TestParquetFileFormat, OpenFailureWithRelevantError) {
  auto format = ParquetFileFormat();

  std::shared_ptr<Schema> dont_care;

  std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, testing::HasSubstr("<Buffer>"),
                                  format.Inspect({buf}, &dont_care));

  constexpr auto file_name = "herp/derp";
  ASSERT_OK_AND_ASSIGN(
      auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, testing::HasSubstr(file_name),
                                  format.Inspect({file_name, fs.get()}, &dont_care));
}

TEST_F(TestParquetFileFormat, Inspect) {
  auto reader = GetRecordBatchReader();
  auto source = GetFileSource(reader.get());
  auto format = ParquetFileFormat();

  std::shared_ptr<Schema> actual;
  ASSERT_OK(format.Inspect(*source.get(), &actual));
  EXPECT_EQ(*actual, *schema_);
}

}  // namespace dataset
}  // namespace arrow
