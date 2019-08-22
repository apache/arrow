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
#include <utility>

#include "arrow/dataset/file_base.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/stl.h"

#include "parquet/arrow/writer.h"

namespace arrow {
namespace dataset {

using parquet::ArrowWriterProperties;
using parquet::default_arrow_writer_properties;

using parquet::default_writer_properties;
using parquet::WriterProperties;

using parquet::CreateOutputStream;
using parquet::arrow::FileWriter;
using parquet::arrow::WriteTable;

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

  return reader->Visit([&](std::shared_ptr<RecordBatch> batch) -> Status {
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

class FileSourceFixtureMixin : public ::testing::Test {
 public:
  std::unique_ptr<FileSource> GetSource(std::shared_ptr<Buffer> buffer) {
    return internal::make_unique<FileSource>(std::move(buffer));
  }
};

template <typename Gen>
class GeneratedRecordBatch : public RecordBatchReader {
 public:
  GeneratedRecordBatch(std::shared_ptr<Schema> schema, Gen gen)
      : schema_(schema), gen_(gen) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override { return gen_(batch); }

 private:
  std::shared_ptr<Schema> schema_;
  Gen gen_;
};

class DatasetFixtureMixin : public ::testing::Test {
 public:
  DatasetFixtureMixin() : ctx_(std::make_shared<ScanContext>()) {}

 protected:
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> ctx_;
};

class TestDataFragmentMixin : public DatasetFixtureMixin {
 public:
  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertFragmentEquals(RecordBatchReader* expected, DataFragment* fragment) {
    std::unique_ptr<ScanTaskIterator> it;
    ARROW_EXPECT_OK(fragment->Scan(ctx_, &it));

    ARROW_EXPECT_OK(it->Visit([expected](std::unique_ptr<ScanTask> task) -> Status {
      auto batch_it = task->Scan();
      return batch_it->Visit([expected](std::shared_ptr<RecordBatch> rhs) -> Status {
        std::shared_ptr<RecordBatch> lhs;
        RETURN_NOT_OK(expected->ReadNext(&lhs));
        EXPECT_NE(lhs, nullptr);
        AssertBatchesEqual(*lhs, *rhs);
        return Status::OK();
      });
    }));
  }
};

class TestDataSourceMixin : public TestDataFragmentMixin {
 public:
  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragments of a source.
  void AssertDataSourceEquals(RecordBatchReader* expected, DataSource* source) {
    auto it = source->GetFragments(options_);

    ARROW_EXPECT_OK(it->Visit([&](std::shared_ptr<DataFragment> fragment) -> Status {
      AssertFragmentEquals(expected, fragment.get());
      return Status::OK();
    }));
  }
};

template <typename Gen>
std::unique_ptr<GeneratedRecordBatch<Gen>> MakeGeneratedRecordBatch(
    std::shared_ptr<Schema> schema, Gen&& gen) {
  return internal::make_unique<GeneratedRecordBatch<Gen>>(schema, std::forward<Gen>(gen));
}

}  // namespace dataset
}  // namespace arrow
