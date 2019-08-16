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

#include "arrow/testing/test_data.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/dataset/file_base.h"
#include "arrow/record_batch.h"
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

// Convenience class allowing to retrieve integration data easily, e.g.
//
// class MyTestClass : public ::testing::Test, TestDataFixtureMixin {
// }
//
// TEST(MyTestClass, ATest) {
//    this->OpenParquetFile("data/binary.parquet")
//    // Do something
// }
class TestDataFixtureMixin : public ::testing::Test {
 public:
  TestDataFixtureMixin() {
    auto root = std::make_shared<fs::LocalFileSystem>();
    arrow_fs_ = std::make_shared<fs::SubTreeFileSystem>(ArrowTestDataPath(), root);
    parquet_fs_ = std::make_shared<fs::SubTreeFileSystem>(ParquetTestDataPath(), root);
  }

  std::shared_ptr<io::RandomAccessFile> OpenParquetFile(
      const std::string& relative_path) {
    std::shared_ptr<io::RandomAccessFile> file;
    ARROW_EXPECT_OK(ParquetDataFileSystem()->OpenInputFile(relative_path, &file));
    return file;
  }

  std::shared_ptr<io::RandomAccessFile> OpenArrowFile(const std::string& relative_path) {
    std::shared_ptr<io::RandomAccessFile> file;
    ARROW_EXPECT_OK(ArrowDataFileSystem()->OpenInputFile(relative_path, &file));
    return file;
  }

  fs::FileSystem* ArrowDataFileSystem() const { return arrow_fs_.get(); }
  fs::FileSystem* ParquetDataFileSystem() const { return parquet_fs_.get(); }

 protected:
  std::shared_ptr<fs::FileSystem> arrow_fs_;
  std::shared_ptr<fs::FileSystem> parquet_fs_;
};

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

// Convenience class allowing easy retrieval of FileSources pointing to test data.
class FileSourceFixtureMixin : public TestDataFixtureMixin {
 public:
  std::unique_ptr<FileSource> GetParquetDataSource(
      const std::string& path,
      Compression::type compression = Compression::UNCOMPRESSED) {
    return internal::make_unique<FileSource>(path, parquet_fs_.get(), compression);
  }

  std::unique_ptr<FileSource> GetArrowDataSource(
      const std::string& path,
      Compression::type compression = Compression::UNCOMPRESSED) {
    return internal::make_unique<FileSource>(path, arrow_fs_.get(), compression);
  }

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

template <typename Gen>
std::unique_ptr<GeneratedRecordBatch<Gen>> MakeGeneratedRecordBatch(
    std::shared_ptr<Schema> schema, Gen&& gen) {
  return internal::make_unique<GeneratedRecordBatch<Gen>>(schema, std::forward<Gen>(gen));
}

}  // namespace dataset
}  // namespace arrow
