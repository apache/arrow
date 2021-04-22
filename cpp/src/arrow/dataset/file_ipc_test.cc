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

#include "arrow/dataset/file_ipc.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/dataset/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace dataset {

using internal::checked_pointer_cast;

class ArrowIpcWriterMixin {
 public:
  static std::shared_ptr<Buffer> Write(RecordBatchReader* reader) {
    EXPECT_OK_AND_ASSIGN(auto sink, io::BufferOutputStream::Create());
    EXPECT_OK_AND_ASSIGN(auto writer, ipc::MakeFileWriter(sink, reader->schema()));
    std::vector<std::shared_ptr<RecordBatch>> batches;
    ARROW_EXPECT_OK(reader->ReadAll(&batches));
    for (auto batch : batches) {
      ARROW_EXPECT_OK(writer->WriteRecordBatch(*batch));
    }
    ARROW_EXPECT_OK(writer->Close());
    EXPECT_OK_AND_ASSIGN(auto out, sink->Finish());
    return out;
  }
};

class TestIpcFileFormat : public FileFormatFixtureMixin<ArrowIpcWriterMixin> {
 protected:
  std::shared_ptr<IpcFileFormat> format_ = std::make_shared<IpcFileFormat>();
};

TEST_F(TestIpcFileFormat, WriteRecordBatchReader) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());
  auto written = TestWrite(format_.get(), reader->schema());
  AssertBufferEqual(*written, *source->buffer());
}

TEST_F(TestIpcFileFormat, WriteRecordBatchReaderCustomOptions) {
  auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
  auto source = GetFileSource(reader.get());
  auto ipc_options =
      checked_pointer_cast<IpcFileWriteOptions>(format_->DefaultWriteOptions());
  if (util::Codec::IsAvailable(Compression::ZSTD)) {
    EXPECT_OK_AND_ASSIGN(ipc_options->options->codec,
                         util::Codec::Create(Compression::ZSTD));
  }
  ipc_options->metadata = key_value_metadata({{"hello", "world"}});

  auto written = TestWrite(format_.get(), reader->schema(), ipc_options);

  EXPECT_OK_AND_ASSIGN(auto ipc_reader, ipc::RecordBatchFileReader::Open(
                                            std::make_shared<io::BufferReader>(written)));
  EXPECT_EQ(ipc_reader->metadata()->sorted_pairs(),
            ipc_options->metadata->sorted_pairs());
}

TEST_F(TestIpcFileFormat, OpenFailureWithRelevantError) {
  TestOpenFailureWithRelevantError(format_.get(), StatusCode::Invalid);
}
TEST_F(TestIpcFileFormat, Inspect) { TestInspect(format_.get()); }
TEST_F(TestIpcFileFormat, IsSupported) { TestIsSupported(format_.get()); }

class TestIpcFileSystemDataset : public testing::Test,
                                 public WriteFileSystemDatasetMixin {
 public:
  void SetUp() override {
    MakeSourceDataset();
    auto ipc_format = std::make_shared<IpcFileFormat>();
    format_ = ipc_format;
    SetWriteOptions(ipc_format->DefaultWriteOptions());
  }

  std::shared_ptr<Scanner> MakeScanner(const std::shared_ptr<Dataset>& dataset,
                                       const std::shared_ptr<ScanOptions>& scan_options) {
    ScannerBuilder builder(dataset, scan_options);
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    return scanner;
  }
};

TEST_F(TestIpcFileSystemDataset, WriteWithIdenticalPartitioningSchema) {
  TestWriteWithIdenticalPartitioningSchema();
}

TEST_F(TestIpcFileSystemDataset, WriteWithUnrelatedPartitioningSchema) {
  TestWriteWithUnrelatedPartitioningSchema();
}

TEST_F(TestIpcFileSystemDataset, WriteWithSupersetPartitioningSchema) {
  TestWriteWithSupersetPartitioningSchema();
}

TEST_F(TestIpcFileSystemDataset, WriteWithEmptyPartitioningSchema) {
  TestWriteWithEmptyPartitioningSchema();
}

TEST_F(TestIpcFileSystemDataset, WriteExceedsMaxPartitions) {
  write_options_.partitioning = std::make_shared<DirectoryPartitioning>(
      SchemaFromColumnNames(source_schema_, {"model"}));

  // require that no batch be grouped into more than 2 written batches:
  write_options_.max_partitions = 2;

  auto scanner = MakeScanner(dataset_, scan_options_);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("This exceeds the maximum"),
                                  FileSystemDataset::Write(write_options_, scanner));
}

class TestIpcFileFormatScan : public FileFormatScanMixin<ArrowIpcWriterMixin> {
 protected:
  std::shared_ptr<IpcFileFormat> format_ = std::make_shared<IpcFileFormat>();
};

TEST_P(TestIpcFileFormatScan, ScanRecordBatchReader) { TestScan(format_.get()); }
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderWithVirtualColumn) {
  TestScanWithVirtualColumn(format_.get());
}
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderProjected) {
  TestScanProjected(format_.get());
}
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderProjectedMissingCols) {
  TestScanProjectedMissingCols(format_.get());
}
TEST_P(TestIpcFileFormatScan, FragmentScanOptions) {
  auto reader = GetRecordBatchReader(
      // ARROW-12077: on Windows/mimalloc/release, nullable list column leads to crash
      schema({field("list", list(float64()), false,
                    key_value_metadata({{"max_length", "1"}})),
              field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  // Set scan options that ensure reading fails
  auto fragment_scan_options = std::make_shared<IpcFragmentScanOptions>();
  fragment_scan_options->options = std::make_shared<ipc::IpcReadOptions>();
  fragment_scan_options->options->max_recursion_depth = 0;
  opts_->fragment_scan_options = fragment_scan_options;
  ASSERT_OK_AND_ASSIGN(auto scan_tasks, fragment->Scan(opts_));
  ASSERT_OK_AND_ASSIGN(auto scan_task, scan_tasks.Next());
  ASSERT_OK_AND_ASSIGN(auto batches, scan_task->Execute());
  ASSERT_RAISES(Invalid, batches.Next());
}
INSTANTIATE_TEST_SUITE_P(TestScan, TestIpcFileFormatScan,
                         ::testing::ValuesIn(TestScannerParams::Values()),
                         TestScannerParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
