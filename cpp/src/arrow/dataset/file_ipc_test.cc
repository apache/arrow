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
#include "arrow/dataset/test_util_internal.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

class IpcFormatHelper {
 public:
  using FormatType = IpcFileFormat;
  static Result<std::shared_ptr<Buffer>> Write(RecordBatchReader* reader) {
    ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer, ipc::MakeFileWriter(sink, reader->schema()));
    ARROW_ASSIGN_OR_RAISE(auto batches, reader->ToRecordBatches());
    for (auto batch : batches) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }
    RETURN_NOT_OK(writer->Close());
    return sink->Finish();
  }

  static std::shared_ptr<IpcFileFormat> MakeFormat() {
    return std::make_shared<IpcFileFormat>();
  }
};

class TestIpcFileFormat : public FileFormatFixtureMixin<IpcFormatHelper> {};

TEST_F(TestIpcFileFormat, WriteRecordBatchReader) { TestWrite(); }

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

  auto written = WriteToBuffer(reader->schema(), ipc_options);

  EXPECT_OK_AND_ASSIGN(auto ipc_reader, ipc::RecordBatchFileReader::Open(
                                            std::make_shared<io::BufferReader>(written)));
  EXPECT_EQ(ipc_reader->metadata()->sorted_pairs(),
            ipc_options->metadata->sorted_pairs());
}

TEST_F(TestIpcFileFormat, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::Invalid, "IPC");
}
TEST_F(TestIpcFileFormat, Inspect) { TestInspect(); }
TEST_F(TestIpcFileFormat, IsSupported) { TestIsSupported(); }
TEST_F(TestIpcFileFormat, CountRows) { TestCountRows(); }
TEST_F(TestIpcFileFormat, FragmentEquals) { TestFragmentEquals(); }

class TestIpcFileSystemDataset : public testing::Test,
                                 public WriteFileSystemDatasetMixin {
 public:
  void SetUp() override {
    MakeSourceDataset();
    auto ipc_format = std::make_shared<IpcFileFormat>();
    format_ = ipc_format;
    SetWriteOptions(ipc_format->DefaultWriteOptions());
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

  auto scanner_builder = ScannerBuilder(dataset_, scan_options_);
  EXPECT_OK_AND_ASSIGN(auto scanner, scanner_builder.Finish());
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("This exceeds the maximum"),
                                  FileSystemDataset::Write(write_options_, scanner));
}

class TestIpcFileFormatScan : public FileFormatScanMixin<IpcFormatHelper> {};

TEST_P(TestIpcFileFormatScan, ScanRecordBatchReader) { TestScan(); }
TEST_P(TestIpcFileFormatScan, ScanBatchSize) { TestScanBatchSize(); }
TEST_P(TestIpcFileFormatScan, ScanNoReadahead) { TestScanNoReadahead(); }
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderProjected) { TestScanProjected(); }
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderProjectedNested) {
  TestScanProjectedNested();
}
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderProjectedMissingCols) {
  TestScanProjectedMissingCols();
}
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderWithVirtualColumn) {
  TestScanWithVirtualColumn();
}
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderWithDuplicateColumn) {
  TestScanWithDuplicateColumn();
}
TEST_P(TestIpcFileFormatScan, ScanRecordBatchReaderWithDuplicateColumnError) {
  TestScanWithDuplicateColumnError();
}
TEST_P(TestIpcFileFormatScan, ScanWithPushdownNulls) { TestScanWithPushdownNulls(); }
TEST_P(TestIpcFileFormatScan, FragmentScanOptions) {
  auto reader = GetRecordBatchReader(
      // ARROW-12077: on Windows/mimalloc/release, nullable list column leads to crash
      schema({field("list", list(float64()), false,
                    key_value_metadata({{"max_length", "1"}})),
              field("f64", float64())}));
  auto source = GetFileSource(reader.get());

  SetSchema(reader->schema()->fields());
  auto fragment = MakeFragment(*source);

  // Set scan options that ensure reading fails
  auto fragment_scan_options = std::make_shared<IpcFragmentScanOptions>();
  fragment_scan_options->options = std::make_shared<ipc::IpcReadOptions>();
  fragment_scan_options->options->max_recursion_depth = 0;
  opts_->fragment_scan_options = fragment_scan_options;
  ASSERT_OK_AND_ASSIGN(auto batch_gen, fragment->ScanBatchesAsync(opts_));
  ASSERT_FINISHES_AND_RAISES(Invalid, CollectAsyncGenerator(batch_gen));
}
INSTANTIATE_TEST_SUITE_P(TestScan, TestIpcFileFormatScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
