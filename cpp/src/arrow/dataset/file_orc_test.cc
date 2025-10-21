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

#include "arrow/dataset/file_orc.h"

#include <memory>
#include <utility>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace dataset {

class OrcFormatHelper {
 public:
  using FormatType = OrcFileFormat;
  static Result<std::shared_ptr<Buffer>> Write(RecordBatchReader* reader) {
    ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer, adapters::orc::ORCFileWriter::Open(sink.get()));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    RETURN_NOT_OK(writer->Write(*table));
    RETURN_NOT_OK(writer->Close());
    return sink->Finish();
  }

  static std::shared_ptr<OrcFileFormat> MakeFormat() {
    return std::make_shared<OrcFileFormat>();
  }
};

class TestOrcFileFormat : public FileFormatFixtureMixin<OrcFormatHelper> {};

// TEST_F(TestOrcFileFormat, WriteRecordBatchReader) { TestWrite(); }

TEST_F(TestOrcFileFormat, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::IOError, "ORC");
}
TEST_F(TestOrcFileFormat, Inspect) { TestInspect(); }
TEST_F(TestOrcFileFormat, IsSupported) { TestIsSupported(); }
TEST_F(TestOrcFileFormat, CountRows) { TestCountRows(); }
TEST_F(TestOrcFileFormat, FragmentEquals) { TestFragmentEquals(); }

// TODO add TestOrcFileSystemDataset if write support is added

class TestOrcFileFormatScan : public FileFormatScanMixin<OrcFormatHelper> {};

TEST_P(TestOrcFileFormatScan, ScanRecordBatchReader) { TestScan(); }
TEST_P(TestOrcFileFormatScan, ScanBatchSize) { TestScanBatchSize(); }
TEST_P(TestOrcFileFormatScan, ScanNoReadahead) { TestScanNoReadahead(); }
TEST_P(TestOrcFileFormatScan, ScanRecordBatchReaderProjected) { TestScanProjected(); }
TEST_P(TestOrcFileFormatScan, ScanRecordBatchReaderProjectedNested) {
  TestScanProjectedNested();
}
TEST_P(TestOrcFileFormatScan, ScanRecordBatchReaderProjectedMissingCols) {
  TestScanProjectedMissingCols();
}
TEST_P(TestOrcFileFormatScan, ScanRecordBatchReaderWithVirtualColumn) {
  TestScanWithVirtualColumn();
}
TEST_P(TestOrcFileFormatScan, ScanRecordBatchReaderWithDuplicateColumn) {
  TestScanWithDuplicateColumn();
}
TEST_P(TestOrcFileFormatScan, ScanRecordBatchReaderWithDuplicateColumnError) {
  TestScanWithDuplicateColumnError();
}
TEST_P(TestOrcFileFormatScan, ScanWithPushdownNulls) { TestScanWithPushdownNulls(); }
INSTANTIATE_TEST_SUITE_P(TestScan, TestOrcFileFormatScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
