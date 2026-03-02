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
#include "arrow/dataset/scanner.h"
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

class TestOrcFileFragment : public ::testing::Test {
 public:
  void SetUp() override {
    format_ = std::make_shared<OrcFileFormat>();
    opts_ = std::make_shared<ScanOptions>();
    opts_->dataset_schema = schema({field("f64", float64())});
    SetSchema(opts_->dataset_schema->fields());
  }

  void SetSchema(std::vector<std::shared_ptr<Field>> fields) {
    opts_->dataset_schema = schema(std::move(fields));
    ASSERT_OK_AND_ASSIGN(input_, WriteMultiStripeBuffer(4, /*batch_size=*/512));
  }

  Result<std::shared_ptr<Buffer>> WriteMultiStripeBuffer(int num_batches,
                                                         int batch_size) {
    adapters::orc::WriteOptions write_opts;
    write_opts.stripe_size = 1024;

    ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer,
                          adapters::orc::ORCFileWriter::Open(sink.get(), write_opts));
    for (int i = 0; i < num_batches; i++) {
      auto batch =
          ConstantArrayGenerator::Zeroes(batch_size, opts_->dataset_schema);
      RETURN_NOT_OK(writer->Write(*batch));
    }
    RETURN_NOT_OK(writer->Close());
    return sink->Finish();
  }

  Result<std::shared_ptr<OrcFileFragment>> MakeFragment(FileSource source) {
    ARROW_ASSIGN_OR_RAISE(auto fragment,
                          format_->MakeFragment(std::move(source), literal(true),
                                               opts_->dataset_schema));
    return std::dynamic_pointer_cast<OrcFileFragment>(fragment);
  }

  Result<std::shared_ptr<OrcFileFragment>> MakeFragment(
      FileSource source, std::vector<int> stripe_ids) {
    return format_->MakeFragment(std::move(source), literal(true),
                                opts_->dataset_schema, std::move(stripe_ids));
  }

  void AssertScanEquals(std::shared_ptr<Fragment> fragment, int64_t expected_rows) {
    auto dataset = std::make_shared<FragmentDataset>(
        opts_->dataset_schema, FragmentVector{std::move(fragment)});
    ScannerBuilder builder(dataset, opts_);
    ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table, scanner->ToTable());
    ASSERT_EQ(table->num_rows(), expected_rows);
  }

 protected:
  std::shared_ptr<Buffer> input_;
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<OrcFileFormat> format_;
};

TEST_F(TestOrcFileFragment, Basics) {
  auto source = FileSource(input_);
  ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source));
  ASSERT_NE(fragment, nullptr);
  ASSERT_TRUE(fragment->stripe_ids().empty());
}

TEST_F(TestOrcFileFragment, MakeFragmentWithStripeIds) {
  auto source = FileSource(input_);
  std::vector<int> stripe_ids = {0, 1};
  ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source, stripe_ids));
  ASSERT_NE(fragment, nullptr);
  ASSERT_EQ(fragment->stripe_ids(), stripe_ids);
}

TEST_F(TestOrcFileFragment, Subset) {
  auto source = FileSource(input_);
  ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source));

  std::vector<int> stripe_ids = {0};
  ASSERT_OK_AND_ASSIGN(auto subset_fragment, fragment->Subset(stripe_ids));
  ASSERT_NE(subset_fragment, nullptr);

  auto* orc_subset = dynamic_cast<OrcFileFragment*>(subset_fragment.get());
  ASSERT_NE(orc_subset, nullptr);
  ASSERT_EQ(orc_subset->stripe_ids(), stripe_ids);
}

TEST_F(TestOrcFileFragment, ScanSubset) {
  auto source = FileSource(input_);

  ASSERT_OK_AND_ASSIGN(auto reader,
                       adapters::orc::ORCFileReader::Open(
                           std::make_shared<io::BufferReader>(input_),
                           default_memory_pool()));
  int64_t num_stripes = reader->NumberOfStripes();
  ASSERT_GT(num_stripes, 1) << "Test file should have multiple stripes";

  ASSERT_OK_AND_ASSIGN(auto full_fragment, MakeFragment(source));
  AssertScanEquals(full_fragment, 2048);

  std::vector<int> first_stripe = {0};
  ASSERT_OK_AND_ASSIGN(auto subset_fragment, MakeFragment(source, first_stripe));

  auto stripe_info = reader->GetStripeInformation(0);
  int64_t expected_rows = stripe_info.num_rows;
  ASSERT_LT(expected_rows, 2048);
  AssertScanEquals(subset_fragment, expected_rows);

  ASSERT_OK_AND_ASSIGN(auto subset_via_subset, full_fragment->Subset(first_stripe));
  AssertScanEquals(subset_via_subset, expected_rows);
}

TEST_F(TestOrcFileFragment, InvalidStripeIdOutOfRange) {
  auto source = FileSource(input_);
  ASSERT_OK_AND_ASSIGN(auto reader,
                       adapters::orc::ORCFileReader::Open(
                           std::make_shared<io::BufferReader>(input_),
                           default_memory_pool()));
  int64_t num_stripes = reader->NumberOfStripes();

  std::vector<int> invalid_ids = {static_cast<int>(num_stripes)};
  ASSERT_RAISES(IndexError, MakeFragment(source, invalid_ids));

  std::vector<int> very_invalid_ids = {9999};
  ASSERT_RAISES(IndexError, MakeFragment(source, very_invalid_ids));
}

TEST_F(TestOrcFileFragment, InvalidStripeIdNegative) {
  auto source = FileSource(input_);
  std::vector<int> negative_ids = {-1};
  ASSERT_RAISES(IndexError, MakeFragment(source, negative_ids));
}

TEST_F(TestOrcFileFragment, CountRowsWithStripeSubset) {
  auto source = FileSource(input_);
  ASSERT_OK_AND_ASSIGN(auto reader,
                       adapters::orc::ORCFileReader::Open(
                           std::make_shared<io::BufferReader>(input_),
                           default_memory_pool()));
  int64_t num_stripes = reader->NumberOfStripes();
  ASSERT_GT(num_stripes, 1) << "Test file should have multiple stripes";

  std::vector<int> first_stripe = {0};
  ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source, first_stripe));

  auto stripe_info = reader->GetStripeInformation(0);
  int64_t expected_rows = stripe_info.num_rows;

  auto count_result = format_->CountRows(fragment, literal(true), opts_);
  ASSERT_OK_AND_ASSIGN(auto count, count_result.result());
  ASSERT_TRUE(count.has_value());
  ASSERT_EQ(count.value(), expected_rows);

  ASSERT_OK_AND_ASSIGN(auto full_fragment, MakeFragment(source));
  auto full_count_result = format_->CountRows(full_fragment, literal(true), opts_);
  ASSERT_OK_AND_ASSIGN(auto full_count, full_count_result.result());
  ASSERT_TRUE(full_count.has_value());
  ASSERT_EQ(full_count.value(), 2048);

  ASSERT_LT(count.value(), full_count.value());
}

}  // namespace dataset
}  // namespace arrow
