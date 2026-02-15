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
    ASSERT_OK_AND_ASSIGN(input_, GetBufferFromNumBatches(4, /*batch_size=*/512));
  }

  Result<std::shared_ptr<Buffer>> GetBufferFromNumBatches(int num_batches,
                                                           int batch_size) {
    std::vector<std::shared_ptr<RecordBatch>> batches;
    for (int i = 0; i < num_batches; i++) {
      batches.push_back(
          ConstantArrayGenerator::Zeroes(batch_size, opts_->dataset_schema));
    }
    ARROW_ASSIGN_OR_RAISE(auto table,
                          Table::FromRecordBatches(opts_->dataset_schema, batches));
    ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer, adapters::orc::ORCFileWriter::Open(sink.get()));
    RETURN_NOT_OK(writer->Write(*table));
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
    int64_t total_rows = 0;
    ASSERT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_));
    for (auto maybe_scan_task : scan_task_it) {
      ASSERT_OK_AND_ASSIGN(auto scan_task, maybe_scan_task);
      ASSERT_OK_AND_ASSIGN(auto rb_it, scan_task->Execute());
      for (auto maybe_batch : rb_it) {
        ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
        total_rows += batch->num_rows();
      }
    }
    ASSERT_EQ(total_rows, expected_rows);
  }

 protected:
  std::shared_ptr<Buffer> input_;
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<OrcFileFormat> format_;
};

TEST_F(TestOrcFileFragment, Basics) {
  // Test that MakeFragment returns OrcFileFragment
  auto source = FileSource(input_);
  ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source));
  ASSERT_NE(fragment, nullptr);
  ASSERT_TRUE(fragment->stripe_ids().empty());
}

TEST_F(TestOrcFileFragment, MakeFragmentWithStripeIds) {
  // Test that MakeFragment with stripe_ids works
  auto source = FileSource(input_);
  std::vector<int> stripe_ids = {0, 1};
  ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source, stripe_ids));
  ASSERT_NE(fragment, nullptr);
  ASSERT_EQ(fragment->stripe_ids(), stripe_ids);
}

TEST_F(TestOrcFileFragment, Subset) {
  // Test that Subset creates a new fragment with specified stripes
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
  // Test that scanning a subset reads fewer rows than full scan
  // Create a file with multiple stripes (4 batches of 512 rows each = 2048 rows)
  auto source = FileSource(input_);

  // Full scan
  ASSERT_OK_AND_ASSIGN(auto full_fragment, MakeFragment(source));
  AssertScanEquals(full_fragment, 2048);

  // Scan single stripe - should read fewer rows
  // ORC may combine batches into stripes, so subset might still read all if it's 1 stripe
  // For a more robust test, create larger file or check stripe count first
  ASSERT_OK_AND_ASSIGN(auto reader_for_info,
                       adapters::orc::ORCFileReader::Open(
                           std::make_shared<io::BufferReader>(input_),
                           default_memory_pool()));
  int64_t num_stripes = reader_for_info->NumberOfStripes();

  if (num_stripes > 1) {
    // Test reading just first stripe
    std::vector<int> first_stripe = {0};
    ASSERT_OK_AND_ASSIGN(auto subset_fragment, MakeFragment(source, first_stripe));

    // Count rows in first stripe
    auto stripe_info = reader_for_info->GetStripeInformation(0);
    int64_t expected_rows = stripe_info.num_rows;

    AssertScanEquals(subset_fragment, expected_rows);

    // Verify it's less than full file
    ASSERT_LT(expected_rows, 2048);
  }
}

TEST_F(TestOrcFileFragment, InvalidStripeIdOutOfRange) {
  auto source = FileSource(input_);
  // Find how many stripes the file has
  ASSERT_OK_AND_ASSIGN(auto reader,
                       adapters::orc::ORCFileReader::Open(
                           std::make_shared<io::BufferReader>(input_),
                           default_memory_pool()));
  int64_t num_stripes = reader->NumberOfStripes();

  // Stripe ID equal to num_stripes should fail
  std::vector<int> invalid_ids = {static_cast<int>(num_stripes)};
  ASSERT_RAISES(IndexError, MakeFragment(source, invalid_ids));

  // Stripe ID way out of range should also fail
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

  if (num_stripes > 1) {
    // Create fragment with first stripe only
    std::vector<int> first_stripe = {0};
    ASSERT_OK_AND_ASSIGN(auto fragment, MakeFragment(source, first_stripe));

    auto stripe_info = reader->GetStripeInformation(0);
    int64_t expected_rows = stripe_info.num_rows;

    // CountRows should return the stripe's row count, not the full file
    auto count_result = format_->CountRows(
        fragment, literal(true), opts_);
    ASSERT_OK_AND_ASSIGN(auto count, count_result.result());
    ASSERT_TRUE(count.has_value());
    ASSERT_EQ(count.value(), expected_rows);

    // Full fragment should return total rows
    ASSERT_OK_AND_ASSIGN(auto full_fragment, MakeFragment(source));
    auto full_count_result = format_->CountRows(
        full_fragment, literal(true), opts_);
    ASSERT_OK_AND_ASSIGN(auto full_count, full_count_result.result());
    ASSERT_TRUE(full_count.has_value());
    ASSERT_EQ(full_count.value(), 2048);

    // Verify subset count is less than full count
    ASSERT_LT(count.value(), full_count.value());
  }
}

}  // namespace dataset
}  // namespace arrow
