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

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
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

TEST_P(TestOrcFileFormatScan, PredicatePushdownAllNullStripes) {
  auto value_field = field("i64", int64());
  const auto test_schema = schema({value_field});

  const int64_t nrows = 2048;
  Int64Builder null_builder;
  ASSERT_OK(null_builder.AppendNulls(nrows));
  ASSERT_OK_AND_ASSIGN(auto all_null_values, null_builder.Finish());

  Int64Builder value_builder;
  for (int64_t i = 0; i < nrows; ++i) {
    ASSERT_OK(value_builder.Append(i));
  }
  ASSERT_OK_AND_ASSIGN(auto non_null_values, value_builder.Finish());

  auto all_null_batch = RecordBatch::Make(test_schema, nrows, {all_null_values});
  auto non_null_batch = RecordBatch::Make(test_schema, nrows, {non_null_values});

  ASSERT_OK_AND_ASSIGN(auto sink, io::BufferOutputStream::Create());
  adapters::orc::WriteOptions write_options;
  write_options.stripe_size = 4096;
  ASSERT_OK_AND_ASSIGN(auto writer,
                       adapters::orc::ORCFileWriter::Open(sink.get(), write_options));
  ASSERT_OK(writer->Write(*all_null_batch));
  ASSERT_OK(writer->Write(*non_null_batch));
  ASSERT_OK(writer->Close());
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  FileSource source(buffer);
  ASSERT_OK_AND_ASSIGN(auto fragment_base, format_->MakeFragment(source, literal(true)));
  auto orc_fragment = checked_pointer_cast<OrcFileFragment>(fragment_base);

  ASSERT_OK_AND_ASSIGN(auto input, source.Open());
  ASSERT_OK_AND_ASSIGN(auto reader,
                       adapters::orc::ORCFileReader::Open(std::move(input),
                                                          default_memory_pool()));

  std::vector<int> all_null_stripes;
  std::vector<int> non_all_null_stripes;
  const int64_t num_stripes = reader->NumberOfStripes();
  for (int64_t stripe = 0; stripe < num_stripes; ++stripe) {
    ASSERT_OK_AND_ASSIGN(auto stripe_stats, reader->GetStripeStatistics(stripe));
    const auto* col_stats = stripe_stats->getColumnStatistics(1);
    ASSERT_NE(col_stats, nullptr);

    if (col_stats->hasNull() && col_stats->getNumberOfValues() == 0) {
      all_null_stripes.push_back(static_cast<int>(stripe));
    } else {
      non_all_null_stripes.push_back(static_cast<int>(stripe));
    }
  }
  ASSERT_FALSE(all_null_stripes.empty());
  ASSERT_FALSE(non_all_null_stripes.empty());

  ASSERT_OK_AND_ASSIGN(
      auto is_null_selected,
      orc_fragment->FilterStripes(compute::is_null(compute::field_ref("i64"))));
  for (int stripe : all_null_stripes) {
    EXPECT_NE(std::find(is_null_selected.begin(), is_null_selected.end(), stripe),
              is_null_selected.end());
  }

  ASSERT_OK_AND_ASSIGN(
      auto is_not_null_selected,
      orc_fragment->FilterStripes(
          compute::not_(compute::is_null(compute::field_ref("i64")))));
  for (int stripe : all_null_stripes) {
    EXPECT_EQ(std::find(is_not_null_selected.begin(), is_not_null_selected.end(), stripe),
              is_not_null_selected.end());
  }
}

TEST_P(TestOrcFileFormatScan, PredicatePushdownWithFileSystemSource) {
  auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
  std::shared_ptr<Schema> test_schema = schema({field("x", int64())});
  std::shared_ptr<RecordBatch> batch = RecordBatchFromJSON(test_schema, "[[0], [1], [2]]");

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<io::OutputStream> out_stream,
                       mock_fs->OpenOutputStream("/foo.orc"));
  ASSERT_OK_AND_ASSIGN(auto writer, adapters::orc::ORCFileWriter::Open(out_stream.get()));
  ASSERT_OK(writer->Write(*batch));
  ASSERT_OK(writer->Close());

  FileSource source("/foo.orc", mock_fs);
  ASSERT_OK_AND_ASSIGN(auto fragment_base, format_->MakeFragment(source, literal(true)));
  auto orc_fragment = checked_pointer_cast<OrcFileFragment>(fragment_base);

  ASSERT_OK_AND_ASSIGN(
      auto stripes,
      orc_fragment->FilterStripes(compute::greater(compute::field_ref("x"),
                                                   compute::literal(int64_t{1}))));
  ASSERT_FALSE(stripes.empty());
}

TEST_P(TestOrcFileFormatScan, PredicatePushdownRepeatedFilterCallsAreStable) {
  auto test_schema = schema({field("a", int64()), field("b", int64())});
  const int64_t nrows = 2048;

  Int64Builder a_first_builder;
  Int64Builder b_first_builder;
  Int64Builder a_second_builder;
  Int64Builder b_second_builder;
  for (int64_t i = 0; i < nrows; ++i) {
    ASSERT_OK(a_first_builder.Append(i));
    ASSERT_OK(b_first_builder.Append(10000 + i));
    ASSERT_OK(a_second_builder.Append(10000 + i));
    ASSERT_OK(b_second_builder.Append(i));
  }
  ASSERT_OK_AND_ASSIGN(auto a_first, a_first_builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto b_first, b_first_builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto a_second, a_second_builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto b_second, b_second_builder.Finish());

  auto first_batch = RecordBatch::Make(test_schema, nrows, {a_first, b_first});
  auto second_batch = RecordBatch::Make(test_schema, nrows, {a_second, b_second});

  ASSERT_OK_AND_ASSIGN(auto sink, io::BufferOutputStream::Create());
  adapters::orc::WriteOptions write_options;
  write_options.stripe_size = 4096;
  ASSERT_OK_AND_ASSIGN(auto writer,
                       adapters::orc::ORCFileWriter::Open(sink.get(), write_options));
  ASSERT_OK(writer->Write(*first_batch));
  ASSERT_OK(writer->Write(*second_batch));
  ASSERT_OK(writer->Close());
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  FileSource source(buffer);
  ASSERT_OK_AND_ASSIGN(auto fragment_base, format_->MakeFragment(source, literal(true)));
  auto orc_fragment = checked_pointer_cast<OrcFileFragment>(fragment_base);

  ASSERT_OK_AND_ASSIGN(auto input, source.Open());
  ASSERT_OK_AND_ASSIGN(auto reader,
                       adapters::orc::ORCFileReader::Open(std::move(input),
                                                          default_memory_pool()));

  std::vector<int> expected_a;
  std::vector<int> expected_b;
  for (int64_t stripe = 0; stripe < reader->NumberOfStripes(); ++stripe) {
    ASSERT_OK_AND_ASSIGN(auto stripe_batch, reader->ReadStripe(stripe));
    auto a_array = checked_pointer_cast<Int64Array>(stripe_batch->column(0));
    auto b_array = checked_pointer_cast<Int64Array>(stripe_batch->column(1));

    bool a_has_match = false;
    bool b_has_match = false;
    for (int64_t i = 0; i < stripe_batch->num_rows(); ++i) {
      if (!a_array->IsNull(i) && a_array->Value(i) < 10) {
        a_has_match = true;
      }
      if (!b_array->IsNull(i) && b_array->Value(i) < 10) {
        b_has_match = true;
      }
      if (a_has_match && b_has_match) {
        break;
      }
    }
    if (a_has_match) {
      expected_a.push_back(static_cast<int>(stripe));
    }
    if (b_has_match) {
      expected_b.push_back(static_cast<int>(stripe));
    }
  }

  auto pred_a = compute::less(compute::field_ref("a"), compute::literal(int64_t{10}));
  auto pred_b = compute::less(compute::field_ref("b"), compute::literal(int64_t{10}));

  ASSERT_OK_AND_ASSIGN(auto selected_a_first, orc_fragment->FilterStripes(pred_a));
  ASSERT_OK_AND_ASSIGN(auto selected_b, orc_fragment->FilterStripes(pred_b));
  ASSERT_OK_AND_ASSIGN(auto selected_a_second, orc_fragment->FilterStripes(pred_a));

  EXPECT_EQ(selected_a_first, expected_a);
  EXPECT_EQ(selected_b, expected_b);
  EXPECT_EQ(selected_a_second, expected_a);
}

INSTANTIATE_TEST_SUITE_P(TestScan, TestOrcFileFormatScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
