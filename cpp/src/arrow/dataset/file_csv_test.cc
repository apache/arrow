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

#include "arrow/dataset/file_csv.h"

#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/compressed.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace dataset {

class TestCsvFileFormat : public testing::TestWithParam<Compression::type> {
 public:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<FileSource> GetFileSource(std::string csv) {
    if (GetCompression() == Compression::UNCOMPRESSED) {
      return internal::make_unique<FileSource>(Buffer::FromString(std::move(csv)));
    }
    std::string path = "test.csv";
    switch (GetCompression()) {
      case Compression::type::GZIP:
        path += ".gz";
        break;
      case Compression::type::ZSTD:
        path += ".zstd";
        break;
      case Compression::type::LZ4_FRAME:
        path += ".lz4";
        break;
      case Compression::type::BZ2:
        path += ".bz2";
        break;
      default:
        // No known extension
        break;
    }
    EXPECT_OK_AND_ASSIGN(auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {}));
    EXPECT_OK_AND_ASSIGN(auto codec, util::Codec::Create(GetCompression()));
    EXPECT_OK_AND_ASSIGN(auto buffer_writer, fs->OpenOutputStream(path));
    EXPECT_OK_AND_ASSIGN(auto stream,
                         io::CompressedOutputStream::Make(codec.get(), buffer_writer));
    ARROW_EXPECT_OK(stream->Write(csv));
    ARROW_EXPECT_OK(stream->Close());
    EXPECT_OK_AND_ASSIGN(auto info, fs->GetFileInfo(path));
    return internal::make_unique<FileSource>(info, fs, GetCompression());
  }

  RecordBatchIterator Batches(ScanTaskIterator scan_task_it) {
    return MakeFlattenIterator(MakeMaybeMapIterator(
        [](std::shared_ptr<ScanTask> scan_task) { return scan_task->Execute(); },
        std::move(scan_task_it)));
  }

  RecordBatchIterator Batches(Fragment* fragment) {
    EXPECT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_));
    return Batches(std::move(scan_task_it));
  }

  void SetSchema(std::vector<std::shared_ptr<Field>> fields) {
    opts_->dataset_schema = schema(std::move(fields));
    ASSERT_OK(SetProjection(opts_.get(), opts_->dataset_schema->field_names()));
  }

  std::shared_ptr<CsvFileFormat> format_ = std::make_shared<CsvFileFormat>();
  std::shared_ptr<ScanOptions> opts_ = std::make_shared<ScanOptions>();
};

TEST_P(TestCsvFileFormat, ScanRecordBatchReader) {
  auto source = GetFileSource(R"(f64
1.0

N/A
2)");
  SetSchema({field("f64", float64())});
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, 3);
}

TEST_P(TestCsvFileFormat, CustomConvertOptions) {
  auto source = GetFileSource(R"(str
foo
MYNULL
N/A
bar)");
  SetSchema({field("str", utf8())});
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));
  auto fragment_scan_options = std::make_shared<CsvFragmentScanOptions>();
  fragment_scan_options->convert_options.null_values = {"MYNULL"};
  fragment_scan_options->convert_options.strings_can_be_null = true;
  opts_->fragment_scan_options = fragment_scan_options;

  int64_t null_count = 0;
  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    null_count += batch->GetColumnByName("str")->null_count();
  }

  ASSERT_EQ(null_count, 1);
}

TEST_P(TestCsvFileFormat, CustomReadOptions) {
  auto source = GetFileSource(R"(header_skipped
str
foo
MYNULL
N/A
bar)");
  SetSchema({field("str", utf8())});
  auto defaults = std::make_shared<CsvFragmentScanOptions>();
  defaults->read_options.skip_rows = 1;
  format_->default_fragment_scan_options = defaults;
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));
  ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(opts_->dataset_schema, physical_schema);

  {
    int64_t rows = 0;
    for (auto maybe_batch : Batches(fragment.get())) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      rows += batch->GetColumnByName("str")->length();
    }
    ASSERT_EQ(rows, 4);
  }
  {
    // These options completely override the default ones
    auto fragment_scan_options = std::make_shared<CsvFragmentScanOptions>();
    fragment_scan_options->read_options.block_size = 1 << 22;
    opts_->fragment_scan_options = fragment_scan_options;
    int64_t rows = 0;
    for (auto maybe_batch : Batches(fragment.get())) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      rows += batch->GetColumnByName("header_skipped")->length();
    }
    ASSERT_EQ(rows, 5);
  }
}

TEST_P(TestCsvFileFormat, ScanRecordBatchReaderWithVirtualColumn) {
  auto source = GetFileSource(R"(f64
1.0

N/A
2)");
  // NB: dataset_schema includes a column not present in the file
  SetSchema({field("f64", float64()), field("virtual", int32())});
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(Schema({field("f64", float64())}), *physical_schema);

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    AssertSchemaEqual(*batch->schema(), *physical_schema);
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, 3);
}

TEST_P(TestCsvFileFormat, OpenFailureWithRelevantError) {
  if (GetCompression() != Compression::type::UNCOMPRESSED) {
    GTEST_SKIP() << "File source name is different with compression";
  }
  auto source = GetFileSource("");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("<Buffer>"),
                                  format_->Inspect(*source).status());

  constexpr auto file_name = "herp/derp";
  ASSERT_OK_AND_ASSIGN(
      auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(file_name),
                                  format_->Inspect({file_name, fs}).status());
}

TEST_P(TestCsvFileFormat, Inspect) {
  auto source = GetFileSource(R"(f64
1.0

N/A
2)");
  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
  EXPECT_EQ(*actual, Schema({field("f64", float64())}));
}

TEST_P(TestCsvFileFormat, IsSupported) {
  bool supported;

  auto source = GetFileSource("");
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  ASSERT_EQ(supported, false);

  source = GetFileSource(R"(declare,two
  1,2,3)");
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  ASSERT_EQ(supported, false);

  source = GetFileSource(R"(f64
1.0

N/A
2)");
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  EXPECT_EQ(supported, true);
}

TEST_P(TestCsvFileFormat, NonProjectedFieldWithDifferingTypeFromInferred) {
  auto source = GetFileSource(R"(betrayal_not_really_f64,str
1.0,foo
,
N/A,bar
2,baz)");
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));
  ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(
      Schema({field("betrayal_not_really_f64", float64()), field("str", utf8())}),
      *physical_schema);

  // CSV is a text format, so it is valid to read column betrayal_not_really_f64 as string
  // rather than double
  auto not_float64 = utf8();
  auto dataset_schema =
      schema({field("betrayal_not_really_f64", not_float64), field("str", utf8())});

  ScannerBuilder builder(dataset_schema, fragment, opts_);

  // This filter is valid with declared schema, but would *not* be valid
  // if betrayal_not_really_f64 were read as double rather than string.
  ASSERT_OK(
      builder.Filter(equal(field_ref("betrayal_not_really_f64"), field_ref("str"))));

  // project only "str"
  ASSERT_OK(builder.Project({"str"}));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());

  ASSERT_OK_AND_ASSIGN(auto scan_task_it, scanner->Scan());
  for (auto maybe_scan_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto scan_task, maybe_scan_task);
    ASSERT_OK_AND_ASSIGN(auto batch_it, scan_task->Execute());
    for (auto maybe_batch : batch_it) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      // Run through the scan checking for errors to ensure that "f64" is read with the
      // specified type and does not revert to the inferred type (if it reverts to
      // inferring float64 then evaluation of the comparison expression should break)
    }
  }
}

INSTANTIATE_TEST_SUITE_P(TestUncompressedCsv, TestCsvFileFormat,
                         ::testing::Values(Compression::UNCOMPRESSED));
#ifdef ARROW_WITH_BZ2
INSTANTIATE_TEST_SUITE_P(TestBZ2Csv, TestCsvFileFormat,
                         ::testing::Values(Compression::BZ2));
#endif
#ifdef ARROW_WITH_LZ4
INSTANTIATE_TEST_SUITE_P(TestLZ4Csv, TestCsvFileFormat,
                         ::testing::Values(Compression::LZ4_FRAME));
#endif
// Snappy does not support streaming compression
#ifdef ARROW_WITH_ZLIB
INSTANTIATE_TEST_SUITE_P(TestGZipCsv, TestCsvFileFormat,
                         ::testing::Values(Compression::GZIP));
#endif
#ifdef ARROW_WITH_ZSTD
INSTANTIATE_TEST_SUITE_P(TestZSTDCsv, TestCsvFileFormat,
                         ::testing::Values(Compression::ZSTD));
#endif

}  // namespace dataset
}  // namespace arrow
