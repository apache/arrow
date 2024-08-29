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

#include "arrow/acero/exec_plan.h"
#include "arrow/csv/writer.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/io/compressed.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace dataset {

class CsvFormatHelper {
 public:
  using FormatType = CsvFileFormat;
  static Result<std::shared_ptr<Buffer>> Write(RecordBatchReader* reader) {
    ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    auto options = csv::WriteOptions::Defaults();
    RETURN_NOT_OK(csv::WriteCSV(*table, options, sink.get()));
    return sink->Finish();
  }

  static std::shared_ptr<CsvFileFormat> MakeFormat() {
    auto format = std::make_shared<CsvFileFormat>();
    // Required for CountRows (since the test generates data with nulls that get written
    // as empty lines)
    format->parse_options.ignore_empty_lines = false;
    return format;
  }
};

struct CsvFileFormatParams {
  Compression::type compression_type;
  bool use_new_scan_v2;
};

class TestCsvFileFormat : public FileFormatFixtureMixin<CsvFormatHelper>,
                          public ::testing::WithParamInterface<CsvFileFormatParams> {
 public:
  bool UseScanV2() { return GetParam().use_new_scan_v2; }
  Compression::type GetCompression() { return GetParam().compression_type; }

  void SetUp() {
    internal::Initialize();
    auto fragment_scan_options = std::make_shared<CsvFragmentScanOptions>();
    fragment_scan_options->parse_options.ignore_empty_lines = false;
    opts_->fragment_scan_options = fragment_scan_options;
  }

  std::unique_ptr<FileSource> GetFileSource(std::string csv) {
    if (GetCompression() == Compression::UNCOMPRESSED) {
      return std::make_unique<FileSource>(Buffer::FromString(std::move(csv)));
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
    return std::make_unique<FileSource>(info, fs, GetCompression());
  }

  CsvFragmentScanOptions MakeDefaultFormatOptions() {
    CsvFragmentScanOptions scan_opts;
    scan_opts.parse_options.ignore_empty_lines = false;
    return scan_opts;
  }

  ScanV2Options MigrateLegacyOptions(std::shared_ptr<Fragment> fragment) {
    std::shared_ptr<Dataset> dataset = std::make_shared<FragmentDataset>(
        opts_->dataset_schema, FragmentVector{std::move(fragment)});
    ScanV2Options updated(std::move(dataset));
    updated.format_options = opts_->fragment_scan_options.get();
    updated.filter = opts_->filter;
    for (const auto& field : opts_->projected_schema->fields()) {
      auto field_name = field->name();
      EXPECT_OK_AND_ASSIGN(FieldPath field_path,
                           FieldRef(field_name).FindOne(*opts_->dataset_schema));
      updated.columns.push_back(field_path);
    }
    return updated;
  }

  RecordBatchIterator Batches(const std::shared_ptr<Fragment>& fragment) {
    if (UseScanV2()) {
      ScanV2Options v2_options = MigrateLegacyOptions(fragment);
      EXPECT_TRUE(ScanV2Options::AddFieldsNeededForFilter(&v2_options).ok());
      EXPECT_OK_AND_ASSIGN(
          std::unique_ptr<RecordBatchReader> reader,
          acero::DeclarationToReader(acero::Declaration("scan2", std::move(v2_options)),
                                     /*use_threads=*/false));
      struct ReaderIterator {
        Result<std::shared_ptr<RecordBatch>> Next() { return reader->Next(); }
        std::unique_ptr<RecordBatchReader> reader;
      };
      return RecordBatchIterator(ReaderIterator{std::move(reader)});
    } else {
      EXPECT_OK_AND_ASSIGN(auto batch_gen, fragment->ScanBatchesAsync(opts_));
      return MakeGeneratorIterator(batch_gen);
    }
  }
};

TEST_P(TestCsvFileFormat, BOMQuoteInHeader) {
  // ARROW-17382: quoted headers after a BOM should be parsed correctly
  auto source = GetFileSource("\xef\xbb\xbf\"ab\",\"cd\"\nef,gh\nij,kl\n");
  auto fields = {field("ab", utf8()), field("cd", utf8())};
  SetSchema(fields);
  auto fragment = MakeFragment(*source);

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment)) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    AssertSchemaEqual(batch->schema(), schema(fields));
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, 2);
}

// Basic scanning tests (to exercise compression support); see the parameterized test
// below for more comprehensive testing of scan behaviors
TEST_P(TestCsvFileFormat, ScanRecordBatchReader) {
  auto source = GetFileSource(R"(f64
1.0

N/A
2)");
  SetSchema({field("f64", float64())});
  auto fragment = MakeFragment(*source);

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment)) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, 4);
}

TEST_P(TestCsvFileFormat, CustomConvertOptions) {
  auto source = GetFileSource(R"(str
foo
MYNULL
N/A
bar)");
  SetSchema({field("str", utf8())});
  auto fragment = MakeFragment(*source);
  auto fragment_scan_options =
      static_cast<CsvFragmentScanOptions*>(opts_->fragment_scan_options.get());
  fragment_scan_options->convert_options.null_values = {"MYNULL"};
  fragment_scan_options->convert_options.strings_can_be_null = true;

  int64_t null_count = 0;
  for (auto maybe_batch : Batches(fragment)) {
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
  {
    SetSchema({field("str", utf8())});
    auto defaults = std::make_shared<CsvFragmentScanOptions>();
    defaults->read_options.skip_rows = 1;
    format_->default_fragment_scan_options = defaults;
    opts_->fragment_scan_options = nullptr;
    auto fragment = MakeFragment(*source);
    ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
    AssertSchemaEqual(opts_->dataset_schema, physical_schema);

    int64_t rows = 0;
    for (auto maybe_batch : Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      rows += batch->GetColumnByName("str")->length();
    }
    ASSERT_EQ(rows, 4);
  }
  {
    SetSchema({field("header_skipped", utf8())});
    // These options completely override the default ones
    opts_->fragment_scan_options = std::make_shared<CsvFragmentScanOptions>();
    auto fragment_scan_options =
        static_cast<CsvFragmentScanOptions*>(opts_->fragment_scan_options.get());
    fragment_scan_options->read_options.block_size = 1 << 22;
    int64_t rows = 0;
    auto fragment = MakeFragment(*source);
    for (auto maybe_batch : Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      rows += batch->GetColumnByName("header_skipped")->length();
    }
    ASSERT_EQ(rows, 5);
  }
  {
    SetSchema({field("custom_header", utf8())});
    auto defaults = std::make_shared<CsvFragmentScanOptions>();
    defaults->read_options.column_names = {"custom_header"};
    format_->default_fragment_scan_options = defaults;
    opts_->fragment_scan_options = nullptr;
    int64_t rows = 0;
    auto fragment = MakeFragment(*source);
    for (auto maybe_batch : Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      rows += batch->GetColumnByName("custom_header")->length();
    }
    ASSERT_EQ(rows, 6);
  }
}

TEST_P(TestCsvFileFormat, CustomReadOptionsColumnNames) {
  auto source = GetFileSource("1,1\n2,3");
  SetSchema({field("ints_1", int64()), field("ints_2", int64())});
  auto defaults = std::make_shared<CsvFragmentScanOptions>();
  defaults->read_options.column_names = {"ints_1", "ints_2"};
  format_->default_fragment_scan_options = defaults;
  opts_->fragment_scan_options = nullptr;
  auto fragment = MakeFragment(*source);
  ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(opts_->dataset_schema, physical_schema);
  int64_t rows = 0;
  for (auto maybe_batch : Batches(fragment)) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    rows += batch->num_rows();
  }
  ASSERT_EQ(rows, 2);

  defaults->read_options.column_names = {"same", "same"};
  format_->default_fragment_scan_options = defaults;
  fragment = MakeFragment(*source);
  SetSchema({field("same", int64())});
  if (UseScanV2()) {
    // V2 scan method's basic evolution strategy builds ds_to_frag_map and just finds
    // the first instance of a matching column and doesn't check further to see if
    // there are duplicates.  So in this case it would grab the first column.
    //
    // Not clear if this is a good thing or not.
    rows = 0;
    for (auto maybe_batch : Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      rows += batch->num_rows();
    }
    ASSERT_EQ(rows, 2);
  } else {
    // Legacy scan method does not support CSV columns with duplicate names
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("CSV file contained multiple columns named same"),
        Batches(fragment).Next());
  }
}

TEST_P(TestCsvFileFormat, ScanRecordBatchReaderWithVirtualColumn) {
  auto source = GetFileSource(R"(f64
1.0

N/A
2)");
  // NB: dataset_schema includes a column not present in the file
  SetSchema({field("f64", float64()), field("virtual", int32())});
  auto fragment = MakeFragment(*source);

  ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
  AssertSchemaEqual(Schema({field("f64", float64())}), *physical_schema);

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment)) {
    ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
    if (UseScanV2()) {
      // In the new scan, evolution happens and inserts a null column in place of the
      // virtual column
      AssertSchemaEqual(*batch->schema(), *opts_->dataset_schema);
    } else {
      AssertSchemaEqual(*batch->schema(), *physical_schema);
    }
    row_count += batch->num_rows();
  }
  ASSERT_EQ(row_count, 4);
}

TEST_P(TestCsvFileFormat, InspectFailureWithRelevantError) {
  TestInspectFailureWithRelevantError(StatusCode::Invalid, "CSV");
}

TEST_P(TestCsvFileFormat, Inspect) {
  TestInspect();
  auto source = GetFileSource(R"(f64
1.0

N/A
2)");
  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
  EXPECT_EQ(*actual, Schema({field("f64", float64())}));
}

TEST_P(TestCsvFileFormat, InspectWithCustomConvertOptions) {
  // Regression test for ARROW-12083
  auto source = GetFileSource(R"(actually_string
1.0

N/A
2)");
  auto defaults = std::make_shared<CsvFragmentScanOptions>();
  format_->default_fragment_scan_options = defaults;

  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
  // Default type inferred
  EXPECT_EQ(*actual, Schema({field("actually_string", float64())}));

  // Override the inferred type
  defaults->convert_options.column_types["actually_string"] = utf8();
  ASSERT_OK_AND_ASSIGN(actual, format_->Inspect(*source.get()));
  EXPECT_EQ(*actual, Schema({field("actually_string", utf8())}));
}

TEST_P(TestCsvFileFormat, IsSupported) {
  TestIsSupported();
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
  auto fragment = MakeFragment(*source);
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

  ASSERT_OK_AND_ASSIGN(auto batch_it, scanner->ScanBatches());
  // Run through the scan checking for errors to ensure that "f64" is read with the
  // specified type and does not revert to the inferred type (if it reverts to
  // inferring float64 then evaluation of the comparison expression should break)
  ASSERT_OK(batch_it.Visit([](TaggedRecordBatch) { return Status::OK(); }));
}

TEST_P(TestCsvFileFormat, WriteRecordBatchReader) { TestWrite(); }

TEST_P(TestCsvFileFormat, WriteRecordBatchReaderCustomOptions) {
  auto options =
      checked_pointer_cast<CsvFileWriteOptions>(format_->DefaultWriteOptions());
  options->write_options->include_header = false;
  auto data_schema = schema({field("f64", float64())});
  ASSERT_OK_AND_ASSIGN(auto sink, GetFileSink());
  ASSERT_OK_AND_ASSIGN(auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {}));
  ASSERT_OK_AND_ASSIGN(auto writer,
                       format_->MakeWriter(sink, data_schema, options, {fs, "<buffer>"}));
  ASSERT_OK(writer->Write(ConstantArrayGenerator::Zeroes(5, data_schema)));
  ASSERT_FINISHES_OK(writer->Finish());
  ASSERT_OK_AND_ASSIGN(auto written, sink->Finish());
  ASSERT_EQ("0\n0\n0\n0\n0\n", written->ToString());
}

TEST_P(TestCsvFileFormat, CountRows) { TestCountRows(); }

TEST_P(TestCsvFileFormat, FragmentEquals) { TestFragmentEquals(); }

INSTANTIATE_TEST_SUITE_P(TestUncompressedCsv, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::UNCOMPRESSED,
                                                               false}));
INSTANTIATE_TEST_SUITE_P(TestUncompressedCsvV2, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::UNCOMPRESSED,
                                                               true}));

// Writing even small CSV files takes a lot of time in valgrind.  The various compression
// codecs should be independently tested and so we do not need to cover those with
// valgrind here.
#ifndef ARROW_VALGRIND
#ifdef ARROW_WITH_BZ2
INSTANTIATE_TEST_SUITE_P(TestBZ2Csv, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::BZ2, false}));
INSTANTIATE_TEST_SUITE_P(TestBZ2CsvV2, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::BZ2, true}));
#endif
#ifdef ARROW_WITH_LZ4
INSTANTIATE_TEST_SUITE_P(TestLZ4Csv, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::LZ4_FRAME,
                                                               false}));
INSTANTIATE_TEST_SUITE_P(TestLZ4CsvV2, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::LZ4_FRAME,
                                                               true}));
#endif
// Snappy does not support streaming compression
#ifdef ARROW_WITH_ZLIB
INSTANTIATE_TEST_SUITE_P(TestGzipCsv, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::GZIP,
                                                               false}));
INSTANTIATE_TEST_SUITE_P(TestGzipCsvV2, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::GZIP, true}));
#endif
#ifdef ARROW_WITH_ZSTD
INSTANTIATE_TEST_SUITE_P(TestZSTDCsv, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::ZSTD,
                                                               false}));
INSTANTIATE_TEST_SUITE_P(TestZSTDCsvV2, TestCsvFileFormat,
                         ::testing::Values(CsvFileFormatParams{Compression::ZSTD, true}));
#endif
#endif  // ARROW_VALGRIND

class TestCsvFileFormatScan : public FileFormatScanMixin<CsvFormatHelper> {};

TEST_P(TestCsvFileFormatScan, ScanRecordBatchReader) { TestScan(); }
TEST_P(TestCsvFileFormatScan, ScanBatchSize) { TestScanBatchSize(); }
TEST_P(TestCsvFileFormatScan, ScanNoReadhead) { TestScanNoReadahead(); }
TEST_P(TestCsvFileFormatScan, ScanRecordBatchReaderProjected) { TestScanProjected(); }
// NOTE(ARROW-14658): TestScanProjectedNested is ignored since CSV
// doesn't have any nested types for us to work with
TEST_P(TestCsvFileFormatScan, ScanRecordBatchReaderProjectedMissingCols) {
  TestScanProjectedMissingCols();
}
TEST_P(TestCsvFileFormatScan, ScanRecordBatchReaderWithVirtualColumn) {
  TestScanWithVirtualColumn();
}
// The CSV reader rejects duplicate columns, so skip
// ScanRecordBatchReaderWithDuplicateColumn
TEST_P(TestCsvFileFormatScan, ScanRecordBatchReaderWithDuplicateColumnError) {
  TestScanWithDuplicateColumnError();
}
TEST_P(TestCsvFileFormatScan, ScanWithPushdownNulls) { TestScanWithPushdownNulls(); }

INSTANTIATE_TEST_SUITE_P(TestScan, TestCsvFileFormatScan,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

class TestCsvFileFormatScanNode : public FileFormatScanNodeMixin<CsvFormatHelper> {
  void SetUp() override {
    internal::Initialize();
    scan_options_.parse_options.ignore_empty_lines = false;
  }

  const FragmentScanOptions* GetFormatOptions() override { return &scan_options_; }

 protected:
  CsvFragmentScanOptions scan_options_;
};

TEST_P(TestCsvFileFormatScanNode, Scan) { TestScan(); }
TEST_P(TestCsvFileFormatScanNode, ScanProjected) { TestScanProjected(); }
TEST_P(TestCsvFileFormatScanNode, ScanMissingFilterField) {
  TestScanMissingFilterField();
}
// NOTE(ARROW-14658): TestScanProjectedNested is ignored since CSV
// doesn't have any nested types for us to work with
TEST_P(TestCsvFileFormatScanNode, ScanProjectedMissingColumns) {
  TestScanProjectedMissingCols();
}
TEST_P(TestCsvFileFormatScanNode, ScanWithDuplicateColumn) {
  TestScanWithDuplicateColumn();
}
// NOTE: TestScanWithPushdownNulls is ignored since CSV doesn't handle pushdown filtering
INSTANTIATE_TEST_SUITE_P(TestScanNode, TestCsvFileFormatScanNode,
                         ::testing::ValuesIn(TestFormatParams::Values()),
                         TestFormatParams::ToTestNameString);

}  // namespace dataset
}  // namespace arrow
