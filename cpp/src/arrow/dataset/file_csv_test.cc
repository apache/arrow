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
#include "arrow/dataset/filter.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"

namespace arrow {
namespace dataset {

class TestCsvFileFormat : public testing::Test {
 public:
  std::unique_ptr<FileSource> GetFileSource() {
    return GetFileSource(R"(f64
1.0

N/A
2)");
  }

  std::unique_ptr<FileSource> GetFileSource(std::string csv) {
    return internal::make_unique<FileSource>(Buffer::FromString(std::move(csv)));
  }

  RecordBatchIterator Batches(ScanTaskIterator scan_task_it) {
    return MakeFlattenIterator(MakeMaybeMapIterator(
        [](std::shared_ptr<ScanTask> scan_task) { return scan_task->Execute(); },
        std::move(scan_task_it)));
  }

  RecordBatchIterator Batches(Fragment* fragment) {
    EXPECT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_, ctx_));
    return Batches(std::move(scan_task_it));
  }

 protected:
  std::shared_ptr<CsvFileFormat> format_ = std::make_shared<CsvFileFormat>();
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<ScanContext> ctx_ = std::make_shared<ScanContext>();
  std::shared_ptr<Schema> schema_ = schema({field("f64", float64())});
};

TEST_F(TestCsvFileFormat, ScanRecordBatchReader) {
  auto source = GetFileSource();

  opts_ = ScanOptions::Make(schema_);
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  int64_t row_count = 0;

  for (auto maybe_batch : Batches(fragment.get())) {
    ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
    row_count += batch->num_rows();
  }

  ASSERT_EQ(row_count, 3);
}

TEST_F(TestCsvFileFormat, OpenFailureWithRelevantError) {
  auto source = GetFileSource("");
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("<Buffer>"),
                                  format_->Inspect(*source).status());

  constexpr auto file_name = "herp/derp";
  ASSERT_OK_AND_ASSIGN(
      auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr(file_name),
                                  format_->Inspect({file_name, fs}).status());
}

TEST_F(TestCsvFileFormat, Inspect) {
  auto source = GetFileSource();

  ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
  EXPECT_EQ(*actual, *schema_);
}

TEST_F(TestCsvFileFormat, IsSupported) {
  bool supported;

  auto source = GetFileSource("");
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  ASSERT_EQ(supported, false);

  source = GetFileSource(R"(declare,two
  1,2,3)");
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  ASSERT_EQ(supported, false);

  source = GetFileSource();
  ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
  EXPECT_EQ(supported, true);
}

TEST_F(TestCsvFileFormat, DISABLED_NonMaterializedFieldWithDifferingTypeFromInferred) {
  auto source = GetFileSource(R"(f64,str
1.0,foo
,
N/A,bar
2,baz)");
  ASSERT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(*source));

  // a valid schema for source:
  schema_ = schema({field("f64", utf8()), field("str", utf8())});
  ScannerBuilder builder(schema_, fragment, ctx_);
  // filter expression validated against declared schema
  ASSERT_OK(builder.Filter("f64"_ == "str"_));
  // project only "str"
  ASSERT_OK(builder.Project({"str"}));
  ASSERT_OK_AND_ASSIGN(auto scanner, builder.Finish());

  ASSERT_OK_AND_ASSIGN(auto scan_task_it, scanner->Scan());
  for (auto maybe_scan_task : scan_task_it) {
    ASSERT_OK_AND_ASSIGN(auto scan_task, std::move(maybe_scan_task));
    ASSERT_OK_AND_ASSIGN(auto batch_it, scan_task->Execute());
    for (auto maybe_batch : batch_it) {
      // ERROR: "f64" is not projected and reverts to inferred type,
      // breaking the comparison expression
      ASSERT_OK_AND_ASSIGN(auto batch, std::move(maybe_batch));
    }
  }
}

}  // namespace dataset
}  // namespace arrow
