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

#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/file_base.h"
#include "arrow/dataset/filter.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace dataset {

using fs::internal::GetAbstractPathExtension;
using internal::TemporaryDir;

class FileSourceFixtureMixin : public ::testing::Test {
 public:
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

void EnsureRecordBatchReaderDrained(RecordBatchReader* reader) {
  std::shared_ptr<RecordBatch> batch = nullptr;

  ARROW_EXPECT_OK(reader->Next(&batch));
  EXPECT_EQ(batch, nullptr);
}

class DatasetFixtureMixin : public ::testing::Test {
 public:
  DatasetFixtureMixin() : ctx_(std::make_shared<ScanContext>()) {}

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertScanTaskEquals(RecordBatchReader* expected, ScanTask* task,
                            bool ensure_drained = true) {
    auto it = task->Scan();
    ARROW_EXPECT_OK(it.Visit([expected](std::shared_ptr<RecordBatch> rhs) -> Status {
      std::shared_ptr<RecordBatch> lhs;
      RETURN_NOT_OK(expected->ReadNext(&lhs));
      EXPECT_NE(lhs, nullptr);
      AssertBatchesEqual(*lhs, *rhs);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertFragmentEquals(RecordBatchReader* expected, DataFragment* fragment,
                            bool ensure_drained = true) {
    ScanTaskIterator it;
    ARROW_EXPECT_OK(fragment->Scan(ctx_, &it));

    ARROW_EXPECT_OK(it.Visit([&](std::unique_ptr<ScanTask> task) -> Status {
      AssertScanTaskEquals(expected, task.get(), false);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragments of a source.
  void AssertDataSourceEquals(RecordBatchReader* expected, DataSource* source,
                              bool ensure_drained = true) {
    auto it = source->GetFragments(options_);

    ARROW_EXPECT_OK(it.Visit([&](std::shared_ptr<DataFragment> fragment) -> Status {
      AssertFragmentEquals(expected, fragment.get(), false);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by a scanner.
  void AssertScannerEquals(RecordBatchReader* expected, Scanner* scanner,
                           bool ensure_drained = true) {
    auto it = scanner->Scan();

    ARROW_EXPECT_OK(it.Visit([&](std::unique_ptr<ScanTask> task) -> Status {
      AssertScanTaskEquals(expected, task.get(), false);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by a dataset.
  void AssertDatasetEquals(RecordBatchReader* expected, Dataset* dataset,
                           bool ensure_drained = true) {
    std::unique_ptr<ScannerBuilder> builder;
    ASSERT_OK(dataset->NewScan(&builder));

    std::unique_ptr<Scanner> scanner;
    ASSERT_OK(builder->Finish(&scanner));

    AssertScannerEquals(expected, scanner.get());

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

 protected:
  std::shared_ptr<ScanOptions> options_ = ScanOptions::Defaults();
  std::shared_ptr<ScanContext> ctx_;
};

template <typename Format>
class FileSystemBasedDataSourceMixin : public FileSourceFixtureMixin {
 public:
  virtual std::vector<std::string> file_names() const = 0;

  fs::Selector selector_;
  std::unique_ptr<DataSource> source_;
  std::shared_ptr<fs::FileSystem> fs_;
  std::shared_ptr<FileFormat> format_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<ScanOptions> options_ = ScanOptions::Defaults();
};

/// \brief A dummy FileFormat implementation
class DummyFileFormat : public FileFormat {
 public:
  explicit DummyFileFormat(std::shared_ptr<Schema> schema = NULLPTR)
      : schema_(std::move(schema)) {}

  std::string name() const override { return "dummy"; }

  /// \brief Return true if the given file extension
  bool IsKnownExtension(const std::string& ext) const override { return ext == name(); }

  Status Inspect(const FileSource& source, std::shared_ptr<Schema>* out) const override {
    *out = schema_;
    return Status::OK();
  }

  /// \brief Open a file for scanning (always returns an empty iterator)
  Status ScanFile(const FileSource& source, std::shared_ptr<ScanOptions> scan_options,
                  std::shared_ptr<ScanContext> scan_context,
                  ScanTaskIterator* out) const override {
    *out = MakeEmptyIterator<std::unique_ptr<ScanTask>>();
    return Status::OK();
  }

  inline Status MakeFragment(const FileSource& location,
                             std::shared_ptr<ScanOptions> opts,
                             std::unique_ptr<DataFragment>* out) override;

 protected:
  std::shared_ptr<Schema> schema_;
};

class DummyFragment : public FileBasedDataFragment {
 public:
  DummyFragment(const FileSource& source, std::shared_ptr<ScanOptions> options)
      : FileBasedDataFragment(source, std::make_shared<DummyFileFormat>(), options) {}

  bool splittable() const override { return false; }
};

Status DummyFileFormat::MakeFragment(const FileSource& source,
                                     std::shared_ptr<ScanOptions> opts,
                                     std::unique_ptr<DataFragment>* out) {
  *out = internal::make_unique<DummyFragment>(source, opts);
  return Status::OK();
}

class JSONRecordBatchFileFormat : public FileFormat {
 public:
  explicit JSONRecordBatchFileFormat(std::shared_ptr<Schema> schema)
      : schema_(std::move(schema)) {}

  std::string name() const override { return "json_record_batch"; }

  /// \brief Return true if the given file extension
  bool IsKnownExtension(const std::string& ext) const override { return ext == name(); }

  Status Inspect(const FileSource& source, std::shared_ptr<Schema>* out) const override {
    *out = schema_;
    return Status::OK();
  }

  /// \brief Open a file for scanning (always returns an empty iterator)
  Status ScanFile(const FileSource& source, std::shared_ptr<ScanOptions> scan_options,
                  std::shared_ptr<ScanContext> scan_context,
                  ScanTaskIterator* out) const override {
    std::shared_ptr<io::RandomAccessFile> file;
    RETURN_NOT_OK(source.Open(&file));

    int64_t size;
    RETURN_NOT_OK(file->GetSize(&size));

    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(file->Read(size, &buffer));

    util::string_view view{*buffer};
    std::shared_ptr<RecordBatch> batch = RecordBatchFromJSON(schema_, view);
    return ScanTaskIteratorFromRecordBatch({batch}, out);
  }

  inline Status MakeFragment(const FileSource& location,
                             std::shared_ptr<ScanOptions> opts,
                             std::unique_ptr<DataFragment>* out) override;

 protected:
  std::shared_ptr<Schema> schema_;
};

class JSONRecordBatchFragment : public FileBasedDataFragment {
 public:
  JSONRecordBatchFragment(const FileSource& source, std::shared_ptr<Schema> schema,
                          std::shared_ptr<ScanOptions> options)
      : FileBasedDataFragment(source, std::make_shared<JSONRecordBatchFileFormat>(schema),
                              options) {}

  bool splittable() const override { return false; }
};

Status JSONRecordBatchFileFormat::MakeFragment(const FileSource& source,
                                               std::shared_ptr<ScanOptions> opts,
                                               std::unique_ptr<DataFragment>* out) {
  *out = internal::make_unique<JSONRecordBatchFragment>(source, schema_, opts);
  return Status::OK();
}

class TestFileSystemBasedDataSource : public ::testing::Test {
 public:
  void MakeFileSystem(const std::vector<fs::FileStats>& stats) {
    ASSERT_OK(fs::internal::MockFileSystem::Make(fs::kNoTime, stats, &fs_));
  }

  void MakeFileSystem(const std::vector<std::string>& paths) {
    std::vector<fs::FileStats> stats{paths.size()};
    std::transform(paths.cbegin(), paths.cend(), stats.begin(),
                   [](const std::string& p) { return fs::File(p); });

    ASSERT_OK(fs::internal::MockFileSystem::Make(fs::kNoTime, stats, &fs_));
  }

  void MakeSource(const std::vector<fs::FileStats>& stats,
                  std::shared_ptr<Expression> source_partition = nullptr,
                  PathPartitions partitions = {}) {
    MakeFileSystem(stats);
    auto format = std::make_shared<DummyFileFormat>();
    ASSERT_OK(FileSystemBasedDataSource::Make(fs_.get(), stats, source_partition,
                                              partitions, format, &source_));
  }

 protected:
  std::shared_ptr<fs::FileSystem> fs_;
  std::shared_ptr<DataSource> source_;
  std::shared_ptr<ScanOptions> options_ = ScanOptions::Defaults();
};

void AssertFragmentsAreFromPath(DataFragmentIterator it,
                                std::vector<std::string> expected) {
  std::vector<std::string> actual;

  auto v = [&actual](std::shared_ptr<DataFragment> fragment) -> Status {
    EXPECT_NE(fragment, nullptr);
    auto dummy = std::static_pointer_cast<DummyFragment>(fragment);
    actual.push_back(dummy->source().path());
    return Status::OK();
  };

  ASSERT_OK(it.Visit(v));
  // Ordering is not guaranteed.
  EXPECT_THAT(actual, testing::UnorderedElementsAreArray(expected));
}

// A frozen shared_ptr<Expression> with behavior expected by GTest
struct TestExpression : util::EqualityComparable<TestExpression>,
                        util::ToStringOstreamable<TestExpression> {
  // NOLINTNEXTLINE runtime/explicit
  TestExpression(std::shared_ptr<Expression> e) : expression(std::move(e)) {}

  // NOLINTNEXTLINE runtime/explicit
  TestExpression(const Expression& e) : expression(e.Copy()) {}

  std::shared_ptr<Expression> expression;

  bool Equals(const TestExpression& other) const {
    return expression->Equals(other.expression);
  }

  std::string ToString() const { return expression->ToString(); }
};

}  // namespace dataset
}  // namespace arrow
