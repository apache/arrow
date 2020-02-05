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
#include <ciso646>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/dataset/dataset_internal.h"
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
#include "arrow/util/make_unique.h"

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
  ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
  EXPECT_EQ(batch, nullptr);
}

class DatasetFixtureMixin : public ::testing::Test {
 public:
  DatasetFixtureMixin() : ctx_(std::make_shared<ScanContext>()) {}

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertScanTaskEquals(RecordBatchReader* expected, ScanTask* task,
                            bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, task->Execute());
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
  void AssertFragmentEquals(RecordBatchReader* expected, Fragment* fragment,
                            bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, fragment->Scan(ctx_));

    ARROW_EXPECT_OK(it.Visit([&](std::shared_ptr<ScanTask> task) -> Status {
      AssertScanTaskEquals(expected, task.get(), false);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragments of a source.
  void AssertSourceEquals(RecordBatchReader* expected, Source* source,
                          bool ensure_drained = true) {
    auto it = source->GetFragments(options_);

    ARROW_EXPECT_OK(it.Visit([&](std::shared_ptr<Fragment> fragment) -> Status {
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
    ASSERT_OK_AND_ASSIGN(auto it, scanner->Scan());

    ARROW_EXPECT_OK(it.Visit([&](std::shared_ptr<ScanTask> task) -> Status {
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
    ASSERT_OK_AND_ASSIGN(auto builder, dataset->NewScan());
    ASSERT_OK_AND_ASSIGN(auto scanner, builder->Finish());
    AssertScannerEquals(expected, scanner.get());

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

 protected:
  void SetSchema(std::vector<std::shared_ptr<Field>> fields) {
    schema_ = schema(std::move(fields));
    options_ = ScanOptions::Make(schema_);
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> ctx_;
};

/// \brief A dummy FileFormat implementation
class DummyFileFormat : public FileFormat {
 public:
  explicit DummyFileFormat(std::shared_ptr<Schema> schema = NULLPTR)
      : schema_(std::move(schema)) {}

  std::string type_name() const override { return "dummy"; }

  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override {
    return schema_;
  }

  /// \brief Open a file for scanning (always returns an empty iterator)
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override {
    return MakeEmptyIterator<std::shared_ptr<ScanTask>>();
  }

  inline Result<std::shared_ptr<Fragment>> MakeFragment(
      const FileSource& location, std::shared_ptr<ScanOptions> options) override;

 protected:
  std::shared_ptr<Schema> schema_;
};

class DummyFragment : public FileFragment {
 public:
  DummyFragment(const FileSource& source, std::shared_ptr<ScanOptions> options)
      : FileFragment(source, std::make_shared<DummyFileFormat>(), options) {}

  bool splittable() const override { return false; }
};

Result<std::shared_ptr<Fragment>> DummyFileFormat::MakeFragment(
    const FileSource& source, std::shared_ptr<ScanOptions> options) {
  return std::make_shared<DummyFragment>(source, options);
}

class JSONRecordBatchFileFormat : public FileFormat {
 public:
  using SchemaResolver = std::function<std::shared_ptr<Schema>(const FileSource&)>;

  explicit JSONRecordBatchFileFormat(std::shared_ptr<Schema> schema)
      : resolver_([schema](const FileSource&) { return schema; }) {}

  explicit JSONRecordBatchFileFormat(SchemaResolver resolver)
      : resolver_(std::move(resolver)) {}

  std::string type_name() const override { return "json_record_batch"; }

  /// \brief Return true if the given file extension
  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override {
    return resolver_(source);
  }

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(const FileSource& source,
                                    std::shared_ptr<ScanOptions> options,
                                    std::shared_ptr<ScanContext> context) const override {
    ARROW_ASSIGN_OR_RAISE(auto file, source.Open());
    ARROW_ASSIGN_OR_RAISE(int64_t size, file->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, file->Read(size));

    util::string_view view{*buffer};

    ARROW_ASSIGN_OR_RAISE(auto schema, Inspect(source));
    std::shared_ptr<RecordBatch> batch = RecordBatchFromJSON(schema, view);
    return ScanTaskIteratorFromRecordBatch({batch}, std::move(options),
                                           std::move(context));
  }

  inline Result<std::shared_ptr<Fragment>> MakeFragment(
      const FileSource& location, std::shared_ptr<ScanOptions> options) override;

 protected:
  SchemaResolver resolver_;
};

class JSONRecordBatchFragment : public FileFragment {
 public:
  JSONRecordBatchFragment(const FileSource& source, std::shared_ptr<Schema> schema,
                          std::shared_ptr<ScanOptions> options)
      : FileFragment(source, std::make_shared<JSONRecordBatchFileFormat>(schema),
                     options) {}

  bool splittable() const override { return false; }
};

Result<std::shared_ptr<Fragment>> JSONRecordBatchFileFormat::MakeFragment(
    const FileSource& source, std::shared_ptr<ScanOptions> options) {
  return std::make_shared<JSONRecordBatchFragment>(source, resolver_(source), options);
}

class TestFileSystemSource : public ::testing::Test {
 public:
  void MakeFileSystem(const std::vector<fs::FileStats>& stats) {
    ASSERT_OK_AND_ASSIGN(fs_, fs::internal::MockFileSystem::Make(fs::kNoTime, stats));
  }

  void MakeFileSystem(const std::vector<std::string>& paths) {
    std::vector<fs::FileStats> stats{paths.size()};
    std::transform(paths.cbegin(), paths.cend(), stats.begin(),
                   [](const std::string& p) { return fs::File(p); });

    ASSERT_OK_AND_ASSIGN(fs_, fs::internal::MockFileSystem::Make(fs::kNoTime, stats));
  }

  void MakeSource(const std::vector<fs::FileStats>& stats,
                  std::shared_ptr<Expression> source_partition = scalar(true),
                  ExpressionVector partitions = {}) {
    if (partitions.empty()) {
      partitions.resize(stats.size(), scalar(true));
    }

    MakeFileSystem(stats);
    auto format = std::make_shared<DummyFileFormat>();
    ASSERT_OK_AND_ASSIGN(source_, FileSystemSource::Make(schema({}), source_partition,
                                                         format, fs_, stats, partitions));
  }

 protected:
  std::shared_ptr<fs::FileSystem> fs_;
  std::shared_ptr<Source> source_;
  std::shared_ptr<ScanOptions> options_ = ScanOptions::Make(schema({}));
};

void AssertFragmentsAreFromPath(FragmentIterator it, std::vector<std::string> expected) {
  std::vector<std::string> actual;

  auto v = [&actual](std::shared_ptr<Fragment> fragment) -> Status {
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

  friend void PrintTo(const TestExpression& expr, std::ostream* os) {
    *os << expr.ToString();
  }
};

struct ArithmeticDatasetFixture {
  static std::shared_ptr<Schema> schema() {
    return ::arrow::schema({
        field("i64", int64()),
        // ARROW-1644: Parquet can't write complex level
        // field("struct", struct_({
        //                     // ARROW-2587: Parquet can't write struct with more
        //                     // than one field.
        //                     // field("i32", int32()),
        //                     field("str", utf8()),
        //                 })),
        field("u8", uint8()),
        field("list", list(int32())),
        field("bool", boolean()),
    });
  }

  /// \brief Creates a single JSON record templated with n as follow.
  ///
  /// {"i64": n, "struct": {"i32": n, "str": "n"}, "u8": n "list": [n,n], "bool": n %
  /// 2},
  static std::string JSONRecordFor(int64_t n) {
    std::stringstream ss;
    int32_t n_i32 = static_cast<int32_t>(n);

    ss << "{";
    ss << "\"i64\": " << n << ", ";
    // ss << "\"struct\": {";
    // {
    //   // ss << "\"i32\": " << n_i32 << ", ";
    //   ss << "\"str\": \"" << std::to_string(n) << "\"";
    // }
    // ss << "}, ";
    ss << "\"u8\": " << static_cast<int32_t>(n) << ", ";
    ss << "\"list\": [" << n_i32 << ", " << n_i32 << "], ";
    ss << "\"bool\": " << (static_cast<bool>(n % 2) ? "true" : "false");
    ss << "}";

    return ss.str();
  }

  /// \brief Creates a JSON RecordBatch
  static std::string JSONRecordBatch(int64_t n) {
    DCHECK_GT(n, 0);

    auto record = JSONRecordFor(n);

    std::stringstream ss;
    ss << "[\n";
    for (int64_t i = 0; i < n; i++) {
      if (i != 0) {
        ss << "\n,";
      }
      ss << record;
    }
    ss << "]\n";
    return ss.str();
  }

  static std::shared_ptr<RecordBatch> GetRecordBatch(int64_t n) {
    return RecordBatchFromJSON(ArithmeticDatasetFixture::schema(), JSONRecordBatch(n));
  }

  static std::unique_ptr<RecordBatchReader> GetRecordBatchReader(int64_t n) {
    DCHECK_GT(n, 0);

    // Functor which generates `n` RecordBatch
    struct {
      Status operator()(std::shared_ptr<RecordBatch>* out) {
        *out = i++ < count ? GetRecordBatch(i) : nullptr;
        return Status::OK();
      }
      int64_t i;
      int64_t count;
    } generator{0, n};

    return MakeGeneratedRecordBatch(schema(), std::move(generator));
  }
};

}  // namespace dataset
}  // namespace arrow
