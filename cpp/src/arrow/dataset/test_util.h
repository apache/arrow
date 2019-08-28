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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
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

class DatasetFixtureMixin : public ::testing::Test {
 public:
  DatasetFixtureMixin() : ctx_(std::make_shared<ScanContext>()) {}

 protected:
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> ctx_;
};

class TestDataFragmentMixin : public DatasetFixtureMixin {
 public:
  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertFragmentEquals(RecordBatchReader* expected, DataFragment* fragment) {
    std::unique_ptr<ScanTaskIterator> it;
    ARROW_EXPECT_OK(fragment->Scan(ctx_, &it));

    ARROW_EXPECT_OK(it->Visit([expected](std::unique_ptr<ScanTask> task) -> Status {
      auto batch_it = task->Scan();
      return batch_it->Visit([expected](std::shared_ptr<RecordBatch> rhs) -> Status {
        std::shared_ptr<RecordBatch> lhs;
        RETURN_NOT_OK(expected->ReadNext(&lhs));
        EXPECT_NE(lhs, nullptr);
        AssertBatchesEqual(*lhs, *rhs);
        return Status::OK();
      });
    }));
  }
};

class TestDataSourceMixin : public TestDataFragmentMixin {
 public:
  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragments of a source.
  void AssertDataSourceEquals(RecordBatchReader* expected, DataSource* source) {
    auto it = source->GetFragments(options_);

    ARROW_EXPECT_OK(it->Visit([&](std::shared_ptr<DataFragment> fragment) -> Status {
      AssertFragmentEquals(expected, fragment.get());
      return Status::OK();
    }));
  }
};

template <typename Format>
class FileSystemBasedDataSourceMixin : public FileSourceFixtureMixin {
 public:
  virtual std::vector<std::string> file_names() const = 0;

  void SetUp() override {
    format_ = std::make_shared<Format>();

    ASSERT_OK(
        TemporaryDir::Make("test-fsdatasource-" + format_->name() + "-", &temp_dir_));
    local_fs_ = std::make_shared<fs::LocalFileSystem>();

    auto path = temp_dir_->path().ToString();
    fs_ = std::make_shared<fs::SubTreeFileSystem>(path, local_fs_);

    for (auto path : file_names()) {
      CreateFile(path, "");
    }
  }

  void CreateFile(std::string path, std::string contents) {
    auto parent = fs::internal::GetAbstractPathParent(path).first;
    if (parent != "") {
      ASSERT_OK(this->fs_->CreateDir(parent, true));
    }
    std::shared_ptr<io::OutputStream> file;
    ASSERT_OK(this->fs_->OpenOutputStream(path, &file));
    ASSERT_OK(file->Write(contents));
  }

  void MakeDataSource() {
    ASSERT_OK(FileSystemBasedDataSource::Make(fs_.get(), selector_, format_,
                                              std::make_shared<ScanOptions>(), &source_));
  }

 protected:
  void NonRecursive() {
    selector_.base_dir = "/";
    MakeDataSource();

    int count = 0;
    ASSERT_OK(
        source_->GetFragments({})->Visit([&](std::shared_ptr<DataFragment> fragment) {
          auto file_fragment =
              internal::checked_pointer_cast<FileBasedDataFragment>(fragment);
          ++count;
          auto extension =
              fs::internal::GetAbstractPathExtension(file_fragment->source().path());
          EXPECT_TRUE(format_->IsKnownExtension(extension));
          std::shared_ptr<io::RandomAccessFile> f;
          return this->fs_->OpenInputFile(file_fragment->source().path(), &f);
        }));

    ASSERT_EQ(count, 1);
  }

  void Recursive() {
    selector_.base_dir = "/";
    selector_.recursive = true;
    MakeDataSource();

    int count = 0;
    ASSERT_OK(
        source_->GetFragments({})->Visit([&](std::shared_ptr<DataFragment> fragment) {
          auto file_fragment =
              internal::checked_pointer_cast<FileBasedDataFragment>(fragment);
          ++count;
          auto extension =
              fs::internal::GetAbstractPathExtension(file_fragment->source().path());
          EXPECT_TRUE(format_->IsKnownExtension(extension));
          std::shared_ptr<io::RandomAccessFile> f;
          return this->fs_->OpenInputFile(file_fragment->source().path(), &f);
        }));

    ASSERT_EQ(count, 4);
  }

  void DeletedFile() {
    selector_.base_dir = "/";
    selector_.recursive = true;
    MakeDataSource();
    ASSERT_GT(file_names().size(), 0);
    ASSERT_OK(this->fs_->DeleteFile(file_names()[0]));

    ASSERT_RAISES(
        IOError,
        source_->GetFragments({})->Visit([&](std::shared_ptr<DataFragment> fragment) {
          auto file_fragment =
              internal::checked_pointer_cast<FileBasedDataFragment>(fragment);
          auto extension =
              fs::internal::GetAbstractPathExtension(file_fragment->source().path());
          EXPECT_TRUE(format_->IsKnownExtension(extension));
          std::shared_ptr<io::RandomAccessFile> f;
          return this->fs_->OpenInputFile(file_fragment->source().path(), &f);
        }));
  }

  fs::Selector selector_;
  std::unique_ptr<FileSystemBasedDataSource> source_;
  std::shared_ptr<fs::LocalFileSystem> local_fs_;
  std::shared_ptr<fs::FileSystem> fs_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::shared_ptr<FileFormat> format_;
};

template <typename Gen>
std::unique_ptr<GeneratedRecordBatch<Gen>> MakeGeneratedRecordBatch(
    std::shared_ptr<Schema> schema, Gen&& gen) {
  return internal::make_unique<GeneratedRecordBatch<Gen>>(schema, std::forward<Gen>(gen));
}

/// \brief A dummy FileFormat implementation
class DummyFileFormat : public FileFormat {
 public:
  std::string name() const override { return "dummy"; }

  /// \brief Return true if the given file extension
  bool IsKnownExtension(const std::string& ext) const override { return ext == name(); }

  /// \brief Open a file for scanning (always returns an empty iterator)
  Status ScanFile(const FileSource& source, std::shared_ptr<ScanOptions> scan_options,
                  std::shared_ptr<ScanContext> scan_context,
                  std::unique_ptr<ScanTaskIterator>* out) const override {
    *out = internal::make_unique<EmptyIterator<std::unique_ptr<ScanTask>>>();
    return Status::OK();
  }

  inline Status MakeFragment(const FileSource& location,
                             std::shared_ptr<ScanOptions> opts,
                             std::unique_ptr<DataFragment>* out) override;
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

}  // namespace dataset
}  // namespace arrow
