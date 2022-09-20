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
#include <string_view>

#include "benchmark/benchmark.h"

#include "arrow/filesystem/localfs.h"
#include "arrow/io/file.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/io_util.h"
#include "arrow/util/make_unique.h"

namespace arrow {

namespace fs {

using arrow::internal::make_unique;
using arrow::internal::TemporaryDir;

/// Set up hierarchical directory structure to test asynchronous
/// file discovery interface (GetFileInfoGenerator()) in the LocalFileSystem
/// class.
///
/// The main routine of the class is `InitializeDatasetStructure()`, which
/// does the following:
/// 1. Create `num_files_` empty files under specified root directory.
/// 2. Create `num_dirs_` additional sub-directories in the current dir.
/// 3. Check if the specified recursion limit is reached (controlled by `nesting_depth_`).
///   a. Return if recursion limit reached.
///   b. Recurse into each sub-directory and perform steps above, increasing current
///      nesting level.
class LocalFSFixture : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State& state) override {
    ASSERT_OK_AND_ASSIGN(tmp_dir_, TemporaryDir::Make("localfs-test-"));

    auto options = LocalFileSystemOptions::Defaults();
    fs_ = make_unique<LocalFileSystem>(options);

    InitializeDatasetStructure(0, tmp_dir_->path());
  }

  void InitializeDatasetStructure(size_t cur_nesting_level,
                                  arrow::internal::PlatformFilename cur_root_dir) {
    ASSERT_OK(arrow::internal::CreateDir(cur_root_dir));

    for (size_t i = 0; i < num_files_; ++i) {
      ASSERT_OK_AND_ASSIGN(auto path,
                           cur_root_dir.Join(std::string{"file_" + std::to_string(i)}));
      ASSERT_OK(MakeEmptyFile(path.ToString()));
    }

    if (cur_nesting_level == nesting_depth_) {
      return;
    }

    for (size_t i = 0; i < num_dirs_; ++i) {
      ASSERT_OK_AND_ASSIGN(auto path,
                           cur_root_dir.Join(std::string{"dir_" + std::to_string(i)}));
      InitializeDatasetStructure(cur_nesting_level + 1, std::move(path));
    }
  }

  Status MakeEmptyFile(const std::string& path) {
    return io::FileOutputStream::Open(path).status();
  }

 protected:
  std::unique_ptr<TemporaryDir> tmp_dir_;
  std::unique_ptr<LocalFileSystem> fs_;

  const size_t nesting_depth_ = 2;
  const size_t num_dirs_ = 10;
  const size_t num_files_ = 1000;
};

/// Benchmark for `LocalFileSystem::GetFileInfoGenerator()` performance.
///
/// The test function is executed for each combination (cartesian product)
/// of input arguments tuple (directory_readahead, file_info_batch_size)
/// to test both internal parallelism and batching.
BENCHMARK_DEFINE_F(LocalFSFixture, AsyncFileDiscovery)
(benchmark::State& st) {
  size_t total_file_count = 0;

  for (auto _ : st) {
    // Instantiate LocalFileSystem with custom options for directory readahead
    // and file info batch size.
    auto options = LocalFileSystemOptions::Defaults();
    options.directory_readahead = static_cast<int32_t>(st.range(0));
    options.file_info_batch_size = static_cast<int32_t>(st.range(1));
    auto test_fs = make_unique<LocalFileSystem>(options);
    // Create recursive FileSelector pointing to the root of the temporary
    // directory, which was set up by the fixture earlier.
    FileSelector select;
    select.base_dir = tmp_dir_->path().ToString();
    select.recursive = true;
    auto file_gen = test_fs->GetFileInfoGenerator(std::move(select));
    // Trigger fetching from the generator and count all received FileInfo:s.
    auto visit_fut =
        VisitAsyncGenerator(file_gen, [&total_file_count](const FileInfoVector& fv) {
          total_file_count += fv.size();
          return Status::OK();
        });
    ASSERT_FINISHES_OK(visit_fut);
  }
  st.SetItemsProcessed(total_file_count);
}
BENCHMARK_REGISTER_F(LocalFSFixture, AsyncFileDiscovery)
    ->ArgNames({"directory_readahead", "file_info_batch_size"})
    ->ArgsProduct({{1, 4, 16}, {100, 1000}})
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

}  // namespace fs

}  // namespace arrow
