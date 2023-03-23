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

#include "benchmark/benchmark.h"

#include "arrow/compute/expression.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

static std::shared_ptr<Dataset> GetDataset() {
  std::vector<fs::FileInfo> files;
  std::vector<std::string> paths;
  for (int a = 0; a < 100; a++) {
    for (int b = 0; b < 100; b++) {
      auto path = "a=" + std::to_string(a) + "/b=" + std::to_string(b) + "/data.feather";
      files.push_back(fs::File(path));
      paths.push_back(path);
    }
  }
  EXPECT_OK_AND_ASSIGN(auto fs,
                       arrow::fs::internal::MockFileSystem::Make(fs::kNoTime, files));
  auto format = std::make_shared<IpcFileFormat>();
  FileSystemFactoryOptions options;
  options.partitioning = HivePartitioning::MakeFactory();
  EXPECT_OK_AND_ASSIGN(auto factory,
                       FileSystemDatasetFactory::Make(fs, paths, format, options));
  FinishOptions finish_options;
  finish_options.inspect_options.fragments = 0;
  EXPECT_OK_AND_ASSIGN(auto dataset, factory->Finish(finish_options));
  return dataset;
}

// A benchmark of filtering fragments in a dataset.
static void GetAllFragments(benchmark::State& state) {
  auto dataset = GetDataset();
  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto fragments, dataset->GetFragments());
    ABORT_NOT_OK(fragments.Visit([](std::shared_ptr<Fragment>) { return Status::OK(); }));
  }
}

static void GetFilteredFragments(benchmark::State& state, compute::Expression filter) {
  auto dataset = GetDataset();
  ASSERT_OK_AND_ASSIGN(filter, filter.Bind(*dataset->schema()));
  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto fragments, dataset->GetFragments(filter));
    ABORT_NOT_OK(fragments.Visit([](std::shared_ptr<Fragment>) { return Status::OK(); }));
  }
}

using compute::field_ref;
using compute::literal;

BENCHMARK(GetAllFragments);
// Drill down to a subtree.
BENCHMARK_CAPTURE(GetFilteredFragments, single_dir, equal(field_ref("a"), literal(90)));
// Drill down, but not to a subtree.
BENCHMARK_CAPTURE(GetFilteredFragments, multi_dir, equal(field_ref("b"), literal(90)));
// Drill down to a single file.
BENCHMARK_CAPTURE(GetFilteredFragments, single_file,
                  and_(equal(field_ref("a"), literal(90)),
                       equal(field_ref("b"), literal(90))));
// Apply a filter, but keep most of the files.
BENCHMARK_CAPTURE(GetFilteredFragments, range, greater(field_ref("a"), literal(1)));

}  // namespace dataset
}  // namespace arrow
