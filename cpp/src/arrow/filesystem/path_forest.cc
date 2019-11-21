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
//

#include "arrow/filesystem/path_forest.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

inline bool IsDescendantOf(const FileStats& anscestor, const FileStats& descendant) {
  if (!anscestor.IsDirectory()) {
    // only directories may have descendants
    return false;
  }

  auto anscestor_path = internal::RemoveTrailingSlash(anscestor.path());
  if (anscestor_path == "") {
    // everything is a descendant of the root directory
    return true;
  }

  auto descendant_path = internal::RemoveTrailingSlash(descendant.path());
  if (!descendant_path.starts_with(anscestor_path)) {
    // an anscestor path is a prefix of descendant paths
    return false;
  }

  descendant_path.remove_prefix(anscestor_path.size());

  // "/hello/w" is not an anscestor of "/hello/world"
  return descendant_path.starts_with(std::string{internal::kSep});
}

Result<PathForest> PathForest::Make(std::vector<FileStats> stats) {
  std::stable_sort(
      stats.begin(), stats.end(),
      [](const FileStats& lhs, const FileStats& rhs) { return lhs.path() < rhs.path(); });

  int out_size = static_cast<int>(stats.size());
  auto out_descendant_counts = std::make_shared<std::vector<int>>(stats.size(), 0);
  auto out_stats = std::make_shared<std::vector<FileStats>>(std::move(stats));

  std::vector<int> stack;

  for (int i = 0; i < out_size; ++i) {
    while (stack.size() != 0 &&
           !IsDescendantOf(out_stats->at(stack.back()), out_stats->at(i))) {
      out_descendant_counts->at(stack.back()) = i - 1 - stack.back();
      stack.pop_back();
    }

    stack.push_back(i);
  }

  while (stack.size() != 0) {
    out_descendant_counts->at(stack.back()) = out_size - 1 - stack.back();
    stack.pop_back();
  }

  return PathForest(std::move(out_stats), std::move(out_descendant_counts));
}

bool PathForest::Equals(const PathForest& other) const {
  return size() == other.size() && *stats_ == *other.stats_ &&
         *descendant_counts_ == *other.descendant_counts_;
}

std::string PathForest::ToString() const {
  std::string repr = "PathForest:";
  if (size() == 0) {
    return repr + " []";
  }

  auto visitor = [&](const fs::FileStats& stats, int i) {
    auto segments = fs::internal::SplitAbstractPath(stats.path());
    repr += "\n" + stats.path();
    return Status::OK();
  };

  DCHECK_OK(Visit(visitor));
  return repr;
}

std::ostream& operator<<(std::ostream& os, const PathForest& tree) {
  return os << tree.ToString();
}

}  // namespace fs
}  // namespace arrow
