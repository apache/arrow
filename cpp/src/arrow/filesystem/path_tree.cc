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

#include "arrow/filesystem/path_tree.h"

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

PathTree::PathTree(FileStats stats)
    : PathTree(std::make_shared<std::vector<FileStats>>(),
               std::make_shared<std::vector<int>>(), 0) {
  stats_->push_back(std::move(stats));
  descendant_counts_->push_back(0);
}

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

PathForest PathTree::subtrees() const {
  PathForest out;
  for (int i = descendants_begin(); i < descendants_end(); ++i) {
    out.push_back(WithOffset(i));
    // skip indirect descendants
    i += descendant_counts_->at(i);
  }
  return out;
}

Result<PathForest> PathTree::Make(std::vector<FileStats> stats) {
  std::stable_sort(
      stats.begin(), stats.end(),
      [](const FileStats& lhs, const FileStats& rhs) { return lhs.path() < rhs.path(); });

  PathForest out;
  int out_count = static_cast<int>(stats.size());
  auto out_descendant_counts = std::make_shared<std::vector<int>>(stats.size(), 0);
  auto out_stats = std::make_shared<std::vector<FileStats>>(std::move(stats));

  std::vector<int> stack;

  for (int i = 0; i < out_count; ++i) {
    while (stack.size() != 0 &&
           !IsDescendantOf(out_stats->at(stack.back()), out_stats->at(i))) {
      out_descendant_counts->at(stack.back()) = i - 1 - stack.back();
      stack.pop_back();
    }

    if (stack.size() == 0) {
      out.push_back(PathTree(out_stats, out_descendant_counts, i));
    }

    stack.push_back(i);
  }

  while (stack.size() != 0) {
    out_descendant_counts->at(stack.back()) = out_count - 1 - stack.back();
    stack.pop_back();
  }

  return out;
}

bool PathTree::Equals(const PathTree& other) const {
  if (num_descendants() != other.num_descendants() || stats() != other.stats()) {
    return false;
  }

  for (int i = descendants_begin(), i_other = other.descendants_begin();
       i < descendants_end(); ++i, ++i_other) {
    if (stats_->at(i) != other.stats_->at(i_other)) {
      return false;
    }
  }

  return true;
}

std::string PathTree::ToString() const {
  std::string out = stats().ToString();

  out += "[";
  int i = 0;
  for (const auto& subtree : subtrees()) {
    if (i++ != 0) out += ", ";
    out += subtree.ToString();
  }
  out += "]";

  return out;
}

std::ostream& operator<<(std::ostream& os, const PathTree& tree) {
  return os << tree.ToString();
}

}  // namespace fs
}  // namespace arrow
