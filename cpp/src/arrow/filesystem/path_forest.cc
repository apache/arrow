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
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

Result<PathForest> PathForest::Make(std::vector<FileStats> stats) {
  std::stable_sort(
      stats.begin(), stats.end(),
      [](const FileStats& lhs, const FileStats& rhs) { return lhs.path() < rhs.path(); });

  std::vector<int> descendant_counts(stats.size(), 0);

  std::stack<int> parent_stack;
  auto is_child_of_stack_top = [&](int i) {
    return internal::IsAncestorOf(stats[parent_stack.top()].path(), stats[i].path());
  };

  for (int i = 0; i < static_cast<int>(stats.size()); ++i) {
    while (parent_stack.size() != 0 && !is_child_of_stack_top(i)) {
      // stats[parent_stack.top()] has no more descendants; finalize count
      descendant_counts[parent_stack.top()] = i - 1 - parent_stack.top();
      parent_stack.pop();
    }

    parent_stack.push(i);
  }

  // finalize descendant_counts for anything left in the stack
  while (parent_stack.size() != 0) {
    descendant_counts[parent_stack.top()] =
        static_cast<int>(stats.size()) - 1 - parent_stack.top();
    parent_stack.pop();
  }

  return PathForest(std::make_shared<std::vector<FileStats>>(std::move(stats)),
                    std::make_shared<std::vector<int>>(std::move(descendant_counts)));
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
