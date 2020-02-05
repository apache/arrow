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

Result<PathForest> PathForest::MakeFromPreSorted(std::vector<FileStats> stats) {
  int size = static_cast<int>(stats.size());
  std::vector<int> descendant_counts(size, 0), parents(size, -1);

  std::stack<int> parent_stack;
  auto is_child_of_stack_top = [&](int i) {
    return internal::IsAncestorOf(stats[parent_stack.top()].path(), stats[i].path());
  };

  for (int i = 0; i < size; ++i) {
    while (parent_stack.size() != 0 && !is_child_of_stack_top(i)) {
      // stats[parent_stack.top()] has no more descendants; finalize count
      descendant_counts[parent_stack.top()] = i - 1 - parent_stack.top();
      parent_stack.pop();
    }

    if (parent_stack.size() != 0) {
      parents[i] = parent_stack.top();
    }

    parent_stack.push(i);
  }

  // finalize descendant_counts for anything left in the stack
  while (parent_stack.size() != 0) {
    descendant_counts[parent_stack.top()] = size - 1 - parent_stack.top();
    parent_stack.pop();
  }

  return PathForest(0, size, std::make_shared<std::vector<FileStats>>(std::move(stats)),
                    std::make_shared<std::vector<int>>(std::move(descendant_counts)),
                    std::make_shared<std::vector<int>>(std::move(parents)));
}

bool PathForest::Equals(const PathForest& other) const {
  return size() == other.size() && *stats_ == *other.stats_;
}

std::string PathForest::ToString() const {
  std::string repr = "PathForest:";
  if (size() == 0) {
    return repr + " []";
  }

  DCHECK_OK(Visit([&](Ref ref) {
    repr += "\n" + ref.stats().path();
    if (ref.stats().IsDirectory() && repr.back() != '/') {
      repr += "/";
    }
    return Status::OK();
  }));
  return repr;
}

const FileStats& PathForest::Ref::stats() const {
  return forest->stats_->at(forest->offset_ + i);
}

int PathForest::Ref::num_descendants() const {
  return forest->descendant_counts_->at(forest->offset_ + i);
}

PathForest PathForest::Ref::descendants() const {
  return PathForest(forest->offset_ + i + 1, num_descendants(), forest->stats_,
                    forest->descendant_counts_, forest->parents_);
}

PathForest::Ref PathForest::Ref::parent() const {
  // XXX(bkietz) this doesn't *need* to be an explicit buffer
  auto parent_i = forest->parents_->at(forest->offset_ + i);
  if (parent_i < forest->offset_) {
    return Ref{nullptr, 0};
  }

  return Ref{forest, parent_i};
}

std::vector<PathForest::Ref> PathForest::roots() const {
  std::vector<Ref> roots;
  DCHECK_OK(Visit([&](Ref ref) -> PathForest::MaybePrune {
    roots.push_back(ref);
    return PathForest::Prune;
  }));
  return roots;
}

std::ostream& operator<<(std::ostream& os, const PathForest& tree) {
  return os << tree.ToString();
}

}  // namespace fs
}  // namespace arrow
