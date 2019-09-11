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

#include "arrow/filesystem/filesystem.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace arrow {
namespace fs {

class ARROW_EXPORT PathTree;

/// \brief A PathTreeForest consists of multiples PathTree
using PathTreeForest = std::vector<std::shared_ptr<PathTree>>;

/// \brief A PathTree is a utility to transform a vector of FileStats into a
/// forest representation for tree traversal purposes. Node in the graph wraps
/// a FileStats. Files are expected to be found only at leaves of the tree.
class ARROW_EXPORT PathTree {
 public:
  explicit PathTree(FileStats stats) : stats_(stats) {}
  PathTree(FileStats stats, std::vector<std::shared_ptr<PathTree>> subtrees)
      : stats_(stats), subtrees_(std::move(subtrees)) {}

  /// \brief Transforms a FileStats vector into a forest of trees. Since there
  /// is no guarantee of complete trees, it is possible to have a forest
  /// (multiple roots). The caller should ensure that stats have unique path.
  static Status Make(std::vector<FileStats> stats, PathTreeForest* out);

  /// \brief Like MakeForest but fails if there's more than one root.
  static Status Make(std::vector<FileStats> stats, std::shared_ptr<PathTree>* out);

  /// \brief Returns the FileStat of this node.
  FileStats stats() const { return stats_; }
  /// \brief Returns the subtrees under this node.
  std::vector<std::shared_ptr<PathTree>> subtrees() const { return subtrees_; }

  template <typename Visitor>
  void Visit(Visitor v) const {
    v(stats_);

    auto recurse = [&v](const std::shared_ptr<PathTree>& tree) { tree->Visit(v); };
    std::for_each(subtrees_.cbegin(), subtrees_.cend(), recurse);
  }

  /// \brief Visit with eager pruning.
  template <typename Visitor, typename Matcher>
  void Visit(Visitor v, Matcher m) const {
    if (!m(stats_)) {
      return;
    }

    v(stats_);

    auto recurse = [&v, &m](const std::shared_ptr<PathTree>& tree) { tree->Visit(v, m); };
    std::for_each(subtrees_.cbegin(), subtrees_.cend(), recurse);
  }

  void AddChild(std::shared_ptr<PathTree> child) {
    subtrees_.push_back(std::move(child));
  }

 protected:
  FileStats stats_;
  std::vector<std::shared_ptr<PathTree>> subtrees_;
};

}  // namespace fs
}  // namespace arrow
