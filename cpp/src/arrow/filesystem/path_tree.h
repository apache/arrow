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
#include <iosfwd>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/status.h"

namespace arrow {
namespace fs {

class ARROW_EXPORT PathTree;

/// \brief A PathForest consists of multiples PathTree
using PathForest = std::vector<std::shared_ptr<PathTree>>;

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
  static Status Make(std::vector<FileStats> stats, PathForest* out);

  /// \brief Like MakeForest but fails if there's more than one root.
  static Status Make(std::vector<FileStats> stats, std::shared_ptr<PathTree>* out);

  /// \brief Returns the FileStat of this node.
  FileStats stats() const { return stats_; }
  /// \brief Returns the subtrees under this node.
  std::vector<std::shared_ptr<PathTree>> subtrees() const { return subtrees_; }

  /// \brief Visit with eager pruning.
  template <typename Visitor, typename Matcher>
  Status Visit(Visitor&& v, Matcher&& m) const {
    bool match = false;
    ARROW_RETURN_NOT_OK(m(stats_, &match));
    if (!match) {
      return Status::OK();
    }

    ARROW_RETURN_NOT_OK(v(stats_));

    for (const auto& t : subtrees_) {
      ARROW_RETURN_NOT_OK(t->Visit(v, m));
    }

    return Status::OK();
  }

  template <typename Visitor>
  Status Visit(Visitor&& v) const {
    auto always_match = [](const FileStats& t, bool* match) {
      *match = true;
      return Status::OK();
    };
    return Visit(v, always_match);
  }

  bool operator==(const PathTree& other) const {
    return stats_ == other.stats_ && subtrees_ == other.subtrees_;
  }

 protected:
  FileStats stats_;
  std::vector<std::shared_ptr<PathTree>> subtrees_;

  // The AddChild method is convenient to create trees in a top-down fashion,
  // e.g. the Make factory constructor.
  void AddChild(std::shared_ptr<PathTree> child) {
    subtrees_.push_back(std::move(child));
  }
};

ARROW_EXPORT std::ostream& operator<<(std::ostream& os,
                                      const std::shared_ptr<PathTree>& tree);
ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const PathTree& tree);

ARROW_EXPORT bool operator==(const std::shared_ptr<PathTree>& lhs,
                             const std::shared_ptr<PathTree>& rhs);

}  // namespace fs
}  // namespace arrow
