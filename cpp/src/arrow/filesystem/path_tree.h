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

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/compare.h"

namespace arrow {
namespace fs {

class ARROW_EXPORT PathTree;

/// \brief A PathForest consists of multiples PathTree
using PathForest = std::vector<PathTree>;

/// \brief A PathTree is a utility to transform a vector of FileStats into a
/// forest representation for tree traversal purposes. Node in the graph wraps
/// a FileStats. Files are expected to be found only at leaves of the tree.
class ARROW_EXPORT PathTree : public util::EqualityComparable<PathTree> {
 public:
  explicit PathTree(FileStats stats);

  /// \brief Transforms a FileStats vector into a forest of trees. Since there
  /// is no guarantee of complete trees, it is possible to have a forest
  /// (multiple roots). The caller should ensure that stats does not contain duplicates.
  static Result<PathForest> Make(std::vector<FileStats> stats);

  /// \brief Returns the FileStat of this node.
  const FileStats& stats() const { return stats_->at(offset_); }

  /// \brief Returns the subtrees under this node.
  PathForest subtrees() const;

  /// \brief Returns the number of descendants of this node.
  int num_descendants() const { return descendant_counts_->at(offset_); }

  bool Equals(const PathTree& other) const;

  std::string ToString() const;

  enum { Continue, Prune };
  using MaybePrune = Result<decltype(Prune)>;

  template <typename Visitor>
  Status Visit(Visitor&& v) const {
    static std::is_same<decltype(v(stats())), MaybePrune> with_pruning;
    return VisitImpl(std::forward<Visitor>(v), with_pruning);
  }

 protected:
  PathTree(std::shared_ptr<std::vector<FileStats>> stats,
           std::shared_ptr<std::vector<int>> descendant_counts, int offset)
      : stats_(std::move(stats)),
        descendant_counts_(std::move(descendant_counts)),
        offset_(offset) {}

  PathTree WithOffset(int offset) const {
    PathTree copy = *this;
    copy.offset_ = offset;
    return copy;
  }

  /// \brief Visit with eager pruning.
  template <typename Visitor>
  Status VisitImpl(Visitor&& v, std::true_type) const {
    auto action = Prune;
    ARROW_ASSIGN_OR_RAISE(action, v(stats()));
    if (action == Prune) {
      return Status::OK();
    }

    for (int i = descendants_begin(); i < descendants_end(); ++i) {
      ARROW_ASSIGN_OR_RAISE(action, v(stats_->at(i)));
      if (action == Prune) {
        // skip descendants
        i += descendant_counts_->at(i);
      }
    }
    return Status::OK();
  }

  template <typename Visitor>
  Status VisitImpl(Visitor&& v, std::false_type) const {
    return Visit([&](const FileStats& s) -> MaybePrune {
      RETURN_NOT_OK(v(s));
      return Continue;
    });
  }

  int descendants_begin() const { return offset_ + 1; }
  int descendants_end() const { return descendants_begin() + num_descendants(); }

  std::shared_ptr<std::vector<FileStats>> stats_;
  std::shared_ptr<std::vector<int>> descendant_counts_;
  int offset_;
};

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const PathTree& tree);

}  // namespace fs
}  // namespace arrow
