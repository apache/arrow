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

/// \brief A PathForest is a utility to transform a vector of FileStats into a
/// forest representation for tree traversal purposes. Each node in the graph wraps
/// a FileStats. Files are expected to be found only at leaves of the tree.
class ARROW_EXPORT PathForest : public util::EqualityComparable<PathForest> {
 public:
  /// \brief Transforms a FileStats vector into a forest. Since there
  /// is no guarantee of a single shared root, it is possible to have a forest
  /// (multiple roots). The caller should ensure that stats does not contain duplicates.
  static Result<PathForest> Make(std::vector<FileStats> stats);

  /// \brief Returns the number of nodes in this forest.
  int size() const { return size_; }

  bool Equals(const PathForest& other) const;

  std::string ToString() const;

  const FileStats& stats(int i) const { return stats_->at(offset_ + i); }

  int num_descendants(int i) const { return descendant_counts_->at(offset_ + i); }

  /// Returns a PathForest containing only nodes which are descendants of the node at i
  PathForest DescendantsForest(int i) const {
    return PathForest(offset_ + i + 1, num_descendants(i), stats_, descendant_counts_);
  }

  enum { Continue, Prune };
  using MaybePrune = Result<decltype(Prune)>;

  /// Visitors may return MaybePrune to enable eager pruning. Visitors will be called with
  /// the FileStats of the currently visited node and the index of that node in depth
  /// first visitation order (useful for accessing parallel vectors of associated data).
  template <typename Visitor>
  Status Visit(Visitor&& v) const {
    static std::is_same<decltype(v(std::declval<const FileStats>(), 0)), MaybePrune>
        with_pruning;
    return VisitImpl(std::forward<Visitor>(v), with_pruning);
  }

 protected:
  PathForest(std::shared_ptr<std::vector<FileStats>> stats,
             std::shared_ptr<std::vector<int>> descendant_counts)
      : offset_(0),
        size_(static_cast<int>(stats->size())),
        stats_(std::move(stats)),
        descendant_counts_(std::move(descendant_counts)) {}

  PathForest(int offset, int size, std::shared_ptr<std::vector<FileStats>> stats,
             std::shared_ptr<std::vector<int>> descendant_counts)
      : offset_(offset),
        size_(size),
        stats_(std::move(stats)),
        descendant_counts_(std::move(descendant_counts)) {}

  /// \brief Visit with eager pruning.
  template <typename Visitor>
  Status VisitImpl(Visitor&& v, std::true_type) const {
    for (int i = 0; i < size_; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto action, v(stats(i), i));

      if (action == Prune) {
        // skip descendants
        i += num_descendants(i);
      }
    }

    return Status::OK();
  }

  template <typename Visitor>
  Status VisitImpl(Visitor&& v, std::false_type) const {
    return Visit([&](const FileStats& s, int i) -> MaybePrune {
      RETURN_NOT_OK(v(s, i));
      return Continue;
    });
  }

  int offset_, size_;
  std::shared_ptr<std::vector<FileStats>> stats_;
  std::shared_ptr<std::vector<int>> descendant_counts_;
};

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const PathForest& tree);

}  // namespace fs
}  // namespace arrow
