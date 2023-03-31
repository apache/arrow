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

#include <memory>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace dataset {

/// A Forest is a view of a sorted range which carries an ancestry relation in addition
/// to an ordering relation: each element's descendants appear directly after it.
/// This can be used to efficiently skip subtrees when iterating through the range.
class Forest {
 public:
  Forest() = default;

  /// \brief Construct a Forest viewing the range [0, size).
  Forest(int size, std::function<bool(int, int)> is_ancestor) : size_(size) {
    std::vector<int> descendant_counts(size, 0);

    std::vector<int> parent_stack;

    for (int i = 0; i < size; ++i) {
      while (parent_stack.size() != 0) {
        if (is_ancestor(parent_stack.back(), i)) break;

        // parent_stack.back() has no more descendants; finalize count and pop
        descendant_counts[parent_stack.back()] = i - 1 - parent_stack.back();
        parent_stack.pop_back();
      }

      parent_stack.push_back(i);
    }

    // finalize descendant_counts for anything left in the stack
    while (parent_stack.size() != 0) {
      descendant_counts[parent_stack.back()] = size - 1 - parent_stack.back();
      parent_stack.pop_back();
    }

    descendant_counts_ = std::make_shared<std::vector<int>>(std::move(descendant_counts));
  }

  /// \brief Returns the number of nodes in this forest.
  int size() const { return size_; }

  bool Equals(const Forest& other) const {
    auto it = descendant_counts_->begin();
    return size_ == other.size_ &&
           std::equal(it, it + size_, other.descendant_counts_->begin());
  }

  struct Ref {
    int num_descendants() const { return forest->descendant_counts_->at(i); }

    bool IsAncestorOf(const Ref& ref) const {
      return i < ref.i && i + 1 + num_descendants() > ref.i;
    }

    explicit operator bool() const { return forest != NULLPTR; }

    const Forest* forest;
    int i;
  };

  /// \brief Visit with eager pruning. Visitors must return Result<bool>, using
  /// true to indicate a subtree should be visited and false to indicate that the
  /// subtree should be skipped.
  template <typename PreVisitor, typename PostVisitor>
  Status Visit(PreVisitor&& pre, PostVisitor&& post) const {
    std::vector<Ref> parent_stack;

    for (int i = 0; i < size_; ++i) {
      Ref ref = {this, i};

      while (parent_stack.size() > 0) {
        if (parent_stack.back().IsAncestorOf(ref)) break;

        post(parent_stack.back());
        parent_stack.pop_back();
      }

      ARROW_ASSIGN_OR_RAISE(bool visit_subtree, pre(ref));

      if (!visit_subtree) {
        // skip descendants
        i += ref.num_descendants();
        continue;
      }

      parent_stack.push_back(ref);
    }

    return Status::OK();
  }

  Ref operator[](int i) const { return Ref{this, i}; }

 private:
  int size_ = 0;
  std::shared_ptr<std::vector<int>> descendant_counts_;
};

}  // namespace dataset
}  // namespace arrow
