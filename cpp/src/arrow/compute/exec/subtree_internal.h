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

#include <stdint.h>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow/compute/expression.h"

namespace arrow {
namespace compute {
// Helper class for efficiently detecting subtrees given expressions.
//
// Using fragment partition expressions as an example:
// Partition expressions are broken into conjunction members and each member dictionary
// encoded to impose a sortable ordering. In addition, subtrees are generated which span
// groups of fragments and nested subtrees. After encoding each fragment is guaranteed to
// be a descendant of at least one subtree. For example, given fragments in a
// HivePartitioning with paths:
//
//   /num=0/al=eh/dat.par
//   /num=0/al=be/dat.par
//   /num=1/al=eh/dat.par
//   /num=1/al=be/dat.par
//
// The following subtrees will be introduced:
//
//   /num=0/
//   /num=0/al=eh/
//   /num=0/al=eh/dat.par
//   /num=0/al=be/
//   /num=0/al=be/dat.par
//   /num=1/
//   /num=1/al=eh/
//   /num=1/al=eh/dat.par
//   /num=1/al=be/
//   /num=1/al=be/dat.par
struct SubtreeImpl {
  // Each unique conjunction member is mapped to an integer.
  using expression_code = char32_t;
  // Partition expressions are mapped to strings of codes; strings give us lexicographic
  // ordering (and potentially useful optimizations).
  using expression_codes = std::basic_string<expression_code>;
  // An encoded guarantee (if index is set) or subtree.
  struct Encoded {
    // An external index identifying the corresponding object (e.g. a Fragment) of the
    // guarantee.
    std::optional<int> index;
    // An encoded expression representing a guarantee.
    expression_codes guarantee;
  };

  std::unordered_map<compute::Expression, expression_code, compute::Expression::Hash>
      expr_to_code_;
  std::vector<compute::Expression> code_to_expr_;
  std::unordered_set<expression_codes> subtree_exprs_;

  // Encode a subexpression (returning the existing code if possible).
  expression_code GetOrInsert(const compute::Expression& expr) {
    auto next_code = static_cast<int>(expr_to_code_.size());
    auto it_success = expr_to_code_.emplace(expr, next_code);

    if (it_success.second) {
      code_to_expr_.push_back(expr);
    }
    return it_success.first->second;
  }

  // Encode an expression (recursively breaking up conjunction members if possible).
  void EncodeConjunctionMembers(const compute::Expression& expr,
                                expression_codes* codes) {
    if (auto call = expr.call()) {
      if (call->function_name == "and_kleene") {
        // expr is a conjunction, encode its arguments
        EncodeConjunctionMembers(call->arguments[0], codes);
        EncodeConjunctionMembers(call->arguments[1], codes);
        return;
      }
    }
    // expr is not a conjunction, encode it whole
    codes->push_back(GetOrInsert(expr));
  }

  // Convert an encoded subtree or guarantee back into an expression.
  compute::Expression GetSubtreeExpression(const Encoded& encoded_subtree) {
    // Filters will already be simplified by all of a subtree's ancestors, so
    // we only need to simplify the filter by the trailing conjunction member
    // of each subtree.
    return code_to_expr_[encoded_subtree.guarantee.back()];
  }

  // Insert subtrees for each component of an encoded partition expression.
  void GenerateSubtrees(expression_codes guarantee, std::vector<Encoded>* encoded) {
    while (!guarantee.empty()) {
      if (subtree_exprs_.insert(guarantee).second) {
        Encoded encoded_subtree{/*index=*/std::nullopt, guarantee};
        encoded->push_back(std::move(encoded_subtree));
      }
      guarantee.resize(guarantee.size() - 1);
    }
  }

  // Encode a guarantee, and generate subtrees for it as well.
  void EncodeOneGuarantee(int index, const Expression& guarantee,
                          std::vector<Encoded>* encoded) {
    Encoded encoded_guarantee{index, {}};
    EncodeConjunctionMembers(guarantee, &encoded_guarantee.guarantee);
    GenerateSubtrees(encoded_guarantee.guarantee, encoded);
    encoded->push_back(std::move(encoded_guarantee));
  }

  template <typename GetGuarantee>
  std::vector<Encoded> EncodeGuarantees(const GetGuarantee& get, int count) {
    std::vector<Encoded> encoded;
    for (int i = 0; i < count; ++i) {
      EncodeOneGuarantee(i, get(i), &encoded);
    }
    return encoded;
  }

  // Comparator for sort
  struct ByGuarantee {
    bool operator()(const Encoded& l, const Encoded& r) {
      const auto cmp = l.guarantee.compare(r.guarantee);
      if (cmp != 0) {
        return cmp < 0;
      }
      // Equal guarantees; sort encodings with indices after encodings without
      return (l.index ? 1 : 0) < (r.index ? 1 : 0);
    }
  };

  // Comparator for building a Forest
  struct IsAncestor {
    const std::vector<Encoded> encoded;

    bool operator()(int l, int r) const {
      if (encoded[l].index) {
        // Leaf-level object (e.g. a Fragment): not an ancestor.
        return false;
      }

      const auto& ancestor = encoded[l].guarantee;
      const auto& descendant = encoded[r].guarantee;

      if (descendant.size() >= ancestor.size()) {
        return std::equal(ancestor.begin(), ancestor.end(), descendant.begin());
      }
      return false;
    }
  };
};

inline bool operator==(const SubtreeImpl::Encoded& l, const SubtreeImpl::Encoded& r) {
  return l.index == r.index && l.guarantee == r.guarantee;
}

}  // namespace compute
}  // namespace arrow
