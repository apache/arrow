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
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace dataset {

/// \brief GetFragmentsFromDatasets transforms a vector<Dataset> into a
/// flattened FragmentIterator.
inline Result<FragmentIterator> GetFragmentsFromDatasets(const DatasetVector& datasets,
                                                         Expression predicate) {
  // Iterator<Dataset>
  auto datasets_it = MakeVectorIterator(datasets);

  // Dataset -> Iterator<Fragment>
  auto fn = [predicate](std::shared_ptr<Dataset> dataset) -> Result<FragmentIterator> {
    return dataset->GetFragments(predicate);
  };

  // Iterator<Iterator<Fragment>>
  auto fragments_it = MakeMaybeMapIterator(fn, std::move(datasets_it));

  // Iterator<Fragment>
  return MakeFlattenIterator(std::move(fragments_it));
}

inline RecordBatchIterator IteratorFromReader(
    const std::shared_ptr<RecordBatchReader>& reader) {
  return MakeFunctionIterator([reader] { return reader->Next(); });
}

inline std::shared_ptr<Schema> SchemaFromColumnNames(
    const std::shared_ptr<Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<Field>> columns;
  for (FieldRef ref : column_names) {
    auto maybe_field = ref.GetOne(*input);
    if (maybe_field.ok()) {
      columns.push_back(std::move(maybe_field).ValueOrDie());
    }
  }

  return schema(std::move(columns))->WithMetadata(input->metadata());
}

// Helper class for efficiently detecting subtrees given fragment partition expressions.
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
  // An encoded fragment (if fragment_index is set) or subtree.
  struct Encoded {
    util::optional<int> fragment_index;
    expression_codes partition_expression;
  };

  std::unordered_map<Expression, expression_code, Expression::Hash> expr_to_code_;
  std::vector<Expression> code_to_expr_;
  std::unordered_set<expression_codes> subtree_exprs_;

  // Encode a subexpression (returning the existing code if possible).
  expression_code GetOrInsert(const Expression& expr) {
    auto next_code = static_cast<int>(expr_to_code_.size());
    auto it_success = expr_to_code_.emplace(expr, next_code);

    if (it_success.second) {
      code_to_expr_.push_back(expr);
    }
    return it_success.first->second;
  }

  // Encode an expression (recursively breaking up conjunction members if possible).
  void EncodeConjunctionMembers(const Expression& expr, expression_codes* codes) {
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

  // Convert an encoded subtree or fragment back into an expression.
  Expression GetSubtreeExpression(const Encoded& encoded_subtree) {
    // Filters will already be simplified by all of a subtree's ancestors, so
    // we only need to simplify the filter by the trailing conjunction member
    // of each subtree.
    return code_to_expr_[encoded_subtree.partition_expression.back()];
  }

  // Insert subtrees for each component of an encoded partition expression.
  void GenerateSubtrees(expression_codes partition_expression,
                        std::vector<Encoded>* encoded) {
    while (!partition_expression.empty()) {
      if (subtree_exprs_.insert(partition_expression).second) {
        Encoded encoded_subtree{/*fragment_index=*/util::nullopt, partition_expression};
        encoded->push_back(std::move(encoded_subtree));
      }
      partition_expression.resize(partition_expression.size() - 1);
    }
  }

  // Encode the fragment's partition expression and generate subtrees for it as well.
  void EncodeOneFragment(int fragment_index, const Fragment& fragment,
                         std::vector<Encoded>* encoded) {
    Encoded encoded_fragment{fragment_index, {}};

    EncodeConjunctionMembers(fragment.partition_expression(),
                             &encoded_fragment.partition_expression);

    GenerateSubtrees(encoded_fragment.partition_expression, encoded);

    encoded->push_back(std::move(encoded_fragment));
  }

  template <typename Fragments>
  std::vector<Encoded> EncodeFragments(const Fragments& fragments) {
    std::vector<Encoded> encoded;
    for (size_t i = 0; i < fragments.size(); ++i) {
      EncodeOneFragment(static_cast<int>(i), *fragments[i], &encoded);
    }
    return encoded;
  }
};

inline bool operator==(const SubtreeImpl::Encoded& l, const SubtreeImpl::Encoded& r) {
  return l.fragment_index == r.fragment_index &&
         l.partition_expression == r.partition_expression;
}

/// Get fragment scan options of the expected type.
/// \return Fragment scan options if provided on the scan options, else the default
///     options if set, else a default-constructed value. If options are provided
///     but of the wrong type, an error is returned.
template <typename T>
arrow::Result<std::shared_ptr<T>> GetFragmentScanOptions(
    const std::string& type_name, ScanOptions* scan_options,
    const std::shared_ptr<FragmentScanOptions>& default_options) {
  auto source = default_options;
  if (scan_options && scan_options->fragment_scan_options) {
    source = scan_options->fragment_scan_options;
  }
  if (!source) {
    return std::make_shared<T>();
  }
  if (source->type_name() != type_name) {
    return Status::Invalid("FragmentScanOptions of type ", source->type_name(),
                           " were provided for scanning a fragment of type ", type_name);
  }
  return internal::checked_pointer_cast<T>(source);
}

}  // namespace dataset
}  // namespace arrow
