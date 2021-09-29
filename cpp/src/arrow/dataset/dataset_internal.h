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
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

/// \brief GetFragmentsFromDatasets transforms a vector<Dataset> into a
/// flattened FragmentIterator.
inline Result<FragmentIterator> GetFragmentsFromDatasets(const DatasetVector& datasets,
                                                         compute::Expression predicate) {
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

/// Get fragment scan options of the expected type.
/// \return Fragment scan options if provided on the scan options, else the default
///     options if set, else a default-constructed value. If options are provided
///     but of the wrong type, an error is returned.
template <typename T>
arrow::Result<std::shared_ptr<T>> GetFragmentScanOptions(
    const std::string& type_name, const ScanOptions* scan_options,
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
  return ::arrow::internal::checked_pointer_cast<T>(source);
}

class FragmentDataset : public Dataset {
 public:
  FragmentDataset(std::shared_ptr<Schema> schema, FragmentVector fragments)
      : Dataset(std::move(schema)), fragments_(std::move(fragments)) {}

  FragmentDataset(std::shared_ptr<Schema> schema,
                  AsyncGenerator<std::shared_ptr<Fragment>> fragments)
      : Dataset(std::move(schema)), fragment_gen_(std::move(fragments)) {}

  std::string type_name() const override { return "fragment"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override {
    return std::make_shared<FragmentDataset>(std::move(schema), fragments_);
  }

 protected:
  Result<FragmentIterator> GetFragmentsImpl(compute::Expression predicate) override {
    if (fragment_gen_) {
      // TODO(ARROW-8163): Async fragment scanning can be forwarded rather than waiting
      // for the whole generator here. For now, all Dataset impls have a vector of
      // Fragments anyway
      auto fragments_fut = CollectAsyncGenerator(std::move(fragment_gen_));
      ARROW_ASSIGN_OR_RAISE(fragments_, fragments_fut.result());
    }

    // TODO(ARROW-12891) Provide subtree pruning for any vector of fragments
    FragmentVector fragments;
    for (const auto& fragment : fragments_) {
      ARROW_ASSIGN_OR_RAISE(
          auto simplified_filter,
          compute::SimplifyWithGuarantee(predicate, fragment->partition_expression()));

      if (simplified_filter.IsSatisfiable()) {
        fragments.push_back(fragment);
      }
    }
    return MakeVectorIterator(std::move(fragments));
  }

  FragmentVector fragments_;
  AsyncGenerator<std::shared_ptr<Fragment>> fragment_gen_;
};

// Given a record batch generator, creates a new generator that slices
// batches so individual batches have at most batch_size rows. The
// resulting generator is async-reentrant, but does not forward
// reentrant pulls, so apply readahead before using this helper.
inline RecordBatchGenerator MakeChunkedBatchGenerator(RecordBatchGenerator gen,
                                                      int64_t batch_size) {
  return MakeFlatMappedGenerator(
      std::move(gen),
      [batch_size](const std::shared_ptr<RecordBatch>& batch)
          -> ::arrow::AsyncGenerator<std::shared_ptr<::arrow::RecordBatch>> {
        const int64_t rows = batch->num_rows();
        if (rows <= batch_size) {
          return ::arrow::MakeVectorGenerator<std::shared_ptr<RecordBatch>>({batch});
        }
        std::vector<std::shared_ptr<RecordBatch>> slices;
        slices.reserve(rows / batch_size + (rows % batch_size != 0));
        for (int64_t i = 0; i < rows; i += batch_size) {
          slices.push_back(batch->Slice(i, batch_size));
        }
        return ::arrow::MakeVectorGenerator(std::move(slices));
      });
}

}  // namespace dataset
}  // namespace arrow
