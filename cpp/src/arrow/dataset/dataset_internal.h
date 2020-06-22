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
#include "arrow/dataset/type_fwd.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

/// \brief GetFragmentsFromDatasets transforms a vector<Dataset> into a
/// flattened FragmentIterator.
inline FragmentIterator GetFragmentsFromDatasets(const DatasetVector& datasets,
                                                 std::shared_ptr<Expression> predicate) {
  // Iterator<Dataset>
  auto datasets_it = MakeVectorIterator(datasets);

  // Dataset -> Iterator<Fragment>
  auto fn = [predicate](std::shared_ptr<Dataset> dataset) -> FragmentIterator {
    return dataset->GetFragments(predicate);
  };

  // Iterator<Iterator<Fragment>>
  auto fragments_it = MakeMapIterator(fn, std::move(datasets_it));

  // Iterator<Fragment>
  return MakeFlattenIterator(std::move(fragments_it));
}

inline RecordBatchIterator IteratorFromReader(std::shared_ptr<RecordBatchReader> reader) {
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

}  // namespace dataset
}  // namespace arrow
