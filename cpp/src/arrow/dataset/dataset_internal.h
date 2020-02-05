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

/// \brief GetFragmentsFromSources transforms a vector<Source> into a
/// flattened FragmentIterator.
static inline FragmentIterator GetFragmentsFromSources(
    const SourceVector& sources, std::shared_ptr<ScanOptions> options) {
  // Iterator<Source>
  auto sources_it = MakeVectorIterator(sources);

  // Source -> Iterator<Fragment>
  auto fn = [options](std::shared_ptr<Source> source) -> FragmentIterator {
    return source->GetFragments(options);
  };

  // Iterator<Iterator<Fragment>>
  auto fragments_it = MakeMapIterator(fn, std::move(sources_it));

  // Iterator<Fragment>
  return MakeFlattenIterator(std::move(fragments_it));
}

inline std::shared_ptr<Schema> SchemaFromColumnNames(
    const std::shared_ptr<Schema>& input, const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<Field>> columns;
  for (const auto& name : column_names) {
    auto field = input->GetFieldByName(name);
    if (field != nullptr) {
      columns.push_back(std::move(field));
    }
  }

  return std::make_shared<Schema>(columns);
}

}  // namespace dataset
}  // namespace arrow
