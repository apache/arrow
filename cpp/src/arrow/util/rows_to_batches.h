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

#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table_builder.h"
#include "arrow/util/iterator.h"

#include <type_traits>

namespace arrow {

namespace detail {
[[nodiscard]] constexpr inline auto make_default_accessor() {
  return [](auto& x) -> Result<decltype(std::ref(x))> { return std::ref(x); };
}

template <typename T, typename = void>
struct is_range : std::false_type {};

template <typename T>
struct is_range<T, std::void_t<decltype(std::begin(std::declval<T>())),
                               decltype(std::end(std::declval<T>()))>> : std::true_type {
};

}  // namespace detail

template <class Range, class DataPointConvertor,
          class RowAccessor = decltype(detail::make_default_accessor())>
typename std::enable_if_t<detail::is_range<Range>::value,
                          Result<std::shared_ptr<RecordBatchReader>>>
/* Result<std::shared_ptr<RecordBatchReader>>> */ rows_to_batches(
    const std::shared_ptr<Schema>& schema, std::reference_wrapper<Range> rows,
    DataPointConvertor&& data_point_convertor,
    RowAccessor&& row_accessor = detail::make_default_accessor()) {
  const std::size_t batch_size = 1024;
  auto make_next_batch =
      [rows_ittr = std::begin(rows.get()), rows_ittr_end = std::end(rows.get()),
       schema = schema, row_accessor = std::forward<RowAccessor>(row_accessor),
       data_point_convertor = std::forward<DataPointConvertor>(
           data_point_convertor)]() mutable -> Result<std::shared_ptr<RecordBatch>> {
    if (rows_ittr == rows_ittr_end) return nullptr;

    ARROW_ASSIGN_OR_RAISE(
        auto record_batch_builder,
        RecordBatchBuilder::Make(schema, default_memory_pool(), batch_size));

    for (size_t i = 0; i < batch_size and (rows_ittr != rows_ittr_end);
         i++, std::advance(rows_ittr, 1)) {
      size_t col_index = 0;
      ARROW_ASSIGN_OR_RAISE(auto row, row_accessor(*rows_ittr));
      for (auto& data_point : row.get()) {
        ArrayBuilder* array_builder = record_batch_builder->GetField(col_index);
        ARROW_RETURN_IF(array_builder == nullptr,
                        Status::Invalid("array_builder == nullptr"));

        ARROW_RETURN_NOT_OK(data_point_convertor(*array_builder, data_point));
        col_index++;
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto result, record_batch_builder->Flush());
    return result;
  };
  return RecordBatchReader::MakeFromIterator(MakeFunctionIterator(make_next_batch),
                                             schema);
}

}  // namespace arrow
