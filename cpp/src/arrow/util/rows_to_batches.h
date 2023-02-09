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

namespace arrow::util {

namespace detail {

// Default identity function row accessor. Used to for the common case where the value
// of each row iterated over is it's self also directly iterable.
[[nodiscard]] constexpr inline auto MakeDefaultRowAccessor() {
  return [](auto& x) -> Result<decltype(std::ref(x))> { return std::ref(x); };
}

// Meta-funciton to check if a type `T` is a range (iterable using `std::begin()` /
// `std::end()`). `is_range<T>::value` will be false if `T` is not a valid range.
template <typename T, typename = void>
struct is_range : std::false_type {};

template <typename T>
struct is_range<T, std::void_t<decltype(std::begin(std::declval<T>())),
                               decltype(std::end(std::declval<T>()))>> : std::true_type {
};

}  // namespace detail

/// \brief Utility function for converting any row-based structure into an
/// `arrow::RecordBatchReader` (this can be easily converted to an `arrow::Table` using
/// `arrow::RecordBatchReader::ToTable()`).
///
/// Examples of supported types:
/// - `std::vector<std::vector<std::variant<int, bsl::string>>>`
/// - `std::vector<MyRowStruct>`

/// If `rows` (client’s row-based structure) is not a valid C++ range, the client will
/// need to either make it iterable, or make an adapter/wrapper that is a valid C++
/// range.

/// The client must provide a `DataPointConvertor` callable type that will convert the
/// structure’s data points into the corresponding arrow types.

/// Complex nested rows can be supported by providing a custom `row_accessor` instead
/// of the default.

/// Example usage:
/// \code{.cpp}
/// auto IntConvertor = [](ArrayBuilder& array_builder, int value) {
///  return static_cast<Int64Builder&>(array_builder).Append(value);
/// };
/// std::vector<std::vector<int>> data = {{1, 2, 4}, {5, 6, 7}};
/// auto batches = RowsToBatches(kTestSchema, std::ref(data), IntConvertor);
/// \endcode

/// \param[in] schema - the schema to be used in the `RecordBatchReader`

/// \param[in] rows - iterable row-based structure that will be converted to arrow
/// batches

/// \param[in] data_point_convertor - client provided callable type that will convert
/// the structure’s data points into the corresponding arrow types. The convertor must
/// return an error `Status` if an error happens during conversion.

/// \param[in] row_accessor - In the common case where the value of each row iterated
/// over is it's self also directly iterable, the client can just use the default.
/// the provided callable must take the values of the otter `rows` range and return a
/// `std::reference_wrapper<Range>` to the data points in a given row.
/// see: /ref `MakeDefaultRowAccessor`

/// \return `Result<std::shared_ptr<RecordBatchReader>>>` result will be a
/// `std::shared_ptr<RecordBatchReader>>` if not errors occurred, else an error status.
template <class Range, class DataPointConvertor,
          class RowAccessor = decltype(detail::MakeDefaultRowAccessor())>
[[nodiscard]] typename std::enable_if_t<detail::is_range<Range>::value,
                                        Result<std::shared_ptr<RecordBatchReader>>>
/* Result<std::shared_ptr<RecordBatchReader>>> */ RowsToBatches(
    const std::shared_ptr<Schema>& schema, std::reference_wrapper<Range> rows,
    DataPointConvertor&& data_point_convertor,
    RowAccessor&& row_accessor = detail::MakeDefaultRowAccessor(),
    MemoryPool* pool = default_memory_pool()) {
  const std::size_t batch_size = 1024;
  auto make_next_batch =
      [rows_ittr = std::begin(rows.get()), rows_ittr_end = std::end(rows.get()),
       schema = schema, row_accessor = std::forward<RowAccessor>(row_accessor),
       data_point_convertor = std::forward<DataPointConvertor>(
           data_point_convertor)]() mutable -> Result<std::shared_ptr<RecordBatch>> {
    if (rows_ittr == rows_ittr_end) return NULLPTR;

    ARROW_ASSIGN_OR_RAISE(
        auto record_batch_builder,
        RecordBatchBuilder::Make(schema, pool, batch_size));

    for (size_t i = 0; i < batch_size and (rows_ittr != rows_ittr_end);
         i++, std::advance(rows_ittr, 1)) {
      size_t col_index = 0;
      ARROW_ASSIGN_OR_RAISE(auto row, row_accessor(*rows_ittr));
      for (auto& data_point : row.get()) {
        ArrayBuilder* array_builder = record_batch_builder->GetField(col_index);
        ARROW_RETURN_IF(array_builder == NULLPTR,
                        Status::Invalid("array_builder == NULLPTR"));

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

}  // namespace arrow::util
