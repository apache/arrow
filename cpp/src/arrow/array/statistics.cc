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

#include "arrow/array/statistics.h"

#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_union.h"
#include "arrow/array/data.h"
#include "arrow/c/abi.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace arrow {

const std::shared_ptr<DataType>& ArrayStatistics::ValueToArrowType(
    const std::optional<ArrayStatistics::ValueType>& value,
    const std::shared_ptr<DataType>& array_type) {
  if (!value.has_value()) {
    return null();
  }

  struct Visitor {
    const std::shared_ptr<DataType>& array_type;

    const std::shared_ptr<DataType>& operator()(const bool&) { return boolean(); }
    const std::shared_ptr<DataType>& operator()(const int64_t&) { return int64(); }
    const std::shared_ptr<DataType>& operator()(const uint64_t&) { return uint64(); }
    const std::shared_ptr<DataType>& operator()(const double&) { return float64(); }
    const std::shared_ptr<DataType>& operator()(const std::shared_ptr<Scalar>& value) {
      return value->type;
    }
    const std::shared_ptr<DataType>& operator()(const std::string&) {
      switch (array_type->id()) {
        // TODO Add StringView and BinaryView to ArrayStatistics (GH-45664)
        case Type::STRING:
        case Type::BINARY:
        case Type::FIXED_SIZE_BINARY:
        case Type::LARGE_STRING:
        case Type::LARGE_BINARY:
          return array_type;
        default:
          return utf8();
      }
    }
  } visitor{array_type};
  return std::visit(visitor, value.value());
}
namespace {
bool ValueTypeEquality(const std::optional<ArrayStatistics::ValueType>& first,
                       const std::optional<ArrayStatistics::ValueType>& second,
                       const EqualOptions& options) {
  if (first == second) {
    return true;
  }
  if (!first || !second) {
    return false;
  }

  return std::visit(
      [&options](auto& v1, auto& v2) {
        if constexpr (std::is_same_v<std::decay_t<decltype(v1)>,
                                     std::shared_ptr<Scalar>> &&
                      std::is_same_v<std::decay_t<decltype(v2)>,
                                     std::shared_ptr<Scalar>>) {
          if (!v1 || !v2) {
            // both empty shared_ptr case is handled in std::optional and return true
            return false;
          }
          return v1->Equals(*v2, options);
        }
        return false;
      },
      first.value(), second.value());
}
}  // namespace
bool ArrayStatistics::Equals(const ArrayStatistics& other,
                             const EqualOptions& options) const {
  return null_count == other.null_count && distinct_count == other.distinct_count &&
         is_min_exact == other.is_min_exact && is_max_exact == other.is_max_exact &&
         ValueTypeEquality(this->max, other.max, options) &&
         ValueTypeEquality(this->min, other.min, options);
}
namespace internal {
// Perform a depth-first search (DFS) on the given array to extract
// all nested arrays up to the specified maximum nesting depth.
Result<ArrayDataVector> ExtractColumnsToArrayData(const std::shared_ptr<Array>& array,
                                                  const int32_t& max_nesting_depth) {
  ArrayDataVector traverse;
  ArrayDataVector result;

  int32_t remaining_nested_depth = max_nesting_depth;
  if (remaining_nested_depth <= 0) {
    return Status::Invalid("Max recursion depth reached");
  }
  result.push_back(array->data());
  if (!array->data()->child_data.empty()) {
    // depth delimiter
    traverse.emplace_back(nullptr);
    traverse.insert(traverse.end(), array->data()->child_data.crbegin(),
                    array->data()->child_data.crend());
    remaining_nested_depth--;
  }

  while (!traverse.empty()) {
    if (remaining_nested_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    auto child_data = traverse.back();
    traverse.pop_back();
    // Check whether the traversal of a depth is finished or not.
    if (!child_data) {
      remaining_nested_depth++;
      continue;
    }
    result.push_back(child_data);
    if (!child_data->child_data.empty()) {
      // depth delimiter
      traverse.emplace_back(nullptr);
      traverse.insert(traverse.end(), child_data->child_data.crbegin(),
                      child_data->child_data.crend());
      remaining_nested_depth--;
    }
  }
  return result;
}
namespace {
struct EnumeratedStatistics {
  int nth_statistics = 0;
  bool start_new_column = false;
  std::optional<int32_t> nth_column = std::nullopt;
  const char* key = nullptr;
  std::shared_ptr<DataType> type = nullptr;
  ArrayStatistics::ValueType value = false;
};
using OnStatistics =
    std::function<Status(const EnumeratedStatistics& enumerated_statistics)>;

Status EnumerateStatistics(const ArrayDataVector& extracted_array_data,
                           std::optional<int64_t> num_rows, OnStatistics on_statistics) {
  // TODO implement Statistic Schema attribute in C++(GH-45639)
  EnumeratedStatistics statistics;
  statistics.nth_statistics = 0;
  statistics.start_new_column = true;
  statistics.nth_column = std::nullopt;

  statistics.key = ARROW_STATISTICS_KEY_ROW_COUNT_EXACT;
  statistics.type = int64();
  statistics.value = ArrayStatistics::ValueType{int64_t{0}};
  if (num_rows.has_value()) {
    statistics.value = ArrayStatistics::ValueType{num_rows.value()};
  } else if (!extracted_array_data.empty()) {
    statistics.value =
        ArrayStatistics::ValueType{int64_t{extracted_array_data[0]->length}};
  }
  RETURN_NOT_OK(on_statistics(statistics));
  statistics.start_new_column = false;

  auto num_fields = static_cast<int64_t>(extracted_array_data.size());
  for (int64_t nth_column = 0; nth_column < num_fields; ++nth_column) {
    const auto& type = extracted_array_data[nth_column]->type;
    auto column_statistics = extracted_array_data[nth_column]->statistics;
    if (!column_statistics) {
      continue;
    }

    statistics.start_new_column = true;
    statistics.nth_column = static_cast<int32_t>(nth_column);
    if (column_statistics->null_count.has_value()) {
      statistics.nth_statistics++;
      statistics.key = ARROW_STATISTICS_KEY_NULL_COUNT_EXACT;
      statistics.type = int64();
      statistics.value = column_statistics->null_count.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->distinct_count.has_value()) {
      statistics.nth_statistics++;
      statistics.key = ARROW_STATISTICS_KEY_DISTINCT_COUNT_EXACT;
      statistics.type = int64();
      statistics.value = column_statistics->distinct_count.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->min.has_value()) {
      statistics.nth_statistics++;
      if (column_statistics->is_min_exact) {
        statistics.key = ARROW_STATISTICS_KEY_MIN_VALUE_EXACT;
      } else {
        statistics.key = ARROW_STATISTICS_KEY_MIN_VALUE_APPROXIMATE;
      }
      statistics.type = column_statistics->MinArrowType(type);
      statistics.value = column_statistics->min.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->max.has_value()) {
      statistics.nth_statistics++;
      if (column_statistics->is_max_exact) {
        statistics.key = ARROW_STATISTICS_KEY_MAX_VALUE_EXACT;
      } else {
        statistics.key = ARROW_STATISTICS_KEY_MAX_VALUE_APPROXIMATE;
      }
      statistics.type = column_statistics->MaxArrowType(type);
      statistics.value = column_statistics->max.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }
  }
  return Status::OK();
}
}  // namespace

Result<std::shared_ptr<Array>> MakeStatisticsArray(
    MemoryPool* memory_pool, const ArrayDataVector& extracted_array_data_vector,
    std::optional<int64_t> num_rows) {
  // Statistics schema:
  // struct<
  //   column: int32,
  //   statistics: map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  // >

  // Statistics schema doesn't define static dense union type for
  // values. Each statistics schema have a dense union type that has
  // needled value types. The following block collects these types.

  std::vector<std::shared_ptr<Field>> values_types;
  std::vector<int8_t> values_type_indexes;
  RETURN_NOT_OK(EnumerateStatistics(
      extracted_array_data_vector, num_rows, [&](const EnumeratedStatistics& statistics) {
        int8_t i = 0;
        for (const auto& field : values_types) {
          if (field->type()->Equals(statistics.type)) {
            break;
          }
          i++;
        }
        if (i == static_cast<int8_t>(values_types.size())) {
          values_types.push_back(field(statistics.type->name(), statistics.type));
        }
        values_type_indexes.push_back(i);
        return Status::OK();
      }));

  // statistics.key: dictionary<indices: int32, dictionary: utf8>
  auto keys_type = dictionary(int32(), utf8(), false);
  // statistics.items: dense_union<...all needed types...>
  auto values_type = dense_union(values_types);
  // struct<
  //   column: int32,
  //   statistics: map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  // >
  auto statistics_type =
      struct_({field("column", int32()),
               field("statistics", map(keys_type, values_type, false))});

  std::vector<std::shared_ptr<ArrayBuilder>> field_builders;
  // columns: int32
  auto columns_builder = std::make_shared<Int32Builder>(memory_pool);
  field_builders.push_back(std::static_pointer_cast<ArrayBuilder>(columns_builder));
  // statistics.key: dictionary<indices: int32, dictionary: utf8>
  auto keys_builder = std::make_shared<StringDictionary32Builder>();
  // statistics.items: dense_union<...all needed types...>
  std::vector<std::shared_ptr<ArrayBuilder>> values_builders;
  for (const auto& values_type : values_types) {
    std::unique_ptr<ArrayBuilder> values_builder;
    RETURN_NOT_OK(MakeBuilder(memory_pool, values_type->type(), &values_builder));
    values_builders.push_back(std::shared_ptr<ArrayBuilder>(std::move(values_builder)));
  }
  auto items_builder = std::make_shared<DenseUnionBuilder>(
      memory_pool, std::move(values_builders), values_type);
  // statistics:
  //   map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  auto values_builder = std::make_shared<MapBuilder>(
      memory_pool, std::static_pointer_cast<ArrayBuilder>(keys_builder),
      std::static_pointer_cast<ArrayBuilder>(items_builder));
  field_builders.push_back(std::static_pointer_cast<ArrayBuilder>(values_builder));
  // struct<
  //   column: int32,
  //   statistics: map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  // >
  StructBuilder builder(statistics_type, memory_pool, std::move(field_builders));

  // Append statistics.
  RETURN_NOT_OK(EnumerateStatistics(
      extracted_array_data_vector, num_rows, [&](const EnumeratedStatistics& statistics) {
        if (statistics.start_new_column) {
          RETURN_NOT_OK(builder.Append());
          if (statistics.nth_column.has_value()) {
            RETURN_NOT_OK(columns_builder->Append(statistics.nth_column.value()));
          } else {
            RETURN_NOT_OK(columns_builder->AppendNull());
          }
          RETURN_NOT_OK(values_builder->Append());
        }
        RETURN_NOT_OK(keys_builder->Append(statistics.key,
                                           static_cast<int32_t>(strlen(statistics.key))));
        const auto values_type_index = values_type_indexes[statistics.nth_statistics];
        RETURN_NOT_OK(items_builder->Append(values_type_index));
        struct Visitor {
          ArrayBuilder* builder;

          Status operator()(const bool& value) {
            return static_cast<BooleanBuilder*>(builder)->Append(value);
          }
          Status operator()(const int64_t& value) {
            return static_cast<Int64Builder*>(builder)->Append(value);
          }
          Status operator()(const uint64_t& value) {
            return static_cast<UInt64Builder*>(builder)->Append(value);
          }
          Status operator()(const double& value) {
            return static_cast<DoubleBuilder*>(builder)->Append(value);
          }
          Status operator()(const std::string& value) {
            return static_cast<StringBuilder*>(builder)->Append(
                value.data(), static_cast<int32_t>(value.size()));
          }
          Status operator()(const std::shared_ptr<Scalar>& value) {
            return builder->AppendScalar(*value);
          }
        } visitor;
        visitor.builder = values_builders[values_type_index].get();
        RETURN_NOT_OK(std::visit(visitor, statistics.value));
        return Status::OK();
      }));

  return builder.Finish();
}

}  // namespace internal
}  // namespace arrow
