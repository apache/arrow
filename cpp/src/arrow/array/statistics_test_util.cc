
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

#include "arrow/array/statistics_test_util.h"

#include <arrow/record_batch.h>

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/statistics.h"
#include "arrow/array/statistics_option.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace {
template <typename ArrowType,
          typename = std::enable_if_t<is_boolean_type<ArrowType>::value ||
                                      is_number_type<ArrowType>::value>>
Result<std::shared_ptr<Array>> BuildArray(
    const std::vector<typename TypeTraits<ArrowType>::CType>& values) {
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;
  BuilderType builder;
  for (const auto& value : values) {
    ARROW_RETURN_NOT_OK(builder.Append(value));
  }
  return builder.Finish();
}

template <typename ArrowType, typename = enable_if_string<ArrowType>>
Result<std::shared_ptr<Array>> BuildArray(const std::vector<std::string>& values) {
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;
  BuilderType builder;
  for (const auto& value : values) {
    ARROW_RETURN_NOT_OK(builder.Append(value));
  }
  return builder.Finish();
}

template <typename RawType>
std::vector<RawType> StatisticsValuesToRawValues(
    const std::vector<ArrayStatistics::ValueType>& values) {
  std::vector<RawType> raw_values;
  for (const auto& value : values) {
    raw_values.push_back(std::get<RawType>(value));
  }
  return raw_values;
}

template <typename ValueType, typename = std::enable_if_t<std::is_same<
                                  ArrayStatistics::ValueType, ValueType>::value>>
Result<std::shared_ptr<Array>> BuildArray(const std::vector<ValueType>& values) {
  struct Builder {
    const std::vector<ArrayStatistics::ValueType>& values_;
    explicit Builder(const std::vector<ArrayStatistics::ValueType>& values)
        : values_(values) {}

    Result<std::shared_ptr<Array>> operator()(const bool&) {
      auto values = StatisticsValuesToRawValues<bool>(values_);
      return BuildArray<BooleanType>(values);
    }
    Result<std::shared_ptr<Array>> operator()(const int64_t&) {
      auto values = StatisticsValuesToRawValues<int64_t>(values_);
      return BuildArray<Int64Type>(values);
    }
    Result<std::shared_ptr<Array>> operator()(const uint64_t&) {
      auto values = StatisticsValuesToRawValues<uint64_t>(values_);
      return BuildArray<UInt64Type>(values);
    }
    Result<std::shared_ptr<Array>> operator()(const double&) {
      auto values = StatisticsValuesToRawValues<double>(values_);
      return BuildArray<DoubleType>(values);
    }
    Result<std::shared_ptr<Array>> operator()(const std::string&) {
      auto values = StatisticsValuesToRawValues<std::string>(values_);
      return BuildArray<StringType>(values);
    }
    Result<std::shared_ptr<Array>> operator()(const std::shared_ptr<Scalar>& scalar) {
      auto values = StatisticsValuesToRawValues<std::shared_ptr<Scalar>>(values_);
      ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(scalar->type));
      ARROW_RETURN_NOT_OK(builder->Reserve(values.size()));
      ARROW_RETURN_NOT_OK(builder->AppendScalars(values));
      return builder->Finish();
    }
  } builder(values);
  return std::visit(builder, values[0]);
}
}  // namespace
namespace test {
Result<std::shared_ptr<Array>> MakeMockStatisticsArray(
    const std::string& columns_json,
    const std::vector<std::vector<std::string>>& nested_statistics_keys,
    const std::vector<std::vector<ArrayStatistics::ValueType>>&
        nested_statistics_values) {
  const auto& columns_type = int32();
  auto columns_array = ArrayFromJSON(columns_type, columns_json);
  const auto n_columns = columns_array->length();

  // nested_statistics_keys:
  //   {
  //     {"ARROW:row_count:exact", "ARROW:null_count:exact"},
  //     {"ARROW:max_value:exact"},
  //     {"ARROW:max_value:exact", "ARROW:distinct_count:exact"},
  //   }
  // nested_statistics_values:
  //   {
  //     {int64_t{29}, int64_t{1}},
  //     {double{2.9}},
  //     {double{-2.9}, int64_t{2}},
  //   }
  // ->
  // keys_dictionary:
  //   {
  //     "ARROW:row_count:exact",
  //     "ARROW:null_count:exact",
  //     "ARROW:max_value:exact",
  //     "ARROW:distinct_count:exact",
  //   }
  // keys_indices: {0, 1, 2, 2, 3}
  // values_types: {int64(), float64()}
  // values_type_codes: {0, 1}
  // values_values[0]: {int64_t{29}, int64_t{1}, int64_t{2}}
  // values_values[1]: {double{2.9}, double{-2.9}}
  // values_value_type_ids: {0, 0, 1, 1, 0}
  // values_value_offsets: {0, 1, 0, 1, 2}
  // statistics_offsets: {0, 2, 3, 5, 5}
  std::vector<std::string> keys_dictionary;
  std::vector<int32_t> keys_indices;
  std::vector<std::shared_ptr<DataType>> values_types;
  std::vector<int8_t> values_type_codes;
  std::vector<std::vector<ArrayStatistics::ValueType>> values_values;
  std::vector<int8_t> values_value_type_ids;
  std::vector<int32_t> values_value_offsets;
  std::vector<int32_t> statistics_offsets;

  int32_t offset = 0;
  std::vector<int32_t> values_value_offset_counters;
  for (size_t i = 0; i < nested_statistics_keys.size(); ++i) {
    const auto& statistics_keys = nested_statistics_keys[i];
    const auto& statistics_values = nested_statistics_values[i];
    statistics_offsets.push_back(offset);
    for (size_t j = 0; j < statistics_keys.size(); ++j) {
      const auto& key = statistics_keys[j];
      const auto& value = statistics_values[j];
      ++offset;

      int32_t key_index = 0;
      for (; key_index < static_cast<int32_t>(keys_dictionary.size()); ++key_index) {
        if (keys_dictionary[key_index] == key) {
          break;
        }
      }
      if (key_index == static_cast<int32_t>(keys_dictionary.size())) {
        keys_dictionary.push_back(key);
      }
      keys_indices.push_back(key_index);

      auto values_type = ArrayStatistics::ValueToArrowType(value, arrow::null());
      int8_t values_type_code = 0;
      for (; values_type_code < static_cast<int32_t>(values_types.size());
           ++values_type_code) {
        if (values_types[values_type_code]->Equals(values_type)) {
          break;
        }
      }
      if (values_type_code == static_cast<int32_t>(values_types.size())) {
        values_types.push_back(values_type);
        values_type_codes.push_back(values_type_code);
        values_values.emplace_back();
        values_value_offset_counters.push_back(0);
      }
      values_values[values_type_code].push_back(value);
      values_value_type_ids.push_back(values_type_code);
      values_value_offsets.push_back(values_value_offset_counters[values_type_code]++);
    }
  }
  statistics_offsets.push_back(offset);

  auto keys_type = dictionary(int32(), utf8(), false);
  std::vector<std::shared_ptr<Field>> values_fields;
  for (const auto& type : values_types) {
    values_fields.push_back(field(type->name(), type));
  }
  auto values_type = dense_union(values_fields);
  auto statistics_type = map(keys_type, values_type, false);
  auto struct_type =
      struct_({field("column", columns_type), field("statistics", statistics_type)});

  ARROW_ASSIGN_OR_RAISE(auto keys_indices_array, BuildArray<Int32Type>(keys_indices));
  ARROW_ASSIGN_OR_RAISE(auto keys_dictionary_array,
                        BuildArray<StringType>(keys_dictionary));
  ARROW_ASSIGN_OR_RAISE(
      auto keys_array,
      DictionaryArray::FromArrays(keys_type, keys_indices_array, keys_dictionary_array));

  std::vector<std::shared_ptr<Array>> values_arrays;
  for (const auto& values : values_values) {
    ARROW_ASSIGN_OR_RAISE(auto values_array,
                          BuildArray<ArrayStatistics::ValueType>(values));
    values_arrays.push_back(values_array);
  }
  ARROW_ASSIGN_OR_RAISE(auto values_value_type_ids_array,
                        BuildArray<Int8Type>(values_value_type_ids));
  ARROW_ASSIGN_OR_RAISE(auto values_value_offsets_array,
                        BuildArray<Int32Type>(values_value_offsets));
  auto values_array = std::make_shared<DenseUnionArray>(
      values_type, values_value_offsets_array->length(), values_arrays,
      values_value_type_ids_array->data()->buffers[1],
      values_value_offsets_array->data()->buffers[1]);
  ARROW_ASSIGN_OR_RAISE(auto statistics_offsets_array,
                        BuildArray<Int32Type>(statistics_offsets));
  ARROW_ASSIGN_OR_RAISE(auto statistics_array,
                        MapArray::FromArrays(statistics_type, statistics_offsets_array,
                                             keys_array, values_array));
  std::vector<std::shared_ptr<Array>> struct_arrays = {std::move(columns_array),
                                                       std::move(statistics_array)};
  return std::make_shared<StructArray>(struct_type, n_columns, struct_arrays);
}

Result<std::shared_ptr<Array>> MakeNestedStruct(const int32_t depth) {
  // each struct occupy two depth
  if (depth < 2) {
    return Status::Invalid("depth must be greater thant 1");
  }
  auto parent_struct_type =
      struct_({field("a", int64()), field("b", int32()), field("c", int16())});
  auto parent_struct =
      ArrayFromJSON(parent_struct_type,
                    R"([{"a":1,"b":4,"c":7},{"a":2,"b":5,"c":8},{"a":3,"b":6,"c":9}])");
  auto child_struct =
      ArrayFromJSON(parent_struct_type,
                    R"([{"a":1,"b":4,"c":7},{"a":2,"b":5,"c":8},{"a":3,"b":6,"c":9}])");

  for (int i = 0; i < depth - 2; ++i) {
    ARROW_ASSIGN_OR_RAISE(
        parent_struct, StructArray::Make({parent_struct, child_struct},
                                         {field("struct", parent_struct->type()),
                                          field("child_struct", child_struct->type())}));
  }
  return parent_struct;
}
Status CheckDepth(
    std::variant<std::shared_ptr<RecordBatch>, std::shared_ptr<Array>> value,
    const StatisticsArrayTOptions& options) {
  struct {
    StatisticsArrayTOptions options;
    Status operator()(const std::shared_ptr<RecordBatch>& batch) const {
      return batch->MakeStatisticsArray(options).status();
    }
    Status operator()(const std::shared_ptr<Array>& array) const {
      // TODO  Lack of method for creating statistics array for Array(GH-45804)
      return Status::NotImplemented("MakeStatisticsArray not implemented for Array");
    }
  } visitor;
  visitor.options = options;
  return std::visit(visitor, value);
}
};  // namespace test
}  // namespace arrow
