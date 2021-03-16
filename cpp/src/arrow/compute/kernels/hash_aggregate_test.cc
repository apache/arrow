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

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/hash_aggregate_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/registry.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_internal.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::BitmapReader;
using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

// Copy-pasta from partition.cc
//
// In the finished product this will only be a test helper for group_by
// and partition.cc will rely on a no-aggregate call to group_by.
namespace group_helpers {
namespace {

// Transform an array of counts to offsets which will divide a ListArray
// into an equal number of slices with corresponding lengths.
Result<std::shared_ptr<Buffer>> CountsToOffsets(std::shared_ptr<Int64Array> counts) {
  TypedBufferBuilder<int32_t> offset_builder;
  RETURN_NOT_OK(offset_builder.Resize(counts->length() + 1));

  int32_t current_offset = 0;
  offset_builder.UnsafeAppend(current_offset);

  for (int64_t i = 0; i < counts->length(); ++i) {
    DCHECK_NE(counts->Value(i), 0);
    current_offset += static_cast<int32_t>(counts->Value(i));
    offset_builder.UnsafeAppend(current_offset);
  }

  std::shared_ptr<Buffer> offsets;
  RETURN_NOT_OK(offset_builder.Finish(&offsets));
  return offsets;
}

class StructDictionary {
 public:
  struct Encoded {
    std::shared_ptr<Int32Array> indices;
    std::shared_ptr<StructDictionary> dictionary;
  };

  static Result<Encoded> Encode(const ArrayVector& columns) {
    Encoded out{nullptr, std::make_shared<StructDictionary>()};

    for (const auto& column : columns) {
      if (column->null_count() != 0) {
        return Status::NotImplemented("Grouping on a field with nulls");
      }

      RETURN_NOT_OK(out.dictionary->AddOne(column, &out.indices));
    }

    return out;
  }

  Result<std::shared_ptr<StructArray>> Decode(std::shared_ptr<Int32Array> fused_indices,
                                              FieldVector fields) {
    std::vector<Int32Builder> builders(dictionaries_.size());
    for (Int32Builder& b : builders) {
      RETURN_NOT_OK(b.Resize(fused_indices->length()));
    }

    std::vector<int32_t> codes(dictionaries_.size());
    for (int64_t i = 0; i < fused_indices->length(); ++i) {
      Expand(fused_indices->Value(i), codes.data());

      auto builder_it = builders.begin();
      for (int32_t index : codes) {
        builder_it++->UnsafeAppend(index);
      }
    }

    ArrayVector columns(dictionaries_.size());
    for (size_t i = 0; i < dictionaries_.size(); ++i) {
      std::shared_ptr<ArrayData> indices;
      RETURN_NOT_OK(builders[i].FinishInternal(&indices));

      ARROW_ASSIGN_OR_RAISE(Datum column, compute::Take(dictionaries_[i], indices));

      if (fields[i]->type()->id() == Type::DICTIONARY) {
        RETURN_NOT_OK(RestoreDictionaryEncoding(
            checked_pointer_cast<DictionaryType>(fields[i]->type()), &column));
      }

      columns[i] = column.make_array();
    }

    return StructArray::Make(std::move(columns), std::move(fields));
  }

 private:
  Status AddOne(Datum column, std::shared_ptr<Int32Array>* fused_indices) {
    if (column.type()->id() != Type::DICTIONARY) {
      ARROW_ASSIGN_OR_RAISE(column, compute::DictionaryEncode(std::move(column)));
    }

    auto dict_column = column.array_as<DictionaryArray>();
    dictionaries_.push_back(dict_column->dictionary());
    ARROW_ASSIGN_OR_RAISE(auto indices, compute::Cast(*dict_column->indices(), int32()));

    if (*fused_indices == nullptr) {
      *fused_indices = checked_pointer_cast<Int32Array>(std::move(indices));
      return IncreaseSize();
    }

    // It's useful to think about the case where each of dictionaries_ has size 10.
    // In this case the decimal digit in the ones place is the code in dictionaries_[0],
    // the tens place corresponds to the code in dictionaries_[1], etc.
    // The incumbent indices must be shifted to the hundreds place so as not to collide.
    ARROW_ASSIGN_OR_RAISE(Datum new_fused_indices,
                          compute::Multiply(indices, MakeScalar(size_)));

    ARROW_ASSIGN_OR_RAISE(new_fused_indices,
                          compute::Add(new_fused_indices, *fused_indices));

    *fused_indices = checked_pointer_cast<Int32Array>(new_fused_indices.make_array());
    return IncreaseSize();
  }

  // expand a fused code into component dict codes, order is in order of addition
  void Expand(int32_t fused_code, int32_t* codes) {
    for (size_t i = 0; i < dictionaries_.size(); ++i) {
      auto dictionary_size = static_cast<int32_t>(dictionaries_[i]->length());
      codes[i] = fused_code % dictionary_size;
      fused_code /= dictionary_size;
    }
  }

  Status RestoreDictionaryEncoding(std::shared_ptr<DictionaryType> expected_type,
                                   Datum* column) {
    DCHECK_NE(column->type()->id(), Type::DICTIONARY);
    ARROW_ASSIGN_OR_RAISE(*column, compute::DictionaryEncode(std::move(*column)));

    if (expected_type->index_type()->id() == Type::INT32) {
      // dictionary_encode has already yielded the expected index_type
      return Status::OK();
    }

    // cast the indices to the expected index type
    auto dictionary = std::move(column->mutable_array()->dictionary);
    column->mutable_array()->type = int32();

    ARROW_ASSIGN_OR_RAISE(*column,
                          compute::Cast(std::move(*column), expected_type->index_type()));

    column->mutable_array()->dictionary = std::move(dictionary);
    column->mutable_array()->type = expected_type;
    return Status::OK();
  }

  Status IncreaseSize() {
    auto factor = static_cast<int32_t>(dictionaries_.back()->length());

    if (arrow::internal::MultiplyWithOverflow(size_, factor, &size_)) {
      return Status::CapacityError("Max groups exceeded");
    }
    return Status::OK();
  }

  int32_t size_ = 1;
  ArrayVector dictionaries_;
};

Result<std::shared_ptr<StructArray>> MakeGroupings(const StructArray& keys) {
  if (keys.num_fields() == 0) {
    return Status::Invalid("Grouping with no keys");
  }

  if (keys.null_count() != 0) {
    return Status::Invalid("Grouping with null keys");
  }

  ARROW_ASSIGN_OR_RAISE(auto fused, StructDictionary::Encode(keys.fields()));

  ARROW_ASSIGN_OR_RAISE(auto sort_indices, compute::SortIndices(*fused.indices));
  ARROW_ASSIGN_OR_RAISE(Datum sorted, compute::Take(fused.indices, *sort_indices));
  fused.indices = checked_pointer_cast<Int32Array>(sorted.make_array());

  ARROW_ASSIGN_OR_RAISE(auto fused_counts_and_values,
                        compute::ValueCounts(fused.indices));
  fused.indices.reset();

  auto unique_fused_indices =
      checked_pointer_cast<Int32Array>(fused_counts_and_values->GetFieldByName("values"));
  ARROW_ASSIGN_OR_RAISE(
      auto unique_rows,
      fused.dictionary->Decode(std::move(unique_fused_indices), keys.type()->fields()));

  auto counts =
      checked_pointer_cast<Int64Array>(fused_counts_and_values->GetFieldByName("counts"));
  ARROW_ASSIGN_OR_RAISE(auto offsets, CountsToOffsets(std::move(counts)));

  auto grouped_sort_indices =
      std::make_shared<ListArray>(list(sort_indices->type()), unique_rows->length(),
                                  std::move(offsets), std::move(sort_indices));

  return StructArray::Make(
      ArrayVector{std::move(unique_rows), std::move(grouped_sort_indices)},
      std::vector<std::string>{"values", "groupings"});
}

Result<std::shared_ptr<ListArray>> ApplyGroupings(const ListArray& groupings,
                                                  const Array& array) {
  ARROW_ASSIGN_OR_RAISE(Datum sorted,
                        compute::Take(array, groupings.data()->child_data[0]));

  return std::make_shared<ListArray>(list(array.type()), groupings.length(),
                                     groupings.value_offsets(), sorted.make_array());
}

struct ScalarVectorToArray {
  template <typename T, typename AppendScalar,
            typename BuilderType = typename TypeTraits<T>::BuilderType,
            typename ScalarType = typename TypeTraits<T>::ScalarType>
  Status UseBuilder(const AppendScalar& append) {
    BuilderType builder(type(), default_memory_pool());
    for (const auto& s : scalars_) {
      if (s->is_valid) {
        RETURN_NOT_OK(append(checked_cast<const ScalarType&>(*s), &builder));
      } else {
        RETURN_NOT_OK(builder.AppendNull());
      }
    }
    return builder.FinishInternal(&data_);
  }

  struct AppendValue {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      return builder->Append(s.value);
    }
  };

  struct AppendBuffer {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      const Buffer& buffer = *s.value;
      return builder->Append(util::string_view{buffer});
    }
  };

  template <typename T>
  enable_if_primitive_ctype<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendValue{});
  }

  template <typename T>
  enable_if_has_string_view<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendBuffer{});
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("ScalarVectorToArray for type ", type);
  }

  Result<Datum> Convert(ScalarVector scalars) && {
    if (scalars.size() == 0) {
      return Status::NotImplemented("ScalarVectorToArray with no scalars");
    }
    scalars_ = std::move(scalars);
    RETURN_NOT_OK(VisitTypeInline(*type(), this));
    return Datum(std::move(data_));
  }

  const std::shared_ptr<DataType>& type() { return scalars_[0]->type; }

  ScalarVector scalars_;
  std::shared_ptr<ArrayData> data_;
};

Result<Datum> NaiveGroupBy(std::vector<Datum> aggregands, std::vector<Datum> keys,
                           GroupByOptions options) {
  ArrayVector keys_arrays;
  for (const Datum& key : keys) keys_arrays.push_back(key.make_array());
  std::vector<std::string> key_names(keys_arrays.size(), "");
  ARROW_ASSIGN_OR_RAISE(auto keys_struct,
                        StructArray::Make(std::move(keys_arrays), std::move(key_names)));

  ARROW_ASSIGN_OR_RAISE(auto groupings_and_values, MakeGroupings(*keys_struct));

  auto groupings =
      checked_pointer_cast<ListArray>(groupings_and_values->GetFieldByName("groupings"));

  int64_t n_groups = groupings->length();

  ArrayVector out_columns;

  for (size_t i_agg = 0; i_agg < aggregands.size(); ++i_agg) {
    const Datum& aggregand = aggregands[i_agg];
    const std::string& function = options.aggregates[i_agg].function;

    ScalarVector aggregated_scalars;

    ARROW_ASSIGN_OR_RAISE(auto grouped_aggregand,
                          ApplyGroupings(*groupings, *aggregand.make_array()));

    for (int64_t i_group = 0; i_group < n_groups; ++i_group) {
      ARROW_ASSIGN_OR_RAISE(
          Datum d, CallFunction(function, {grouped_aggregand->value_slice(i_group)}));
      aggregated_scalars.push_back(d.scalar());
    }

    ARROW_ASSIGN_OR_RAISE(Datum aggregated_column,
                          ScalarVectorToArray{}.Convert(std::move(aggregated_scalars)));
    out_columns.push_back(aggregated_column.make_array());
  }

  keys_struct =
      checked_pointer_cast<StructArray>(groupings_and_values->GetFieldByName("values"));
  for (size_t i_key = 0; i_key < aggregands.size(); ++i_key) {
    out_columns.push_back(keys_struct->field(i_key));
  }

  std::vector<std::string> out_names(out_columns.size(), "");
  return StructArray::Make(std::move(out_columns), std::move(out_names));
}

void ValidateGroupBy(GroupByOptions options, std::vector<Datum> aggregands,
                     std::vector<Datum> keys) {
  ASSERT_OK_AND_ASSIGN(Datum expected,
                       group_helpers::NaiveGroupBy(aggregands, keys, options));

  ASSERT_OK_AND_ASSIGN(Datum actual, GroupBy(aggregands, keys, options));

  // Ordering of groups is not important, so sort by key columns to ensure the comparison
  // doesn't fail spuriously

  for (Datum* out : {&expected, &actual}) {
    auto out_columns = out->array_as<StructArray>()->fields();

    SortOptions sort_options;
    FieldVector key_fields;
    ArrayVector key_columns;
    for (size_t i = 0; i < keys.size(); ++i) {
      auto name = std::to_string(i);
      sort_options.sort_keys.emplace_back(name);
      key_fields.push_back(field(name, out_columns[0]->type()));
      key_columns.push_back(out_columns[0]);
    }
    auto key_batch = RecordBatch::Make(schema(std::move(key_fields)), out->length(),
                                       std::move(key_columns));

    ASSERT_OK_AND_ASSIGN(Datum sort_indices, SortIndices(key_batch, sort_options));
    ASSERT_OK_AND_ASSIGN(*out, Take(*out, sort_indices, TakeOptions::NoBoundsCheck()));
  }

  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

}  // namespace
}  // namespace group_helpers

TEST(GroupBy, SumOnly8bitKey) {
  auto aggregand = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int8(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped, GroupBy({aggregand}, {key},
                                                             GroupByOptions{
                                                                 {"sum", nullptr},
                                                             }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int8()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [0.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, SumOnly32bitKey) {
  auto aggregand = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int32(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped, GroupBy({aggregand}, {key},
                                                             GroupByOptions{
                                                                 {"sum", nullptr},
                                                             }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int32()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [0.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, SumOnly) {
  auto aggregand = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int64(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped, GroupBy({aggregand}, {key},
                                                             GroupByOptions{
                                                                 {"sum", nullptr},
                                                             }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int64()),
                                  }),
                                  R"([
    [4.25,   1],
    [-0.125, 2],
    [null,   3],
    [0.75,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, MinMaxOnly) {
  auto aggregand = ArrayFromJSON(float64(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int64(), "[1, 2, 3, 1, 2, 2, null]");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped, GroupBy({aggregand}, {key},
                                                             GroupByOptions{
                                                                 {"min_max", nullptr},
                                                             }));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", struct_({
                                                    field("min", float64()),
                                                    field("max", float64()),
                                                })),
                                      field("", int64()),
                                  }),
                                  R"([
    [{"min": 1.0,   "max": 3.25},  1],
    [{"min": -0.25, "max": 0.125}, 2],
    [{"min": null,  "max": null},  3],
    [{"min": 0.75,  "max": 0.75},  null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, CountAndSum) {
  auto aggregand = ArrayFromJSON(float32(), "[1.0, 0.0, null, 3.25, 0.125, -0.25, 0.75]");
  auto key = ArrayFromJSON(int64(), "[1, 2, 1, 3, 2, 3, null]");

  CountOptions count_options;

  ASSERT_OK_AND_ASSIGN(
      Datum aggregated_and_grouped,
      // NB: passing an aggregand twice or also using it as a key is legal
      GroupBy({aggregand, aggregand, key}, {key},
              GroupByOptions{
                  {"count", &count_options},
                  {"sum", nullptr},
                  {"sum", nullptr},
              }));

  AssertDatumsEqual(
      ArrayFromJSON(struct_({
                        field("", int64()),
                        // NB: summing a float32 array results in float64 sums
                        field("", float64()),
                        field("", int64()),
                        field("", int64()),
                    }),
                    R"([
    [1, 1.0,   2,    1],
    [2, 0.125, 4,    2],
    [2, 3.0,   6,    3],
    [1, 0.75,  null, null]
  ])"),
      aggregated_and_grouped,
      /*verbose=*/true);
}

TEST(GroupBy, StringKey) {
  auto aggregand = ArrayFromJSON(int64(), "[10, 5, 4, 2, 12, 9]");
  auto key = ArrayFromJSON(utf8(), R"(["alfa", "beta", "gamma", "gamma", null, "beta"])");

  ASSERT_OK_AND_ASSIGN(Datum aggregated_and_grouped,
                       GroupBy({aggregand}, {key}, GroupByOptions{{"sum", nullptr}}));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", int64()),
                                      field("", utf8()),
                                  }),
                                  R"([
    [10,   "alfa"],
    [14,   "beta"],
    [6,    "gamma"],
    [12,   null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, MultipleKeys) {
  auto aggregand = ArrayFromJSON(float32(), "[0.125, 0.5, -0.75, 8, 1.0, 2.0]");
  auto int_key = ArrayFromJSON(int32(), "[0, 1, 0, 1, 0, 1]");
  auto str_key =
      ArrayFromJSON(utf8(), R"(["beta", "beta", "gamma", "gamma", null, "beta"])");

  ASSERT_OK_AND_ASSIGN(
      Datum aggregated_and_grouped,
      GroupBy({aggregand}, {int_key, str_key}, GroupByOptions{{"sum", nullptr}}));

  AssertDatumsEqual(ArrayFromJSON(struct_({
                                      field("", float64()),
                                      field("", int32()),
                                      field("", utf8()),
                                  }),
                                  R"([
    [0.125, 0, "beta"],
    [2.5,   1, "beta"],
    [-0.75, 0, "gamma"],
    [8,     1, "gamma"],
    [1.0,   0, null]
  ])"),
                    aggregated_and_grouped,
                    /*verbose=*/true);
}

TEST(GroupBy, ConcreteCaseWithValidateGroupBy) {
  auto aggregand = ArrayFromJSON(int64(), "[10, 5, 4, 2, 12]");
  auto key = ArrayFromJSON(utf8(), R"(["alfa", "beta", "gamma", "gamma", "beta"])");

  group_helpers::ValidateGroupBy(GroupByOptions{{"sum", nullptr}}, {aggregand}, {key});
}

TEST(GroupBy, RandomArraySum) {
  auto rand = random::RandomArrayGenerator(0xdeadbeef);

  for (size_t i = 3; i < 14; i += 2) {
    for (auto null_probability : {0.0, 0.001, 0.1, 0.5, 0.999, 1.0}) {
      int64_t length = 1UL << i;
      auto summand = rand.Float32(length, -100, 100, null_probability);
      auto key = rand.Int64(length, 0, 12);

      group_helpers::ValidateGroupBy(
          GroupByOptions{
              {"sum", nullptr},
          },
          {summand}, {key});
    }
  }
}

}  // namespace compute
}  // namespace arrow

