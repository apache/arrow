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
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/list_util.h"
#include "arrow/util/unreachable.h"

namespace arrow {

class SimpleRandomArrayGenerator {
 private:
  random::SeedType seed_ = 0xdeadbeef;
  std::default_random_engine random_engine_;
  random::RandomArrayGenerator rag_;

 public:
  SimpleRandomArrayGenerator() : random_engine_(seed_), rag_(seed_) {}

  template <typename offset_type>
  std::vector<offset_type> RandomOffsetsInRange(offset_type min_offset,
                                                offset_type max_offset,
                                                int64_t num_offsets) {
    std::vector<offset_type> offsets(static_cast<std::size_t>(num_offsets));
    std::uniform_int_distribution<offset_type> dist(min_offset, max_offset);
    std::generate(offsets.begin(), offsets.end(), [&] { return dist(random_engine_); });
    return offsets;
  }

  template <typename offset_type>
  std::vector<offset_type> Offsets(int32_t values_length, int32_t slice_count) {
    auto offsets = RandomOffsetsInRange<offset_type>(0, values_length, slice_count + 1);
    std::sort(offsets.begin(), offsets.end());
    return offsets;
  }

  /// \param[in] random_offsets Random offsets in [0, values_size] and no particular order
  template <typename offset_type>
  std::vector<offset_type> ListViewSizes(const std::vector<offset_type>& random_offsets,
                                         int64_t values_size, double avg_size,
                                         int64_t num_sizes) {
    std::normal_distribution<double> normal(/*mean=*/avg_size, /*stddev=*/3.0);
    std::vector<offset_type> sizes;
    sizes.reserve(num_sizes);
    for (int64_t i = 0; i < num_sizes; ++i) {
      const auto sampled_size = std::llround(normal(random_engine_));
      auto size = std::max<offset_type>(0, static_cast<offset_type>(sampled_size));
      if (random_offsets[i] > values_size - size) {
        size = static_cast<offset_type>(values_size - random_offsets[i]);
      }
      sizes.push_back(size);
    }
    return sizes;
  }

  ArrayVector Slices(const std::shared_ptr<Array>& array,
                     const std::vector<int32_t>& offsets) {
    ArrayVector slices(offsets.size() - 1);
    for (size_t i = 0; i != slices.size(); ++i) {
      slices[i] = array->Slice(offsets[i], offsets[i + 1] - offsets[i]);
    }
    return slices;
  }

  std::shared_ptr<Buffer> ValidityBitmap(int64_t size, double null_probability) {
    return rag_.NullBitmap(size, null_probability, kDefaultBufferAlignment,
                           default_memory_pool());
  }

  template <typename PrimitiveType>
  std::shared_ptr<Array> PrimitiveArray(int64_t size, double null_probability) {
    if (std::is_same<PrimitiveType, BooleanType>::value) {
      return rag_.Boolean(size, 0.5, null_probability);
    }
    return rag_.Numeric<PrimitiveType, uint8_t>(size, 0, 127, null_probability);
  }

  std::shared_ptr<Array> StringArray(int64_t size, double null_probability) {
    return rag_.String(size, /*min_length =*/0, /*max_length =*/15, null_probability);
  }

  std::shared_ptr<Array> LargeStringArray(int64_t size, double null_probability) {
    return rag_.LargeString(size, /*min_length =*/0, /*max_length =*/15,
                            null_probability);
  }

  std::shared_ptr<Array> StringViewArray(int64_t size, double null_probability) {
    return rag_.StringView(size, /*min_length =*/0, /*max_length =*/40, null_probability,
                           /*max_buffer_length=*/200);
  }

  std::shared_ptr<Array> ArrayOf(std::shared_ptr<DataType> type, int64_t size,
                                 double null_probability) {
    return rag_.ArrayOf(std::move(type), size, null_probability);
  }

  // TODO(GH-38656): Use the random array generators from testing/random.h here

  template <typename ListType,
            typename ListArrayType = typename TypeTraits<ListType>::ArrayType>
  Result<std::shared_ptr<ListArrayType>> ListArray(int32_t length,
                                                   double null_probability) {
    using offset_type = typename ListType::offset_type;
    using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;

    auto values_size = length * 4;
    auto values = PrimitiveArray<Int8Type>(values_size, null_probability);
    auto offsets_vector = Offsets<offset_type>(values_size, length);
    // Ensure first and last offsets encompass the whole values array
    offsets_vector.front() = 0;
    offsets_vector.back() = static_cast<offset_type>(values_size);
    std::shared_ptr<Array> offsets;
    ArrayFromVector<OffsetArrowType>(offsets_vector, &offsets);
    return ListArrayType::FromArrays(*offsets, *values);
  }

  template <typename ListViewType,
            typename ListViewArrayType = typename TypeTraits<ListViewType>::ArrayType>
  Result<std::shared_ptr<ListViewArrayType>> ListViewArray(int32_t length,
                                                           double null_probability) {
    using offset_type = typename ListViewType::offset_type;
    using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;

    constexpr int kAvgListViewSize = 4;
    auto values_size = kAvgListViewSize * length;

    auto values = PrimitiveArray<Int8Type>(values_size, null_probability);

    std::shared_ptr<Array> offsets;
    auto offsets_vector = RandomOffsetsInRange<offset_type>(0, values_size, length);
    ArrayFromVector<OffsetArrowType>(offsets_vector, &offsets);

    std::shared_ptr<Array> sizes;
    auto sizes_vector =
        ListViewSizes<offset_type>(offsets_vector, values_size, kAvgListViewSize, length);
    ArrayFromVector<OffsetArrowType>(sizes_vector, &sizes);

    auto validity_bitmap = ValidityBitmap(length, null_probability);
    auto valid_count = internal::CountSetBits(validity_bitmap->data(), 0, length);

    return ListViewArrayType::FromArrays(
        *offsets, *sizes, *values, default_memory_pool(),
        valid_count == length ? nullptr : std::move(validity_bitmap));
  }
};

class ConcatenateTest : public ::testing::Test {
 private:
  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;

 protected:
  SimpleRandomArrayGenerator rag;

  ConcatenateTest()
      : sizes_({0, 1, 2, 4, 16, 31, 1234}),
        null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  void CheckTrailingBitsAreZeroed(const std::shared_ptr<Buffer>& bitmap, int64_t length) {
    if (auto preceding_bits = bit_util::kPrecedingBitmask[length % 8]) {
      auto last_byte = bitmap->data()[length / 8];
      ASSERT_EQ(static_cast<uint8_t>(last_byte & preceding_bits), last_byte)
          << length << " " << static_cast<int>(preceding_bits);
    }
  }

  template <typename ArrayFactory>
  void Check(ArrayFactory&& factory) {
    for (auto size : this->sizes_) {
      auto offsets = rag.Offsets<int32_t>(size, 3);
      for (auto null_probability : this->null_probabilities_) {
        std::shared_ptr<Array> array;
        factory(size, null_probability, &array);
        ASSERT_OK(array->ValidateFull());
        auto expected = array->Slice(offsets.front(), offsets.back() - offsets.front());
        ASSERT_OK(expected->ValidateFull());
        auto slices = rag.Slices(array, offsets);
        for (auto slice : slices) {
          ASSERT_OK(slice->ValidateFull());
        }
        ASSERT_OK_AND_ASSIGN(auto actual, Concatenate(slices));
        ASSERT_OK(actual->ValidateFull());
        AssertArraysEqual(*expected, *actual);
        if (actual->data()->buffers[0]) {
          CheckTrailingBitsAreZeroed(actual->data()->buffers[0], actual->length());
        }
        if (actual->type_id() == Type::BOOL) {
          CheckTrailingBitsAreZeroed(actual->data()->buffers[1], actual->length());
        }
      }
    }
  }
};

TEST(ConcatenateEmptyArraysTest, TestValueBuffersNullPtr) {
  ArrayVector inputs;

  std::shared_ptr<Array> binary_array;
  BinaryBuilder builder;
  ASSERT_OK(builder.Finish(&binary_array));
  inputs.push_back(std::move(binary_array));

  builder.Reset();
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.Finish(&binary_array));
  inputs.push_back(std::move(binary_array));

  ASSERT_OK_AND_ASSIGN(auto actual, Concatenate(inputs));
  AssertArraysEqual(*actual, *inputs[1]);
}

template <typename PrimitiveType>
class PrimitiveConcatenateTest : public ConcatenateTest {
 public:
};

TYPED_TEST_SUITE(PrimitiveConcatenateTest, PrimitiveArrowTypes);

TYPED_TEST(PrimitiveConcatenateTest, Primitives) {
  this->Check([this](int64_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = this->rag.template PrimitiveArray<TypeParam>(size, null_probability);
  });
}

TEST_F(ConcatenateTest, NullType) {
  Check([](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = std::make_shared<NullArray>(size);
  });
}

TEST_F(ConcatenateTest, StringType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rag.StringArray(size, null_probability);
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, StringViewType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rag.StringViewArray(size, null_probability);
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, LargeStringType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rag.LargeStringArray(size, null_probability);
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, FixedSizeListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto list_size = 3;
    auto values_size = size * list_size;
    auto values = this->rag.PrimitiveArray<Int8Type>(values_size, null_probability);
    ASSERT_OK_AND_ASSIGN(*out, FixedSizeListArray::FromArrays(values, list_size));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, ListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    ASSERT_OK_AND_ASSIGN(*out, this->rag.ListArray<ListType>(size, null_probability));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, LargeListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    ASSERT_OK_AND_ASSIGN(*out,
                         this->rag.ListArray<LargeListType>(size, null_probability));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, ListViewType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    ASSERT_OK_AND_ASSIGN(*out,
                         this->rag.ListViewArray<ListViewType>(size, null_probability));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, LargeListViewType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    ASSERT_OK_AND_ASSIGN(
        *out, this->rag.ListViewArray<LargeListViewType>(size, null_probability));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, StructType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto foo = this->rag.PrimitiveArray<Int8Type>(size, null_probability);
    auto bar = this->rag.PrimitiveArray<DoubleType>(size, null_probability);
    auto baz = this->rag.PrimitiveArray<BooleanType>(size, null_probability);
    *out = std::make_shared<StructArray>(
        struct_({field("foo", int8()), field("bar", float64()), field("baz", boolean())}),
        size, ArrayVector{foo, bar, baz});
  });
}

TEST_F(ConcatenateTest, DictionaryType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto indices = rag.PrimitiveArray<Int32Type>(size, null_probability);
    auto dict = rag.PrimitiveArray<DoubleType>(128, 0);
    auto type = dictionary(int32(), dict->type());
    *out = std::make_shared<DictionaryArray>(type, indices, dict);
  });
}

TEST_F(ConcatenateTest, DictionaryTypeDifferentDictionaries) {
  {
    auto dict_type = dictionary(uint8(), utf8());
    auto dict_one = DictArrayFromJSON(dict_type, "[1, 2, null, 3, 0]",
                                      "[\"A0\", \"A1\", \"A2\", \"A3\"]");
    auto dict_two = DictArrayFromJSON(dict_type, "[null, 4, 2, 1]",
                                      "[\"B0\", \"B1\", \"B2\", \"B3\", \"B4\"]");
    auto concat_expected = DictArrayFromJSON(
        dict_type, "[1, 2, null, 3, 0, null, 8, 6, 5]",
        "[\"A0\", \"A1\", \"A2\", \"A3\", \"B0\", \"B1\", \"B2\", \"B3\", \"B4\"]");
    ASSERT_OK_AND_ASSIGN(auto concat_actual, Concatenate({dict_one, dict_two}));
    AssertArraysEqual(*concat_expected, *concat_actual);
  }
  {
    const int SIZE = 500;
    auto dict_type = dictionary(uint16(), utf8());

    UInt16Builder index_builder;
    UInt16Builder expected_index_builder;
    ASSERT_OK(index_builder.Reserve(SIZE));
    ASSERT_OK(expected_index_builder.Reserve(SIZE * 2));
    for (auto i = 0; i < SIZE; i++) {
      index_builder.UnsafeAppend(i);
      expected_index_builder.UnsafeAppend(i);
    }
    for (auto i = SIZE; i < 2 * SIZE; i++) {
      expected_index_builder.UnsafeAppend(i);
    }
    ASSERT_OK_AND_ASSIGN(auto indices, index_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto expected_indices, expected_index_builder.Finish());

    // Creates three dictionaries.  The first maps i->"{i}" the second maps i->"{500+i}",
    // each for 500 values and the third maps i->"{i}" but for 1000 values.
    // The first and second concatenated should end up equaling the third.  All strings
    // are padded to length 8 so we can know the size ahead of time.
    StringBuilder values_one_builder;
    StringBuilder values_two_builder;
    ASSERT_OK(values_one_builder.Resize(SIZE));
    ASSERT_OK(values_two_builder.Resize(SIZE));
    ASSERT_OK(values_one_builder.ReserveData(8 * SIZE));
    ASSERT_OK(values_two_builder.ReserveData(8 * SIZE));
    for (auto i = 0; i < SIZE; i++) {
      auto i_str = std::to_string(i);
      auto padded = i_str.insert(0, 8 - i_str.length(), '0');
      values_one_builder.UnsafeAppend(padded);
      auto upper_i_str = std::to_string(i + SIZE);
      auto upper_padded = upper_i_str.insert(0, 8 - i_str.length(), '0');
      values_two_builder.UnsafeAppend(upper_padded);
    }
    ASSERT_OK_AND_ASSIGN(auto dictionary_one, values_one_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto dictionary_two, values_two_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto expected_dictionary,
                         Concatenate({dictionary_one, dictionary_two}))

    auto one = std::make_shared<DictionaryArray>(dict_type, indices, dictionary_one);
    auto two = std::make_shared<DictionaryArray>(dict_type, indices, dictionary_two);
    auto expected = std::make_shared<DictionaryArray>(dict_type, expected_indices,
                                                      expected_dictionary);
    ASSERT_OK_AND_ASSIGN(auto combined, Concatenate({one, two}));
    AssertArraysEqual(*combined, *expected);
  }
}

TEST_F(ConcatenateTest, DictionaryTypePartialOverlapDictionaries) {
  auto dict_type = dictionary(uint8(), utf8());
  auto dict_one = DictArrayFromJSON(dict_type, "[1, 2, null, 3, 0]",
                                    "[\"A0\", \"A1\", \"C2\", \"C3\"]");
  auto dict_two = DictArrayFromJSON(dict_type, "[null, 4, 2, 1]",
                                    "[\"B0\", \"B1\", \"C2\", \"C3\", \"B4\"]");
  auto concat_expected =
      DictArrayFromJSON(dict_type, "[1, 2, null, 3, 0, null, 6, 2, 5]",
                        "[\"A0\", \"A1\", \"C2\", \"C3\", \"B0\", \"B1\", \"B4\"]");
  ASSERT_OK_AND_ASSIGN(auto concat_actual, Concatenate({dict_one, dict_two}));
  AssertArraysEqual(*concat_expected, *concat_actual);
}

TEST_F(ConcatenateTest, DictionaryTypeDifferentSizeIndex) {
  auto dict_type = dictionary(uint8(), utf8());
  auto bigger_dict_type = dictionary(uint16(), utf8());
  auto dict_one = DictArrayFromJSON(dict_type, "[0]", "[\"A0\"]");
  auto dict_two = DictArrayFromJSON(bigger_dict_type, "[0]", "[\"B0\"]");
  ASSERT_RAISES(Invalid, Concatenate({dict_one, dict_two}).status());
}

TEST_F(ConcatenateTest, DictionaryTypeCantUnifyNullInDictionary) {
  auto dict_type = dictionary(uint8(), utf8());
  auto dict_one = DictArrayFromJSON(dict_type, "[0, 1]", "[null, \"A\"]");
  auto dict_two = DictArrayFromJSON(dict_type, "[0, 1]", "[null, \"B\"]");
  ASSERT_RAISES(Invalid, Concatenate({dict_one, dict_two}).status());
}

TEST_F(ConcatenateTest, DictionaryTypeEnlargedIndices) {
  auto size = std::numeric_limits<uint8_t>::max() + 1;
  auto dict_type = dictionary(uint8(), uint16());

  UInt8Builder index_builder;
  ASSERT_OK(index_builder.Reserve(size));
  for (auto i = 0; i < size; i++) {
    index_builder.UnsafeAppend(i);
  }
  ASSERT_OK_AND_ASSIGN(auto indices, index_builder.Finish());

  UInt16Builder values_builder;
  ASSERT_OK(values_builder.Reserve(size));
  UInt16Builder values_builder_two;
  ASSERT_OK(values_builder_two.Reserve(size));
  for (auto i = 0; i < size; i++) {
    values_builder.UnsafeAppend(i);
    values_builder_two.UnsafeAppend(i + size);
  }
  ASSERT_OK_AND_ASSIGN(auto dictionary_one, values_builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto dictionary_two, values_builder_two.Finish());

  auto dict_one = std::make_shared<DictionaryArray>(dict_type, indices, dictionary_one);
  auto dict_two = std::make_shared<DictionaryArray>(dict_type, indices, dictionary_two);
  ASSERT_RAISES(Invalid, Concatenate({dict_one, dict_two}).status());

  auto bigger_dict_type = dictionary(uint16(), uint16());

  auto bigger_one =
      std::make_shared<DictionaryArray>(bigger_dict_type, dictionary_one, dictionary_one);
  auto bigger_two =
      std::make_shared<DictionaryArray>(bigger_dict_type, dictionary_one, dictionary_two);
  ASSERT_OK_AND_ASSIGN(auto combined, Concatenate({bigger_one, bigger_two}));
  ASSERT_EQ(size * 2, combined->length());
}

TEST_F(ConcatenateTest, DictionaryTypeNullSlots) {
  // Regression test for ARROW-13639
  auto dict_type = dictionary(uint32(), utf8());
  auto dict_one = DictArrayFromJSON(dict_type, "[null, null, null, null]", "[]");
  auto dict_two =
      DictArrayFromJSON(dict_type, "[null, null, null, null, 0, 1]", R"(["a", "b"])");
  auto expected = DictArrayFromJSON(
      dict_type, "[null, null, null, null, null, null, null, null, 0, 1]",
      R"(["a", "b"])");
  ASSERT_OK_AND_ASSIGN(auto concat_actual, Concatenate({dict_one, dict_two}));
  ASSERT_OK(concat_actual->ValidateFull());
  TestInitialized(*concat_actual);
  AssertArraysEqual(*expected, *concat_actual);
}

TEST_F(ConcatenateTest, UnionType) {
  // sparse mode
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rag.ArrayOf(sparse_union({
                           field("a", float64()),
                           field("b", boolean()),
                       }),
                       size, null_probability);
  });
  // dense mode
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rag.ArrayOf(dense_union({
                           field("a", uint32()),
                           field("b", boolean()),
                           field("c", int8()),
                       }),
                       size, null_probability);
  });
}

TEST_F(ConcatenateTest, DenseUnionTypeOverflow) {
  // Offset overflow
  auto type_ids = ArrayFromJSON(int8(), "[0]");
  auto offsets = ArrayFromJSON(int32(), "[2147483646]");
  auto child_array = ArrayFromJSON(uint8(), "[0, 1]");
  ASSERT_OK_AND_ASSIGN(auto array,
                       DenseUnionArray::Make(*type_ids, *offsets, {child_array}));
  ArrayVector arrays({array, array});
  ASSERT_RAISES(Invalid, Concatenate(arrays, default_memory_pool()));

  // Length overflow
  auto type_ids_ok = ArrayFromJSON(int8(), "[0]");
  auto offsets_ok = ArrayFromJSON(int32(), "[0]");
  auto child_array_overflow =
      rag.ArrayOf(null(), std::numeric_limits<int32_t>::max() - 1, 0.0);
  ASSERT_OK_AND_ASSIGN(
      auto array_overflow,
      DenseUnionArray::Make(*type_ids_ok, *offsets_ok, {child_array_overflow}));
  ArrayVector arrays_overflow({array_overflow, array_overflow});
  ASSERT_RAISES(Invalid, Concatenate(arrays_overflow, default_memory_pool()));
}

TEST_F(ConcatenateTest, DenseUnionType) {
  auto array = ArrayFromJSON(dense_union({field("", boolean()), field("", int8())}), R"([
    [0, true],
    [0, true],
    [1, 1],
    [1, 2],
    [0, false],
    [1, 3]
  ])");
  ASSERT_OK(array->ValidateFull());

  // Test concatenation of an unsliced array.
  ASSERT_OK_AND_ASSIGN(auto concat_arrays,
                       Concatenate({array, array}, default_memory_pool()));
  ASSERT_OK(concat_arrays->ValidateFull());
  AssertArraysEqual(
      *ArrayFromJSON(dense_union({field("", boolean()), field("", int8())}), R"([
    [0, true],
    [0, true],
    [1, 1],
    [1, 2],
    [0, false],
    [1, 3],
    [0, true],
    [0, true],
    [1, 1],
    [1, 2],
    [0, false],
    [1, 3]
  ])"),
      *concat_arrays);

  // Test concatenation of a sliced array with an unsliced array.
  ASSERT_OK_AND_ASSIGN(auto concat_sliced_arrays,
                       Concatenate({array->Slice(1, 4), array}, default_memory_pool()));
  ASSERT_OK(concat_sliced_arrays->ValidateFull());
  AssertArraysEqual(
      *ArrayFromJSON(dense_union({field("", boolean()), field("", int8())}), R"([
    [0, true],
    [1, 1],
    [1, 2],
    [0, false],
    [0, true],
    [0, true],
    [1, 1],
    [1, 2],
    [0, false],
    [1, 3]
  ])"),
      *concat_sliced_arrays);

  // Test concatenation of an unsliced array, but whose children are sliced.
  auto type_ids = ArrayFromJSON(int8(), "[1, 1, 0, 0, 0]");
  auto offsets = ArrayFromJSON(int32(), "[0, 1, 0, 1, 2]");
  auto child_one =
      ArrayFromJSON(boolean(), "[false, true, true, true, false, false, false]");
  auto child_two = ArrayFromJSON(int8(), "[0, 1, 1, 0, 0, 0, 0]");
  ASSERT_OK_AND_ASSIGN(
      auto array_sliced_children,
      DenseUnionArray::Make(*type_ids, *offsets,
                            {child_one->Slice(1, 3), child_two->Slice(1, 2)}));
  ASSERT_OK(array_sliced_children->ValidateFull());
  ASSERT_OK_AND_ASSIGN(
      auto concat_sliced_children,
      Concatenate({array_sliced_children, array_sliced_children}, default_memory_pool()));
  ASSERT_OK(concat_sliced_children->ValidateFull());
  AssertArraysEqual(
      *ArrayFromJSON(dense_union({field("0", boolean()), field("1", int8())}), R"([
    [1, 1],
    [1, 1],
    [0, true],
    [0, true],
    [0, true],
    [1, 1],
    [1, 1],
    [0, true],
    [0, true],
    [0, true]
  ])"),
      *concat_sliced_children);

  // Test concatenation of a sliced array, whose children also have an offset.
  ASSERT_OK_AND_ASSIGN(auto concat_sliced_array_sliced_children,
                       Concatenate({array_sliced_children->Slice(1, 1),
                                    array_sliced_children->Slice(2, 3)},
                                   default_memory_pool()));
  ASSERT_OK(concat_sliced_array_sliced_children->ValidateFull());
  AssertArraysEqual(
      *ArrayFromJSON(dense_union({field("0", boolean()), field("1", int8())}), R"([
    [1, 1],
    [0, true],
    [0, true],
    [0, true]
  ])"),
      *concat_sliced_array_sliced_children);

  // Test concatenation of dense union array with types codes other than 0..n
  auto array_type_codes =
      ArrayFromJSON(dense_union({field("", int32()), field("", utf8())}, {2, 5}), R"([
    [2, 42],
    [5, "Hello world!"],
    [2, 42]
  ])");
  ASSERT_OK(array_type_codes->ValidateFull());
  ASSERT_OK_AND_ASSIGN(
      auto concat_array_type_codes,
      Concatenate({array_type_codes, array_type_codes, array_type_codes->Slice(1, 1)},
                  default_memory_pool()));
  ASSERT_OK(concat_array_type_codes->ValidateFull());
  AssertArraysEqual(
      *ArrayFromJSON(dense_union({field("", int32()), field("", utf8())}, {2, 5}),
                     R"([
    [2, 42],
    [5, "Hello world!"],
    [2, 42],
    [2, 42],
    [5, "Hello world!"],
    [2, 42],
    [5, "Hello world!"]
  ])"),
      *concat_array_type_codes);
}

TEST_F(ConcatenateTest, ExtensionType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto storage = this->rag.PrimitiveArray<Int16Type>(size, null_probability);
    *out = ExtensionType::WrapArray(smallint(), storage);
  });
}

std::shared_ptr<DataType> LargeVersionOfType(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::BINARY:
      return large_binary();
    case Type::STRING:
      return large_utf8();
    case Type::LIST:
      return large_list(static_cast<const ListType&>(*type).value_type());
    case Type::LIST_VIEW:
      return large_list_view(static_cast<const ListViewType&>(*type).value_type());
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
    case Type::LARGE_LIST:
    case Type::LARGE_LIST_VIEW:
      return type;
    default:
      Unreachable();
  }
}

std::shared_ptr<DataType> fixed_size_list_of_1(std::shared_ptr<DataType> type) {
  return fixed_size_list(std::move(type), 1);
}

TEST_F(ConcatenateTest, OffsetOverflow) {
  using TypeFactory = std::shared_ptr<DataType> (*)(std::shared_ptr<DataType>);
  static const std::vector<TypeFactory> kNestedTypeFactories = {
      list, large_list, list_view, large_list_view, fixed_size_list_of_1,
  };

  auto* pool = default_memory_pool();
  std::shared_ptr<DataType> suggested_cast;
  for (auto& ty : {binary(), utf8()}) {
    auto large_ty = LargeVersionOfType(ty);

    auto fake_long = ArrayFromJSON(ty, "[\"\"]");
    fake_long->data()->GetMutableValues<int32_t>(1)[1] =
        std::numeric_limits<int32_t>::max();
    // XXX: since the data fake_long claims to own isn't there, this would
    // segfault if Concatenate didn't detect overflow and raise an error.
    auto concatenate_status = Concatenate({fake_long, fake_long});
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::StrEq("Invalid: offset overflow while concatenating arrays, "
                         "consider casting input from `" +
                         ty->ToString() + "` to `large_" + ty->ToString() + "` first."),
        concatenate_status);

    concatenate_status =
        internal::Concatenate({fake_long, fake_long}, pool, &suggested_cast);
    // Message is doesn't contain the suggested cast type when the caller
    // asks for it by passing the output parameter.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::StrEq("Invalid: offset overflow while concatenating arrays"),
        concatenate_status);
    ASSERT_TRUE(large_ty->Equals(*suggested_cast));

    // Check that the suggested cast is correct when concatenation
    // fails due to the child array being too large.
    for (auto factory : kNestedTypeFactories) {
      auto nested_ty = factory(ty);
      auto expected_suggestion = factory(large_ty);
      auto fake_long_list = ArrayFromJSON(nested_ty, "[[\"\"]]");
      fake_long_list->data()->child_data[0] = fake_long->data();

      ASSERT_RAISES(Invalid, internal::Concatenate({fake_long_list, fake_long_list}, pool,
                                                   &suggested_cast)
                                 .status());
      ASSERT_TRUE(suggested_cast->Equals(*expected_suggestion));
    }
  }

  auto list_ty = list(utf8());
  auto fake_long_list = ArrayFromJSON(list_ty, "[[\"Hello\"]]");
  fake_long_list->data()->GetMutableValues<int32_t>(1)[1] =
      std::numeric_limits<int32_t>::max();
  ASSERT_RAISES(Invalid, internal::Concatenate({fake_long_list, fake_long_list}, pool,
                                               &suggested_cast)
                             .status());
  ASSERT_TRUE(suggested_cast->Equals(LargeVersionOfType(list_ty)));

  auto list_view_ty = list_view(null());
  auto fake_long_list_view = ArrayFromJSON(list_view_ty, "[[], []]");
  {
    constexpr int kInt32Max = std::numeric_limits<int32_t>::max();
    auto* values = fake_long_list_view->data()->child_data[0].get();
    auto* mutable_offsets = fake_long_list_view->data()->GetMutableValues<int32_t>(1);
    auto* mutable_sizes = fake_long_list_view->data()->GetMutableValues<int32_t>(2);
    values->length = 2 * static_cast<int64_t>(kInt32Max);
    mutable_offsets[1] = kInt32Max;
    mutable_offsets[0] = kInt32Max;
    mutable_sizes[0] = kInt32Max;
  }
  ASSERT_RAISES(Invalid, internal::Concatenate({fake_long_list_view, fake_long_list_view},
                                               pool, &suggested_cast)
                             .status());
  ASSERT_TRUE(suggested_cast->Equals(LargeVersionOfType(list_view_ty)));
}

TEST_F(ConcatenateTest, DictionaryConcatenateWithEmptyUint16) {
  // Regression test for ARROW-17733
  auto dict_type = dictionary(uint16(), utf8());
  auto dict_one = DictArrayFromJSON(dict_type, "[]", "[]");
  auto dict_two =
      DictArrayFromJSON(dict_type, "[0, 1, null, null, null, null]", "[\"A0\", \"A1\"]");
  ASSERT_OK_AND_ASSIGN(auto concat_actual, Concatenate({dict_one, dict_two}));

  AssertArraysEqual(*dict_two, *concat_actual);
}

}  // namespace arrow
