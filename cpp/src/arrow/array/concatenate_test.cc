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

namespace arrow {

class ConcatenateTest : public ::testing::Test {
 protected:
  ConcatenateTest()
      : rng_(seed_),
        sizes_({0, 1, 2, 4, 16, 31, 1234}),
        null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  template <typename OffsetType>
  std::vector<OffsetType> Offsets(int32_t length, int32_t slice_count) {
    std::vector<OffsetType> offsets(static_cast<std::size_t>(slice_count + 1));
    std::default_random_engine gen(seed_);
    std::uniform_int_distribution<OffsetType> dist(0, length);
    std::generate(offsets.begin(), offsets.end(), [&] { return dist(gen); });
    std::sort(offsets.begin(), offsets.end());
    return offsets;
  }

  ArrayVector Slices(const std::shared_ptr<Array>& array,
                     const std::vector<int32_t>& offsets) {
    ArrayVector slices(offsets.size() - 1);
    for (size_t i = 0; i != slices.size(); ++i) {
      slices[i] = array->Slice(offsets[i], offsets[i + 1] - offsets[i]);
    }
    return slices;
  }

  template <typename PrimitiveType>
  std::shared_ptr<Array> GeneratePrimitive(int64_t size, double null_probability) {
    if (std::is_same<PrimitiveType, BooleanType>::value) {
      return rng_.Boolean(size, 0.5, null_probability);
    }
    return rng_.Numeric<PrimitiveType, uint8_t>(size, 0, 127, null_probability);
  }

  void CheckTrailingBitsAreZeroed(const std::shared_ptr<Buffer>& bitmap, int64_t length) {
    if (auto preceding_bits = bit_util::kPrecedingBitmask[length % 8]) {
      auto last_byte = bitmap->data()[length / 8];
      ASSERT_EQ(static_cast<uint8_t>(last_byte & preceding_bits), last_byte)
          << length << " " << int(preceding_bits);
    }
  }

  template <typename ArrayFactory>
  void Check(ArrayFactory&& factory) {
    for (auto size : this->sizes_) {
      auto offsets = this->Offsets<int32_t>(size, 3);
      for (auto null_probability : this->null_probabilities_) {
        std::shared_ptr<Array> array;
        factory(size, null_probability, &array);
        auto expected = array->Slice(offsets.front(), offsets.back() - offsets.front());
        auto slices = this->Slices(array, offsets);
        ASSERT_OK_AND_ASSIGN(auto actual, Concatenate(slices));
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

  random::SeedType seed_ = 0xdeadbeef;
  random::RandomArrayGenerator rng_;
  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
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
    *out = this->template GeneratePrimitive<TypeParam>(size, null_probability);
  });
}

TEST_F(ConcatenateTest, NullType) {
  Check([](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = std::make_shared<NullArray>(size);
  });
}

TEST_F(ConcatenateTest, StringType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rng_.String(size, /*min_length =*/0, /*max_length =*/15, null_probability);
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, LargeStringType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out =
        rng_.LargeString(size, /*min_length =*/0, /*max_length =*/15, null_probability);
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, FixedSizeListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto list_size = 3;
    auto values_size = size * list_size;
    auto values = this->GeneratePrimitive<Int8Type>(values_size, null_probability);
    ASSERT_OK_AND_ASSIGN(*out, FixedSizeListArray::FromArrays(values, list_size));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, ListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto values_size = size * 4;
    auto values = this->GeneratePrimitive<Int8Type>(values_size, null_probability);
    auto offsets_vector = this->Offsets<int32_t>(values_size, size);
    // Ensure first and last offsets encompass the whole values array
    offsets_vector.front() = 0;
    offsets_vector.back() = static_cast<int32_t>(values_size);
    std::shared_ptr<Array> offsets;
    ArrayFromVector<Int32Type>(offsets_vector, &offsets);
    ASSERT_OK_AND_ASSIGN(*out, ListArray::FromArrays(*offsets, *values));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, LargeListType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto values_size = size * 4;
    auto values = this->GeneratePrimitive<Int8Type>(values_size, null_probability);
    auto offsets_vector = this->Offsets<int64_t>(values_size, size);
    // Ensure first and last offsets encompass the whole values array
    offsets_vector.front() = 0;
    offsets_vector.back() = static_cast<int64_t>(values_size);
    std::shared_ptr<Array> offsets;
    ArrayFromVector<Int64Type>(offsets_vector, &offsets);
    ASSERT_OK_AND_ASSIGN(*out, LargeListArray::FromArrays(*offsets, *values));
    ASSERT_OK((**out).ValidateFull());
  });
}

TEST_F(ConcatenateTest, StructType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto foo = this->GeneratePrimitive<Int8Type>(size, null_probability);
    auto bar = this->GeneratePrimitive<DoubleType>(size, null_probability);
    auto baz = this->GeneratePrimitive<BooleanType>(size, null_probability);
    *out = std::make_shared<StructArray>(
        struct_({field("foo", int8()), field("bar", float64()), field("baz", boolean())}),
        size, ArrayVector{foo, bar, baz});
  });
}

TEST_F(ConcatenateTest, DictionaryType) {
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    auto indices = this->GeneratePrimitive<Int32Type>(size, null_probability);
    auto dict = this->GeneratePrimitive<DoubleType>(128, 0);
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
    *out = rng_.ArrayOf(sparse_union({
                            field("a", float64()),
                            field("b", boolean()),
                        }),
                        size, null_probability);
  });
  // dense mode
  Check([this](int32_t size, double null_probability, std::shared_ptr<Array>* out) {
    *out = rng_.ArrayOf(dense_union({
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
      this->rng_.ArrayOf(null(), std::numeric_limits<int32_t>::max() - 1, 0.0);
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
    auto storage = this->GeneratePrimitive<Int16Type>(size, null_probability);
    *out = ExtensionType::WrapArray(smallint(), storage);
  });
}

TEST_F(ConcatenateTest, OffsetOverflow) {
  auto fake_long = ArrayFromJSON(utf8(), "[\"\"]");
  fake_long->data()->GetMutableValues<int32_t>(1)[1] =
      std::numeric_limits<int32_t>::max();
  std::shared_ptr<Array> concatenated;
  // XX since the data fake_long claims to own isn't there, this will segfault if
  // Concatenate doesn't detect overflow and raise an error.
  ASSERT_RAISES(Invalid, Concatenate({fake_long, fake_long}).status());
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
