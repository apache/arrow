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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/cast.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/span.h"
#include "arrow/util/string.h"
#include "parquet/encoding.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;
using arrow::internal::checked_cast;
using arrow::util::span;

namespace bit_util = arrow::bit_util;

namespace parquet::test {

TEST(VectorBooleanTest, TestEncodeBoolDecode) {
  // PARQUET-454
  const int nvalues = 10000;
  bool decode_buffer[nvalues] = {false};

  int nbytes = static_cast<int>(bit_util::BytesForBits(nvalues));

  std::vector<bool> draws;
  ::arrow::random_is_valid(nvalues, 0.5 /* null prob */, &draws, 0 /* seed */);

  std::unique_ptr<BooleanEncoder> encoder =
      MakeTypedEncoder<BooleanType>(Encoding::PLAIN);
  encoder->Put(draws, nvalues);

  std::unique_ptr<BooleanDecoder> decoder =
      MakeTypedDecoder<BooleanType>(Encoding::PLAIN);

  std::shared_ptr<Buffer> encode_buffer = encoder->FlushValues();
  ASSERT_EQ(nbytes, encode_buffer->size());

  decoder->SetData(nvalues, encode_buffer->data(),
                   static_cast<int>(encode_buffer->size()));
  int values_decoded = decoder->Decode(&decode_buffer[0], nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (int i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], decode_buffer[i]);
  }
}

TEST(VectorBooleanTest, TestEncodeIntDecode) {
  // PARQUET-454
  int nvalues = 10000;

  int nbytes = static_cast<int>(bit_util::BytesForBits(nvalues));

  std::vector<bool> draws;
  ::arrow::random_is_valid(nvalues, 0.5 /* null prob */, &draws, 0 /* seed */);

  std::unique_ptr<BooleanEncoder> encoder =
      MakeTypedEncoder<BooleanType>(Encoding::PLAIN);
  encoder->Put(draws, nvalues);

  std::unique_ptr<BooleanDecoder> decoder =
      MakeTypedDecoder<BooleanType>(Encoding::PLAIN);

  std::shared_ptr<Buffer> encode_buffer = encoder->FlushValues();
  ASSERT_EQ(nbytes, encode_buffer->size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const uint8_t* decode_data = &decode_buffer[0];

  decoder->SetData(nvalues, encode_buffer->data(),
                   static_cast<int>(encode_buffer->size()));
  int values_decoded = decoder->Decode(&decode_buffer[0], nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (int i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], ::arrow::bit_util::GetBit(decode_data, i)) << i;
  }
}

template <typename T>
void VerifyResults(T* result, T* expected, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    ASSERT_EQ(expected[i], result[i]) << i;
  }
}

template <typename T>
void VerifyResultsSpaced(T* result, T* expected, int num_values,
                         const uint8_t* valid_bits, int64_t valid_bits_offset) {
  for (auto i = 0; i < num_values; ++i) {
    if (bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
      ASSERT_EQ(expected[i], result[i]) << i;
    }
  }
}

template <>
void VerifyResults<FLBA>(FLBA* result, FLBA* expected, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    ASSERT_EQ(0, memcmp(expected[i].ptr, result[i].ptr, kGenerateDataFLBALength)) << i;
  }
}

template <>
void VerifyResultsSpaced<FLBA>(FLBA* result, FLBA* expected, int num_values,
                               const uint8_t* valid_bits, int64_t valid_bits_offset) {
  for (auto i = 0; i < num_values; ++i) {
    if (bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
      ASSERT_EQ(0, memcmp(expected[i].ptr, result[i].ptr, kGenerateDataFLBALength)) << i;
    }
  }
}

// ----------------------------------------------------------------------
// Create some column descriptors

template <typename DType>
std::shared_ptr<ColumnDescriptor> ExampleDescr() {
  auto node = schema::PrimitiveNode::Make("name", Repetition::OPTIONAL, DType::type_num);
  return std::make_shared<ColumnDescriptor>(node, 0, 0);
}

template <>
std::shared_ptr<ColumnDescriptor> ExampleDescr<FLBAType>() {
  auto node = schema::PrimitiveNode::Make(
      "name", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::DECIMAL,
      kGenerateDataFLBALength, 10, 2);
  return std::make_shared<ColumnDescriptor>(node, 0, 0);
}

// ----------------------------------------------------------------------
// Plain encoding tests

template <typename Type>
class TestEncodingBase : public ::testing::Test {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;

  void SetUp() {
    descr_ = ExampleDescr<Type>();
    type_length_ = descr_->type_length();
    unencoded_byte_array_data_bytes_ = 0;
    allocator_ = default_memory_pool();
  }

  void TearDown() {}

  virtual void InitData(int nvalues, int repeats) {
    num_values_ = nvalues * repeats;
    input_bytes_.resize(num_values_ * sizeof(c_type));
    output_bytes_.resize(num_values_ * sizeof(c_type));
    draws_ = reinterpret_cast<c_type*>(input_bytes_.data());
    decode_buf_ = reinterpret_cast<c_type*>(output_bytes_.data());
    GenerateData<c_type>(nvalues, draws_, &data_buffer_);

    // add some repeated values
    for (int j = 1; j < repeats; ++j) {
      for (int i = 0; i < nvalues; ++i) {
        draws_[nvalues * j + i] = draws_[i];
      }
    }

    InitUnencodedByteArrayDataBytes();
  }

  virtual void CheckRoundtrip() = 0;

  virtual void CheckRoundtripSpaced(const uint8_t* valid_bits,
                                    int64_t valid_bits_offset) {}

  void Execute(int nvalues, int repeats) {
    InitData(nvalues, repeats);
    CheckRoundtrip();
  }

  void ExecuteSpaced(int nvalues, int repeats, int64_t valid_bits_offset,
                     double null_probability) {
    InitData(nvalues, repeats);

    int64_t size = num_values_ + valid_bits_offset;
    auto rand = ::arrow::random::RandomArrayGenerator(1923);
    const auto array = rand.UInt8(size, 0, 100, null_probability);
    const auto valid_bits = array->null_bitmap_data();
    if (valid_bits) {
      CheckRoundtripSpaced(valid_bits, valid_bits_offset);
    }
  }

  void InitUnencodedByteArrayDataBytes() {
    // Calculate expected unencoded bytes based on type
    if constexpr (std::is_same_v<Type, ByteArrayType>) {
      unencoded_byte_array_data_bytes_ = 0;
      for (int i = 0; i < num_values_; i++) {
        unencoded_byte_array_data_bytes_ += draws_[i].len;
      }
    }
  }

 protected:
  MemoryPool* allocator_;

  int num_values_;
  int type_length_;
  c_type* draws_;
  c_type* decode_buf_;
  std::vector<uint8_t> input_bytes_;
  std::vector<uint8_t> output_bytes_;
  std::vector<uint8_t> data_buffer_;

  std::shared_ptr<Buffer> encode_buffer_;
  std::shared_ptr<ColumnDescriptor> descr_;
  int64_t unencoded_byte_array_data_bytes_;  // unencoded data size for dense values
};

// Member variables are not visible to templated subclasses. Possibly figure
// out an alternative to this class layering at some point
#define USING_BASE_MEMBERS()                    \
  using TestEncodingBase<Type>::allocator_;     \
  using TestEncodingBase<Type>::descr_;         \
  using TestEncodingBase<Type>::num_values_;    \
  using TestEncodingBase<Type>::draws_;         \
  using TestEncodingBase<Type>::data_buffer_;   \
  using TestEncodingBase<Type>::type_length_;   \
  using TestEncodingBase<Type>::encode_buffer_; \
  using TestEncodingBase<Type>::decode_buf_;

template <typename Type>
class TestPlainEncoding : public TestEncodingBase<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;

  virtual void CheckRoundtrip() {
    auto encoder =
        MakeTypedEncoder<Type>(Encoding::PLAIN, /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(Encoding::PLAIN, descr_.get());
    encoder->Put(draws_, num_values_);
    encode_buffer_ = encoder->FlushValues();
    if constexpr (std::is_same_v<Type, ByteArrayType>) {
      ASSERT_EQ(encoder->ReportUnencodedDataBytes(),
                this->unencoded_byte_array_data_bytes_);
    }

    decoder->SetData(num_values_, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    int values_decoded = decoder->Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
  }

  void CheckRoundtripSpaced(const uint8_t* valid_bits, int64_t valid_bits_offset) {
    auto encoder =
        MakeTypedEncoder<Type>(Encoding::PLAIN, /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(Encoding::PLAIN, descr_.get());
    int null_count = 0;
    for (auto i = 0; i < num_values_; i++) {
      if (!bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
        null_count++;
      }
    }

    encoder->PutSpaced(draws_, num_values_, valid_bits, valid_bits_offset);
    encode_buffer_ = encoder->FlushValues();
    decoder->SetData(num_values_ - null_count, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    auto values_decoded = decoder->DecodeSpaced(decode_buf_, num_values_, null_count,
                                                valid_bits, valid_bits_offset);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResultsSpaced<c_type>(decode_buf_, draws_, num_values_,
                                                        valid_bits, valid_bits_offset));
  }

 protected:
  USING_BASE_MEMBERS();
};

TYPED_TEST_SUITE(TestPlainEncoding, ParquetTypes);

TYPED_TEST(TestPlainEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(10000, 1));

  // Spaced test with different sizes and offset to guarantee SIMD implementation
  constexpr int kAvx512Size = 64;         // sizeof(__m512i) for Avx512
  constexpr int kSimdSize = kAvx512Size;  // Current the max is Avx512
  constexpr int kMultiSimdSize = kSimdSize * 33;

  for (auto null_prob : {0.001, 0.1, 0.5, 0.9, 0.999}) {
    // Test with both size and offset up to 3 Simd block
    for (auto i = 1; i < kSimdSize * 3; i++) {
      ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(i, 1, 0, null_prob));
      ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(i, 1, i + 1, null_prob));
    }
    // Large block and offset
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize, 1, 0, null_prob));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize + 33, 1, 0, null_prob));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize, 1, 33, null_prob));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize + 33, 1, 33, null_prob));
  }
}

// ----------------------------------------------------------------------
// Dictionary encoding tests

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                         ByteArrayType, FLBAType>
    DictEncodedTypes;

template <typename Type>
class TestDictionaryEncoding : public TestEncodingBase<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;

  void CheckRoundtrip() {
    std::vector<uint8_t> valid_bits(::arrow::bit_util::BytesForBits(num_values_) + 1,
                                    255);

    auto base_encoder = MakeEncoder(Type::type_num, Encoding::PLAIN, true, descr_.get());
    auto encoder =
        dynamic_cast<typename EncodingTraits<Type>::Encoder*>(base_encoder.get());
    auto dict_traits = dynamic_cast<DictEncoder<Type>*>(base_encoder.get());

    ASSERT_NO_THROW(encoder->Put(draws_, num_values_));
    dict_buffer_ =
        AllocateBuffer(default_memory_pool(), dict_traits->dict_encoded_size());
    dict_traits->WriteDict(dict_buffer_->mutable_data());
    std::shared_ptr<Buffer> indices = encoder->FlushValues();
    if constexpr (std::is_same_v<Type, ByteArrayType>) {
      ASSERT_EQ(encoder->ReportUnencodedDataBytes(),
                this->unencoded_byte_array_data_bytes_);
    }

    auto base_spaced_encoder =
        MakeEncoder(Type::type_num, Encoding::PLAIN, true, descr_.get());
    auto spaced_encoder =
        dynamic_cast<typename EncodingTraits<Type>::Encoder*>(base_spaced_encoder.get());

    // PutSpaced should lead to the same results
    // This also checks the PutSpaced implementation for valid_bits=nullptr
    ASSERT_NO_THROW(spaced_encoder->PutSpaced(draws_, num_values_, nullptr, 0));
    std::shared_ptr<Buffer> indices_from_spaced = spaced_encoder->FlushValues();
    ASSERT_TRUE(indices_from_spaced->Equals(*indices));

    auto dict_decoder = MakeTypedDecoder<Type>(Encoding::PLAIN, descr_.get());
    dict_decoder->SetData(dict_traits->num_entries(), dict_buffer_->data(),
                          static_cast<int>(dict_buffer_->size()));

    auto decoder = MakeDictDecoder<Type>(descr_.get());
    decoder->SetDict(dict_decoder.get());

    decoder->SetData(num_values_, indices->data(), static_cast<int>(indices->size()));
    int values_decoded = decoder->Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);

    // TODO(wesm): The DictionaryDecoder must stay alive because the decoded
    // values' data is owned by a buffer inside the DictionaryEncoder. We
    // should revisit when data lifetime is reviewed more generally.
    ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));

    // Also test spaced decoding
    decoder->SetData(num_values_, indices->data(), static_cast<int>(indices->size()));
    // Also tests DecodeSpaced handling for valid_bits=nullptr
    values_decoded = decoder->DecodeSpaced(decode_buf_, num_values_, 0, nullptr, 0);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
  }

 protected:
  USING_BASE_MEMBERS();
  std::shared_ptr<ResizableBuffer> dict_buffer_;
};

TYPED_TEST_SUITE(TestDictionaryEncoding, DictEncodedTypes);

TYPED_TEST(TestDictionaryEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(2500, 2));
}

TEST(TestDictionaryEncoding, CannotDictDecodeBoolean) {
  ASSERT_THROW(MakeDictDecoder<BooleanType>(nullptr), ParquetException);
}

// ----------------------------------------------------------------------
// Shared arrow builder decode tests

std::vector<std::shared_ptr<::arrow::DataType>> binary_like_types_for_dense_decoding() {
  return {::arrow::binary(), ::arrow::large_binary(), ::arrow::binary_view(),
          ::arrow::utf8(),   ::arrow::large_utf8(),   ::arrow::utf8_view()};
}

std::vector<std::shared_ptr<::arrow::DataType>> binary_like_types_for_dict_decoding() {
  return {::arrow::binary()};
}

class TestArrowBuilderDecoding : public ::testing::Test {
 public:
  using DictBuilder = ::arrow::BinaryDictionary32Builder;

  void SetUp() override { null_probabilities_ = {0.0, 0.5, 1.0}; }
  void TearDown() override {}

  void InitTestCase(const std::shared_ptr<::arrow::DataType>& dense_type,
                    double null_probability, bool create_dict) {
    GenerateInputData(dense_type, null_probability, create_dict);
    SetupEncoderDecoder();
  }

  void GenerateInputData(const std::shared_ptr<::arrow::DataType>& dense_type,
                         double null_probability, bool create_dict) {
    constexpr int num_unique = 100;
    constexpr int repeat = 100;
    constexpr int64_t min_length = 2;
    constexpr int64_t max_length = 10;
    ::arrow::random::RandomArrayGenerator rag(0);
    binary_dense_ = rag.BinaryWithRepeats(repeat * num_unique, num_unique, min_length,
                                          max_length, null_probability);
    dense_type_ = dense_type;
    ASSERT_OK_AND_ASSIGN(expected_dense_,
                         ::arrow::compute::Cast(*binary_dense_, dense_type_));

    num_values_ = static_cast<int>(binary_dense_->length());
    null_count_ = static_cast<int>(binary_dense_->null_count());
    valid_bits_ = binary_dense_->null_bitmap_data();

    if (create_dict) {
      auto builder = CreateDictBuilder();
      ASSERT_OK(builder->AppendArray(*binary_dense_));
      ASSERT_OK(builder->Finish(&expected_dict_));
    }

    // Initialize input_data_ for the encoder from the expected_array_ values
    const auto& binary_array = checked_cast<const ::arrow::BinaryArray&>(*binary_dense_);
    input_data_.resize(binary_array.length());
    for (int64_t i = 0; i < binary_array.length(); ++i) {
      input_data_[i] = binary_array.GetView(i);
    }
  }

  std::unique_ptr<DictBuilder> CreateDictBuilder() {
    EXPECT_EQ(dense_type_->id(), ::arrow::Type::BINARY)
        << "Only BINARY is supported for dictionary decoding";
    return std::make_unique<DictBuilder>(default_memory_pool());
  }

  // Setup encoder/decoder pair for testing with
  virtual void SetupEncoderDecoder() = 0;

  void CheckDense(int actual_num_values, const ::arrow::Array& chunk) {
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    ASSERT_OK(chunk.ValidateFull());
    ASSERT_ARRAYS_EQUAL(chunk, *expected_dense_);
  }

  template <typename Builder>
  void CheckDict(int actual_num_values, Builder& builder) {
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    std::shared_ptr<::arrow::Array> actual;
    ASSERT_OK(builder.Finish(&actual));
    ASSERT_OK(actual->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*actual, *expected_dict_);
  }

  void CheckDecodeArrowUsingDenseBuilder() {
    for (auto dense_type : binary_like_types_for_dense_decoding()) {
      ARROW_SCOPED_TRACE("dense_type = ", *dense_type);
      for (auto np : null_probabilities_) {
        InitTestCase(dense_type, np, /*create_dict=*/false);

        typename EncodingTraits<ByteArrayType>::Accumulator acc;
        ASSERT_OK_AND_ASSIGN(acc.builder, ::arrow::MakeBuilder(dense_type_));
        auto actual_num_values =
            decoder_->DecodeArrow(num_values_, null_count_, valid_bits_, 0, &acc);

        std::shared_ptr<::arrow::Array> chunk;
        ASSERT_OK(acc.builder->Finish(&chunk));
        CheckDense(actual_num_values, *chunk);
      }
    }
  }

  void CheckDecodeArrowUsingDictBuilder() {
    for (auto dense_type : binary_like_types_for_dict_decoding()) {
      ARROW_SCOPED_TRACE("dense_type = ", *dense_type);
      for (auto np : null_probabilities_) {
        InitTestCase(dense_type, np, /*create_dict=*/true);
        auto builder = CreateDictBuilder();
        auto actual_num_values = decoder_->DecodeArrow(num_values_, null_count_,
                                                       valid_bits_, 0, builder.get());
        CheckDict(actual_num_values, *builder);
      }
    }
  }

  void CheckDecodeArrowNonNullUsingDenseBuilder() {
    for (auto dense_type : binary_like_types_for_dense_decoding()) {
      ARROW_SCOPED_TRACE("dense_type = ", *dense_type);
      InitTestCase(dense_type, /*null_probability=*/0.0, /*create_dict=*/false);

      typename EncodingTraits<ByteArrayType>::Accumulator acc;
      ASSERT_OK_AND_ASSIGN(acc.builder, ::arrow::MakeBuilder(dense_type_));
      auto actual_num_values = decoder_->DecodeArrowNonNull(num_values_, &acc);

      std::shared_ptr<::arrow::Array> chunk;
      ASSERT_OK(acc.builder->Finish(&chunk));
      CheckDense(actual_num_values, *chunk);
    }
  }

  void CheckDecodeArrowNonNullUsingDictBuilder() {
    for (auto dense_type : binary_like_types_for_dict_decoding()) {
      ARROW_SCOPED_TRACE("dense_type = ", *dense_type);
      InitTestCase(dense_type, /*null_probability=*/0.0, /*create_dict=*/true);
      auto builder = CreateDictBuilder();
      auto actual_num_values = decoder_->DecodeArrowNonNull(num_values_, builder.get());
      CheckDict(actual_num_values, *builder);
    }
  }

 protected:
  std::vector<double> null_probabilities_;
  std::shared_ptr<::arrow::Array> binary_dense_;
  std::shared_ptr<::arrow::DataType> dense_type_;
  std::shared_ptr<::arrow::Array> expected_dict_;
  std::shared_ptr<::arrow::Array> expected_dense_;
  int num_values_;
  int null_count_;
  std::vector<ByteArray> input_data_;
  const uint8_t* valid_bits_;
  std::unique_ptr<ByteArrayEncoder> encoder_;
  ByteArrayDecoder* decoder_;
  std::unique_ptr<ByteArrayDecoder> plain_decoder_;
  std::unique_ptr<DictDecoder<ByteArrayType>> dict_decoder_;
  std::shared_ptr<Buffer> buffer_;
};

class PlainEncoding : public TestArrowBuilderDecoding {
 public:
  void SetupEncoderDecoder() override {
    encoder_ = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    plain_decoder_ = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder_ = plain_decoder_.get();
    if (valid_bits_ != nullptr) {
      ASSERT_NO_THROW(
          encoder_->PutSpaced(input_data_.data(), num_values_, valid_bits_, 0));
    } else {
      ASSERT_NO_THROW(encoder_->Put(input_data_.data(), num_values_));
    }
    buffer_ = encoder_->FlushValues();
    decoder_->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
  }
};

TEST_F(PlainEncoding, CheckDecodeArrowUsingDenseBuilder) {
  this->CheckDecodeArrowUsingDenseBuilder();
}

TEST_F(PlainEncoding, CheckDecodeArrowUsingDictBuilder) {
  this->CheckDecodeArrowUsingDictBuilder();
}

TEST_F(PlainEncoding, CheckDecodeArrowNonNullDenseBuilder) {
  this->CheckDecodeArrowNonNullUsingDenseBuilder();
}

TEST_F(PlainEncoding, CheckDecodeArrowNonNullDictBuilder) {
  this->CheckDecodeArrowNonNullUsingDictBuilder();
}

TEST(PlainEncodingAdHoc, ArrowBinaryDirectPut) {
  // Implemented as part of ARROW-3246

  const int64_t size = 50;
  const int32_t min_length = 0;
  const int32_t max_length = 10;
  const double null_probability = 0.25;

  auto CheckSeed = [&](int seed) {
    ::arrow::random::RandomArrayGenerator rag(seed);
    auto values = rag.String(size, min_length, max_length, null_probability);

    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);

    ASSERT_NO_THROW(encoder->Put(*values));
    // For Plain encoding, the estimated size should be at least the total byte size
    auto& string_array = dynamic_cast<const ::arrow::StringArray&>(*values);
    EXPECT_GE(encoder->EstimatedDataEncodedSize(), string_array.total_values_length())
        << "Estimated size should be at least the total byte size";

    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    acc.builder = std::make_unique<::arrow::StringBuilder>();
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(50, result->length());
    ::arrow::AssertArraysEqual(*values, *result);
  };

  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    CheckSeed(seed);
  }
}

// Check that one can put several Arrow arrays into a given encoder
// and decode to the right values (see GH-36939)
TEST(BooleanArrayEncoding, AdHocRoundTrip) {
  std::vector<std::shared_ptr<::arrow::Array>> arrays{
      ::arrow::ArrayFromJSON(::arrow::boolean(), R"([])"),
      ::arrow::ArrayFromJSON(::arrow::boolean(), R"([false, null, true])"),
      ::arrow::ArrayFromJSON(::arrow::boolean(), R"([null, null, null])"),
      ::arrow::ArrayFromJSON(::arrow::boolean(), R"([true, null, false])"),
  };

  for (auto encoding : {Encoding::PLAIN, Encoding::RLE}) {
    auto encoder = MakeTypedEncoder<BooleanType>(encoding,
                                                 /*use_dictionary=*/false);
    for (const auto& array : arrays) {
      encoder->Put(*array);
    }
    auto buffer = encoder->FlushValues();
    auto decoder = MakeTypedDecoder<BooleanType>(encoding);
    EXPECT_OK_AND_ASSIGN(auto expected, ::arrow::Concatenate(arrays));
    decoder->SetData(static_cast<int>(expected->length()), buffer->data(),
                     static_cast<int>(buffer->size()));

    ::arrow::BooleanBuilder builder;
    ASSERT_EQ(static_cast<int>(expected->length() - expected->null_count()),
              decoder->DecodeArrow(static_cast<int>(expected->length()),
                                   static_cast<int>(expected->null_count()),
                                   expected->null_bitmap_data(), 0, &builder));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(builder.Finish(&result));
    ASSERT_EQ(expected->length(), result->length());
    ::arrow::AssertArraysEqual(*expected, *result, /*verbose=*/true);
  }
}

class TestBooleanArrowDecoding : public ::testing::Test {
 public:
  // number of values including nulls
  constexpr static int kNumValues = 10000;

  void SetUp() override {
    null_probabilities_ = {0.0, 0.001, 0.5, 0.999, 1.0};
    read_batch_sizes_ = {1024, 4096, 10000};
    true_probabilities_ = {0.0, 0.001, 0.5, 0.999, 1.0};
  }
  void TearDown() override {}

  void InitTestCase(Encoding::type encoding, double null_probability,
                    double true_probability) {
    GenerateInputData(null_probability, true_probability);
    SetupEncoderDecoder(encoding);
  }

  void GenerateInputData(double null_probability, double true_probability) {
    ::arrow::random::RandomArrayGenerator rag(0);
    expected_dense_ = rag.Boolean(kNumValues, true_probability, null_probability);
    null_count_ = static_cast<int>(expected_dense_->null_count());
    valid_bits_ = expected_dense_->null_bitmap_data();

    // Initialize input_data_ for the encoder from the expected_array_ values
    const auto& boolean_array =
        checked_cast<const ::arrow::BooleanArray&>(*expected_dense_);
    input_data_.resize(boolean_array.length());

    for (int64_t i = 0; i < boolean_array.length(); ++i) {
      input_data_[i] = boolean_array.Value(i);
    }
  }

  // Setup encoder/decoder pair for testing with boolean encoding
  void SetupEncoderDecoder(Encoding::type encoding) {
    encoder_ = MakeTypedEncoder<BooleanType>(encoding);
    decoder_ = MakeTypedDecoder<BooleanType>(encoding);
    const auto* data_ptr = reinterpret_cast<const bool*>(input_data_.data());
    if (valid_bits_ != nullptr) {
      ASSERT_NO_THROW(encoder_->PutSpaced(data_ptr, kNumValues, valid_bits_, 0));
    } else {
      ASSERT_NO_THROW(encoder_->Put(data_ptr, kNumValues));
    }
    buffer_ = encoder_->FlushValues();
    ResetTheDecoder();
  }

  void ResetTheDecoder() {
    ASSERT_NE(nullptr, buffer_);
    decoder_->SetData(kNumValues, buffer_->data(), static_cast<int>(buffer_->size()));
  }

  void CheckDense(int actual_num_values, const ::arrow::Array& chunk) {
    ASSERT_EQ(actual_num_values, kNumValues - null_count_);
    ASSERT_ARRAYS_EQUAL(chunk, *expected_dense_);
  }

  void CheckDecodeArrow(Encoding::type encoding) {
    for (double np : null_probabilities_) {
      for (double true_prob : true_probabilities_) {
        InitTestCase(encoding, np, true_prob);
        for (int read_batch_size : this->read_batch_sizes_) {
          ResetTheDecoder();

          int num_values_left = kNumValues;
          ::arrow::BooleanBuilder acc;
          int actual_num_not_null_values = 0;
          while (num_values_left > 0) {
            int batch_size = std::min(num_values_left, read_batch_size);
            ASSERT_NE(0, batch_size);
            // Counting nulls
            int64_t batch_null_count = 0;
            if (null_count_ != 0) {
              batch_null_count =
                  batch_size - ::arrow::internal::CountSetBits(
                                   valid_bits_, kNumValues - num_values_left, batch_size);
            }
            int batch_num_values =
                decoder_->DecodeArrow(batch_size, static_cast<int>(batch_null_count),
                                      valid_bits_, kNumValues - num_values_left, &acc);
            actual_num_not_null_values += batch_num_values;
            num_values_left -= batch_size;
          }
          std::shared_ptr<::arrow::Array> chunk;
          ASSERT_OK(acc.Finish(&chunk));
          CheckDense(actual_num_not_null_values, *chunk);
        }
      }
    }
  }

  void CheckDecodeArrowNonNull(Encoding::type encoding) {
    // NonNull skips tests for null_prob != 0.
    for (auto true_prob : true_probabilities_) {
      InitTestCase(encoding, /*null_probability=*/0, true_prob);
      for (int read_batch_size : this->read_batch_sizes_) {
        // Resume the decoder
        ResetTheDecoder();
        ::arrow::BooleanBuilder acc;
        int actual_num_values = 0;
        int num_values_left = kNumValues;
        while (num_values_left > 0) {
          int batch_size = std::min(num_values_left, read_batch_size);
          int batch_num_values = decoder_->DecodeArrowNonNull(batch_size, &acc);
          actual_num_values += batch_num_values;
          num_values_left -= batch_num_values;
        }
        std::shared_ptr<::arrow::Array> chunk;
        ASSERT_OK(acc.Finish(&chunk));
        CheckDense(actual_num_values, *chunk);
      }
    }
  }

 protected:
  std::vector<double> null_probabilities_;
  std::vector<double> true_probabilities_;
  std::vector<int> read_batch_sizes_;
  std::shared_ptr<::arrow::Array> expected_dense_;
  int null_count_{0};
  std::vector<uint8_t> input_data_;
  const uint8_t* valid_bits_ = NULLPTR;
  std::unique_ptr<BooleanEncoder> encoder_;
  std::unique_ptr<BooleanDecoder> decoder_;
  std::shared_ptr<Buffer> buffer_;
};

TEST_F(TestBooleanArrowDecoding, CheckDecodeArrowUsingPlain) {
  this->CheckDecodeArrow(Encoding::PLAIN);
}

TEST_F(TestBooleanArrowDecoding, CheckDecodeArrowNonNullPlain) {
  this->CheckDecodeArrowNonNull(Encoding::PLAIN);
}

TEST_F(TestBooleanArrowDecoding, CheckDecodeArrowRle) {
  this->CheckDecodeArrow(Encoding::RLE);
}

TEST_F(TestBooleanArrowDecoding, CheckDecodeArrowNonNullRle) {
  this->CheckDecodeArrowNonNull(Encoding::RLE);
}

template <typename T>
void GetDictDecoder(DictEncoder<T>* encoder, int64_t num_values,
                    std::shared_ptr<Buffer>* out_values,
                    std::shared_ptr<Buffer>* out_dict, const ColumnDescriptor* descr,
                    std::unique_ptr<TypedDecoder<T>>* out_decoder) {
  auto decoder = MakeDictDecoder<T>(descr);
  auto buf = encoder->FlushValues();
  auto dict_buf = AllocateBuffer(default_memory_pool(), encoder->dict_encoded_size());
  encoder->WriteDict(dict_buf->mutable_data());

  auto dict_decoder = MakeTypedDecoder<T>(Encoding::PLAIN, descr);
  dict_decoder->SetData(encoder->num_entries(), dict_buf->data(),
                        static_cast<int>(dict_buf->size()));

  decoder->SetData(static_cast<int>(num_values), buf->data(),
                   static_cast<int>(buf->size()));
  decoder->SetDict(dict_decoder.get());

  *out_values = buf;
  *out_dict = dict_buf;
  ASSERT_NE(decoder, nullptr);
  auto released = dynamic_cast<TypedDecoder<T>*>(decoder.release());
  ASSERT_NE(released, nullptr);
  *out_decoder = std::unique_ptr<TypedDecoder<T>>(released);
}

template <typename ParquetType>
class EncodingAdHocTyped : public ::testing::Test {
 public:
  using ArrowType = typename EncodingTraits<ParquetType>::ArrowType;
  using EncoderType = typename EncodingTraits<ParquetType>::Encoder;
  using DecoderType = typename EncodingTraits<ParquetType>::Decoder;
  using BuilderType = typename EncodingTraits<ParquetType>::Accumulator;
  using DictBuilderType = typename EncodingTraits<ParquetType>::DictAccumulator;

  static const ColumnDescriptor* column_descr() {
    static auto column_descr = ExampleDescr<ParquetType>();
    return column_descr.get();
  }

  std::shared_ptr<::arrow::Array> GetValues(int seed);

  static std::shared_ptr<::arrow::DataType> arrow_type();

  void Plain(int seed, int rounds = 1, int offset = 0) {
    auto random_array = GetValues(seed)->Slice(offset);
    auto encoder = MakeTypedEncoder<ParquetType>(
        Encoding::PLAIN, /*use_dictionary=*/false, column_descr());
    auto decoder = MakeTypedDecoder<ParquetType>(Encoding::PLAIN, column_descr());

    for (int i = 0; i < rounds; ++i) {
      ASSERT_NO_THROW(encoder->Put(*random_array));
    }
    std::shared_ptr<::arrow::Array> values;
    if (rounds == 1) {
      values = random_array;
    } else {
      ::arrow::ArrayVector arrays(rounds, random_array);
      EXPECT_OK_AND_ASSIGN(values,
                           ::arrow::Concatenate(arrays, ::arrow::default_memory_pool()));
    }
    auto buf = encoder->FlushValues();

    decoder->SetData(static_cast<int>(values->length()), buf->data(),
                     static_cast<int>(buf->size()));

    BuilderType acc(arrow_type(), ::arrow::default_memory_pool());
    ASSERT_EQ(static_cast<int>(values->length() - values->null_count()),
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.Finish(&result));
    ASSERT_OK(result->ValidateFull());
    ASSERT_EQ(values->length(), result->length());
    ::arrow::AssertArraysEqual(*values, *result, /*verbose=*/true);
  }

  void ByteStreamSplit(int seed) {
    if constexpr (!std::is_same_v<ParquetType, FloatType> &&
                  !std::is_same_v<ParquetType, DoubleType> &&
                  !std::is_same_v<ParquetType, Int32Type> &&
                  !std::is_same_v<ParquetType, Int64Type> &&
                  !std::is_same_v<ParquetType, FLBAType>) {
      return;
    }
    auto values = GetValues(seed);
    auto encoder = MakeTypedEncoder<ParquetType>(
        Encoding::BYTE_STREAM_SPLIT, /*use_dictionary=*/false, column_descr());
    auto decoder =
        MakeTypedDecoder<ParquetType>(Encoding::BYTE_STREAM_SPLIT, column_descr());

    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    BuilderType acc(arrow_type(), ::arrow::default_memory_pool());
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.Finish(&result));
    ASSERT_OK(result->ValidateFull());
    ASSERT_EQ(50, result->length());
    ::arrow::AssertArraysEqual(*values, *result);
  }

  void Rle(int seed) {
    if (!std::is_same<ParquetType, BooleanType>::value) {
      return;
    }
    auto values = GetValues(seed);
    auto encoder = MakeTypedEncoder<ParquetType>(Encoding::RLE, /*use_dictionary=*/false,
                                                 column_descr());
    auto decoder = MakeTypedDecoder<ParquetType>(Encoding::RLE, column_descr());

    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    BuilderType acc(arrow_type(), ::arrow::default_memory_pool());
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.Finish(&result));
    ASSERT_OK(result->ValidateFull());
    ASSERT_EQ(50, result->length());
    ::arrow::AssertArraysEqual(*values, *result);
  }

  void DeltaBitPack(int seed) {
    if (!std::is_same<ParquetType, Int32Type>::value &&
        !std::is_same<ParquetType, Int64Type>::value) {
      return;
    }
    auto values = GetValues(seed);
    auto encoder = MakeTypedEncoder<ParquetType>(
        Encoding::DELTA_BINARY_PACKED, /*use_dictionary=*/false, column_descr());
    auto decoder =
        MakeTypedDecoder<ParquetType>(Encoding::DELTA_BINARY_PACKED, column_descr());

    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    BuilderType acc(arrow_type(), ::arrow::default_memory_pool());
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.Finish(&result));
    ASSERT_EQ(50, result->length());
    ::arrow::AssertArraysEqual(*values, *result);
  }

  void Dict(int seed) {
    if (std::is_same<ParquetType, BooleanType>::value) {
      return;
    }

    auto values = GetValues(seed);

    auto owned_encoder =
        MakeTypedEncoder<ParquetType>(Encoding::PLAIN,
                                      /*use_dictionary=*/true, column_descr());
    auto encoder = dynamic_cast<DictEncoder<ParquetType>*>(owned_encoder.get());

    ASSERT_NO_THROW(encoder->Put(*values));

    std::shared_ptr<Buffer> buf, dict_buf;
    int num_values = static_cast<int>(values->length() - values->null_count());

    std::unique_ptr<TypedDecoder<ParquetType>> decoder;
    GetDictDecoder(encoder, num_values, &buf, &dict_buf, column_descr(), &decoder);

    BuilderType acc(arrow_type(), ::arrow::default_memory_pool());
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.Finish(&result));
    ASSERT_OK(result->ValidateFull());
    ::arrow::AssertArraysEqual(*values, *result);
  }

  void DictPutIndices() {
    if (std::is_same<ParquetType, BooleanType>::value) {
      return;
    }

    auto dict_values = ::arrow::ArrayFromJSON(
        arrow_type(), std::is_same<ParquetType, FLBAType>::value
                          ? R"(["abcdefgh", "ijklmnop", "qrstuvwx"])"
                          : "[120, -37, 47]");
    auto indices = ::arrow::ArrayFromJSON(::arrow::int32(), "[0, 1, 2]");
    auto indices_nulls =
        ::arrow::ArrayFromJSON(::arrow::int32(), "[null, 0, 1, null, 2]");

    auto expected = ::arrow::ArrayFromJSON(
        arrow_type(), std::is_same<ParquetType, FLBAType>::value
                          ? R"(["abcdefgh", "ijklmnop", "qrstuvwx", null,
                                "abcdefgh", "ijklmnop", null, "qrstuvwx"])"
                          : "[120, -37, 47, null, "
                            "120, -37, null, 47]");

    auto owned_encoder =
        MakeTypedEncoder<ParquetType>(Encoding::PLAIN,
                                      /*use_dictionary=*/true, column_descr());
    auto owned_decoder = MakeDictDecoder<ParquetType>(column_descr());

    auto encoder = dynamic_cast<DictEncoder<ParquetType>*>(owned_encoder.get());

    ASSERT_NO_THROW(encoder->PutDictionary(*dict_values));

    // Trying to call PutDictionary again throws
    ASSERT_THROW(encoder->PutDictionary(*dict_values), ParquetException);

    ASSERT_NO_THROW(encoder->PutIndices(*indices));
    ASSERT_NO_THROW(encoder->PutIndices(*indices_nulls));

    std::shared_ptr<Buffer> buf, dict_buf;
    int num_values = static_cast<int>(expected->length() - expected->null_count());

    std::unique_ptr<TypedDecoder<ParquetType>> decoder;
    GetDictDecoder(encoder, num_values, &buf, &dict_buf, column_descr(), &decoder);

    BuilderType acc(arrow_type(), ::arrow::default_memory_pool());
    ASSERT_EQ(num_values, decoder->DecodeArrow(static_cast<int>(expected->length()),
                                               static_cast<int>(expected->null_count()),
                                               expected->null_bitmap_data(),
                                               expected->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.Finish(&result));
    ASSERT_OK(result->ValidateFull());
    ::arrow::AssertArraysEqual(*expected, *result);
  }

 protected:
  const int64_t size_ = 50;
  double null_probability_ = 0.25;
};

template <typename ParquetType>
std::shared_ptr<::arrow::DataType> EncodingAdHocTyped<ParquetType>::arrow_type() {
  return ::arrow::TypeTraits<ArrowType>::type_singleton();
}

template <>
std::shared_ptr<::arrow::DataType> EncodingAdHocTyped<FLBAType>::arrow_type() {
  return ::arrow::fixed_size_binary(sizeof(uint64_t));
}

template <typename ParquetType>
std::shared_ptr<::arrow::Array> EncodingAdHocTyped<ParquetType>::GetValues(int seed) {
  ::arrow::random::RandomArrayGenerator rag(seed);
  return rag.Numeric<ArrowType>(size_, 0, 10, null_probability_);
}

template <>
std::shared_ptr<::arrow::Array> EncodingAdHocTyped<BooleanType>::GetValues(int seed) {
  ::arrow::random::RandomArrayGenerator rag(seed);
  return rag.Boolean(size_, 0.1, null_probability_);
}

template <>
std::shared_ptr<::arrow::Array> EncodingAdHocTyped<FLBAType>::GetValues(int seed) {
  ::arrow::random::RandomArrayGenerator rag(seed);
  std::shared_ptr<::arrow::Array> values;
  ARROW_EXPECT_OK(
      rag.UInt64(size_, 0, std::numeric_limits<uint64_t>::max(), null_probability_)
          ->View(arrow_type())
          .Value(&values));
  return values;
}

using EncodingAdHocTypedCases =
    ::testing::Types<BooleanType, Int32Type, Int64Type, FloatType, DoubleType, FLBAType>;

TYPED_TEST_SUITE(EncodingAdHocTyped, EncodingAdHocTypedCases);

TYPED_TEST(EncodingAdHocTyped, PlainArrowDirectPut) {
  for (auto seed : {0, 1, 2, 3, 4}) {
    this->Plain(seed);
  }
  // Same, but without nulls (this could trigger different code paths)
  this->null_probability_ = 0.0;
  for (auto seed : {0, 1, 2, 3, 4}) {
    this->Plain(seed, /*rounds=*/3);
  }
}

TYPED_TEST(EncodingAdHocTyped, PlainArrowDirectPutMultiRound) {
  // Check that one can put several Arrow arrays into a given encoder
  // and decode to the right values (see GH-36939)
  for (auto seed : {0, 1, 2, 3, 4}) {
    this->Plain(seed, /*rounds=*/3);
  }
  // Same, but without nulls
  this->null_probability_ = 0.0;
  for (auto seed : {0, 1, 2, 3, 4}) {
    this->Plain(seed, /*rounds=*/3);
  }
}

TYPED_TEST(EncodingAdHocTyped, PlainArrowDirectPutSliced) {
  this->Plain(/*seed=*/0, /*rounds=*/1, /*offset=*/3);
  // Same, but without nulls
  this->null_probability_ = 0.0;
  this->Plain(/*seed=*/0, /*rounds=*/1, /*offset=*/3);
}

TYPED_TEST(EncodingAdHocTyped, ByteStreamSplitArrowDirectPut) {
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    this->ByteStreamSplit(seed);
  }
  // Same, but without nulls (this could trigger different code paths)
  this->null_probability_ = 0.0;
  for (auto seed : {0, 1, 2, 3, 4}) {
    this->ByteStreamSplit(seed);
  }
}

TYPED_TEST(EncodingAdHocTyped, RleArrowDirectPut) {
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    this->Rle(seed);
  }
}

TYPED_TEST(EncodingAdHocTyped, DeltaBitPackArrowDirectPut) {
  // TODO: test with nulls once DeltaBitPackDecoder::DecodeArrow supports them
  this->null_probability_ = 0;
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    this->DeltaBitPack(seed);
  }
}

TYPED_TEST(EncodingAdHocTyped, DictArrowDirectPut) { this->Dict(0); }

TYPED_TEST(EncodingAdHocTyped, DictArrowDirectPutIndices) { this->DictPutIndices(); }

TEST(DictEncodingAdHoc, ArrowBinaryDirectPut) {
  // Implemented as part of ARROW-3246
  const int64_t size = 50;
  const int64_t min_length = 0;
  const int64_t max_length = 10;
  const double null_probability = 0.1;
  ::arrow::random::RandomArrayGenerator rag(0);
  auto values = rag.String(size, min_length, max_length, null_probability);

  auto owned_encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN,
                                                       /*use_dictionary=*/true);

  auto encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(owned_encoder.get());

  ASSERT_NO_THROW(encoder->Put(*values));

  std::unique_ptr<ByteArrayDecoder> decoder;
  std::shared_ptr<Buffer> buf, dict_buf;
  int num_values = static_cast<int>(values->length() - values->null_count());
  GetDictDecoder(encoder, num_values, &buf, &dict_buf, nullptr, &decoder);

  typename EncodingTraits<ByteArrayType>::Accumulator acc;
  acc.builder.reset(new ::arrow::StringBuilder);
  ASSERT_EQ(num_values,
            decoder->DecodeArrow(static_cast<int>(values->length()),
                                 static_cast<int>(values->null_count()),
                                 values->null_bitmap_data(), values->offset(), &acc));

  std::shared_ptr<::arrow::Array> result;
  ASSERT_OK(acc.builder->Finish(&result));
  ::arrow::AssertArraysEqual(*values, *result);
}

TEST(DictEncodingAdHoc, PutDictionaryPutIndices) {
  // Part of ARROW-3246
  auto dict_values =
      ::arrow::ArrayFromJSON(::arrow::binary(), "[\"foo\", \"bar\", \"baz\"]");

  auto CheckIndexType = [&](const std::shared_ptr<::arrow::DataType>& index_ty) {
    auto indices = ::arrow::ArrayFromJSON(index_ty, "[0, 1, 2]");
    auto indices_nulls = ::arrow::ArrayFromJSON(index_ty, "[null, 0, 1, null, 2]");

    auto expected = ::arrow::ArrayFromJSON(::arrow::binary(),
                                           "[\"foo\", \"bar\", \"baz\", null, "
                                           "\"foo\", \"bar\", null, \"baz\"]");

    auto owned_encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN,
                                                         /*use_dictionary=*/true);
    auto owned_decoder = MakeDictDecoder<ByteArrayType>();

    auto encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(owned_encoder.get());

    ASSERT_NO_THROW(encoder->PutDictionary(*dict_values));

    // Trying to call PutDictionary again throws
    ASSERT_THROW(encoder->PutDictionary(*dict_values), ParquetException);

    ASSERT_NO_THROW(encoder->PutIndices(*indices));
    ASSERT_NO_THROW(encoder->PutIndices(*indices_nulls));

    std::unique_ptr<ByteArrayDecoder> decoder;
    std::shared_ptr<Buffer> buf, dict_buf;
    int num_values = static_cast<int>(expected->length() - expected->null_count());
    GetDictDecoder(encoder, num_values, &buf, &dict_buf, nullptr, &decoder);

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    acc.builder.reset(new ::arrow::BinaryBuilder);
    ASSERT_EQ(num_values, decoder->DecodeArrow(static_cast<int>(expected->length()),
                                               static_cast<int>(expected->null_count()),
                                               expected->null_bitmap_data(),
                                               expected->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ::arrow::AssertArraysEqual(*expected, *result);
  };

  for (auto ty : ::arrow::all_dictionary_index_types()) {
    CheckIndexType(ty);
  }
}

class DictEncoding : public TestArrowBuilderDecoding {
 public:
  void SetupEncoderDecoder() override {
    auto node = schema::ByteArray("name");
    descr_ = std::make_unique<ColumnDescriptor>(node, 0, 0);
    encoder_ = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN, /*use_dictionary=*/true,
                                               descr_.get());
    if (null_count_ == 0) {
      ASSERT_NO_THROW(encoder_->Put(input_data_.data(), num_values_));
    } else {
      ASSERT_NO_THROW(
          encoder_->PutSpaced(input_data_.data(), num_values_, valid_bits_, 0));
    }
    buffer_ = encoder_->FlushValues();

    auto dict_encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(encoder_.get());
    ASSERT_NE(dict_encoder, nullptr);
    dict_buffer_ =
        AllocateBuffer(default_memory_pool(), dict_encoder->dict_encoded_size());
    dict_encoder->WriteDict(dict_buffer_->mutable_data());

    // Simulate reading the dictionary page followed by a data page
    plain_decoder_ = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN, descr_.get());
    plain_decoder_->SetData(dict_encoder->num_entries(), dict_buffer_->data(),
                            static_cast<int>(dict_buffer_->size()));

    dict_decoder_ = MakeDictDecoder<ByteArrayType>(descr_.get());
    dict_decoder_->SetDict(plain_decoder_.get());
    dict_decoder_->SetData(num_values_, buffer_->data(),
                           static_cast<int>(buffer_->size()));
    decoder_ = dynamic_cast<ByteArrayDecoder*>(dict_decoder_.get());
  }

 protected:
  std::unique_ptr<ColumnDescriptor> descr_;
  std::shared_ptr<Buffer> dict_buffer_;
};

TEST_F(DictEncoding, CheckDecodeArrowUsingDenseBuilder) {
  this->CheckDecodeArrowUsingDenseBuilder();
}

TEST_F(DictEncoding, CheckDecodeArrowUsingDictBuilder) {
  this->CheckDecodeArrowUsingDictBuilder();
}

TEST_F(DictEncoding, CheckDecodeArrowNonNullDenseBuilder) {
  this->CheckDecodeArrowNonNullUsingDenseBuilder();
}

TEST_F(DictEncoding, CheckDecodeArrowNonNullDictBuilder) {
  this->CheckDecodeArrowNonNullUsingDictBuilder();
}

TEST_F(DictEncoding, CheckDecodeIndicesSpaced) {
  for (auto np : null_probabilities_) {
    InitTestCase(::arrow::binary(), np, /*create_dict=*/true);
    auto builder = CreateDictBuilder();
    dict_decoder_->InsertDictionary(builder.get());
    int actual_num_values;
    if (null_count_ == 0) {
      actual_num_values = dict_decoder_->DecodeIndices(num_values_, builder.get());
    } else {
      actual_num_values = dict_decoder_->DecodeIndicesSpaced(
          num_values_, null_count_, valid_bits_, 0, builder.get());
    }
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    std::shared_ptr<::arrow::Array> actual;
    ASSERT_OK(builder->Finish(&actual));
    ASSERT_ARRAYS_EQUAL(*actual, *expected_dict_);

    // Check that null indices are zero-initialized
    const auto& dict_actual = checked_cast<const ::arrow::DictionaryArray&>(*actual);
    const auto& indices =
        checked_cast<const ::arrow::Int32Array&>(*dict_actual.indices());

    auto raw_values = indices.raw_values();
    for (int64_t i = 0; i < indices.length(); ++i) {
      if (indices.IsNull(i) && raw_values[i] != 0) {
        FAIL() << "Null slot not zero-initialized";
      }
    }
  }
}

TEST_F(DictEncoding, CheckDecodeIndicesNoNulls) {
  InitTestCase(::arrow::binary(), /*null_probability=*/0.0, /*create_dict=*/true);
  auto builder = CreateDictBuilder();
  dict_decoder_->InsertDictionary(builder.get());
  auto actual_num_values = dict_decoder_->DecodeIndices(num_values_, builder.get());
  CheckDict(actual_num_values, *builder);
}

// ----------------------------------------------------------------------
// BYTE_STREAM_SPLIT encode/decode tests.

template <typename Type>
class TestByteStreamSplitEncoding : public TestEncodingBase<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;

  void CheckRoundtrip() override {
    auto encoder = MakeTypedEncoder<Type>(Encoding::BYTE_STREAM_SPLIT,
                                          /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(Encoding::BYTE_STREAM_SPLIT, descr_.get());
    encoder->Put(draws_, num_values_);
    encode_buffer_ = encoder->FlushValues();

    {
      decoder->SetData(num_values_, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));
      int values_decoded = decoder->Decode(decode_buf_, num_values_);
      ASSERT_EQ(num_values_, values_decoded);
      ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
    }

    {
      // Try again but with a small step.
      decoder->SetData(num_values_, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));
      int step = 131;
      int remaining = num_values_;
      for (int i = 0; i < num_values_; i += step) {
        int num_decoded = decoder->Decode(decode_buf_, step);
        ASSERT_EQ(num_decoded, std::min(step, remaining));
        ASSERT_NO_FATAL_FAILURE(
            VerifyResults<c_type>(decode_buf_, &draws_[i], num_decoded));
        remaining -= num_decoded;
      }
    }

    {
      std::vector<uint8_t> valid_bits(::arrow::bit_util::BytesForBits(num_values_), 0);
      std::vector<c_type> expected_filtered_output;
      const int every_nth = 5;
      expected_filtered_output.reserve((num_values_ + every_nth - 1) / every_nth);
      ::arrow::internal::BitmapWriter writer{valid_bits.data(), 0, num_values_};
      // Set every fifth bit.
      for (int i = 0; i < num_values_; ++i) {
        if (i % every_nth == 0) {
          writer.Set();
          expected_filtered_output.push_back(draws_[i]);
        }
        writer.Next();
      }
      writer.Finish();
      const int expected_size = static_cast<int>(expected_filtered_output.size());
      ASSERT_NO_THROW(encoder->PutSpaced(draws_, num_values_, valid_bits.data(), 0));
      encode_buffer_ = encoder->FlushValues();

      decoder->SetData(expected_size, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));
      int values_decoded = decoder->Decode(decode_buf_, num_values_);
      ASSERT_EQ(expected_size, values_decoded);
      ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(
          decode_buf_, expected_filtered_output.data(), expected_size));
    }
  }

  void CheckRoundtripSpaced(const uint8_t* valid_bits,
                            int64_t valid_bits_offset) override {
    auto encoder =
        MakeTypedEncoder<Type>(Encoding::BYTE_STREAM_SPLIT, false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(Encoding::BYTE_STREAM_SPLIT, descr_.get());
    int null_count = 0;
    for (auto i = 0; i < num_values_; i++) {
      if (!bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
        null_count++;
      }
    }

    encoder->PutSpaced(draws_, num_values_, valid_bits, valid_bits_offset);
    encode_buffer_ = encoder->FlushValues();
    ASSERT_EQ(encode_buffer_->size(), physical_byte_width() * (num_values_ - null_count));
    decoder->SetData(num_values_, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    auto values_decoded = decoder->DecodeSpaced(decode_buf_, num_values_, null_count,
                                                valid_bits, valid_bits_offset);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResultsSpaced<c_type>(decode_buf_, draws_, num_values_,
                                                        valid_bits, valid_bits_offset));
  }

  void CheckDecode();
  void CheckEncode();

 protected:
  USING_BASE_MEMBERS();

  template <typename U>
  void CheckDecode(span<const uint8_t> encoded_data, span<const U> expected_decoded_data,
                   const ColumnDescriptor* descr = nullptr) {
    static_assert(sizeof(U) == sizeof(c_type));
    static_assert(std::is_same_v<U, FLBA> == std::is_same_v<c_type, FLBA>);

    std::unique_ptr<TypedDecoder<Type>> decoder =
        MakeTypedDecoder<Type>(Encoding::BYTE_STREAM_SPLIT, descr);
    int num_elements = static_cast<int>(expected_decoded_data.size());
    decoder->SetData(num_elements, encoded_data.data(),
                     static_cast<int>(encoded_data.size()));
    std::vector<U> decoded_data(num_elements);
    int num_decoded_elements =
        decoder->Decode(reinterpret_cast<c_type*>(decoded_data.data()), num_elements);
    ASSERT_EQ(num_elements, num_decoded_elements);
    // Compare to expected values
    if constexpr (std::is_same_v<c_type, FLBA>) {
      auto type_length = descr->type_length();
      for (int i = 0; i < num_elements; ++i) {
        ASSERT_EQ(span<const uint8_t>(expected_decoded_data[i].ptr, type_length),
                  span<const uint8_t>(decoded_data[i].ptr, type_length));
      }
    } else {
      for (int i = 0; i < num_elements; ++i) {
        ASSERT_EQ(expected_decoded_data[i], decoded_data[i]);
      }
    }
    ASSERT_EQ(0, decoder->values_left());
  }

  template <typename U>
  void CheckEncode(span<const U> data, span<const uint8_t> expected_encoded_data,
                   const ColumnDescriptor* descr = nullptr) {
    static_assert(sizeof(U) == sizeof(c_type));
    static_assert(std::is_same_v<U, FLBA> == std::is_same_v<c_type, FLBA>);

    std::unique_ptr<TypedEncoder<Type>> encoder = MakeTypedEncoder<Type>(
        Encoding::BYTE_STREAM_SPLIT, /*use_dictionary=*/false, descr);
    int num_elements = static_cast<int>(data.size());
    encoder->Put(reinterpret_cast<const c_type*>(data.data()), num_elements);
    auto encoded_data = encoder->FlushValues();
    ASSERT_EQ(expected_encoded_data.size(), encoded_data->size());
    const uint8_t* encoded_data_raw = encoded_data->data();
    for (int64_t i = 0; i < encoded_data->size(); ++i) {
      ASSERT_EQ(expected_encoded_data[i], encoded_data_raw[i]);
    }
  }

  int physical_byte_width() const {
    return std::is_same_v<c_type, FLBA> ? descr_->type_length()
                                        : static_cast<int>(sizeof(c_type));
  }
};

template <typename c_type>
static std::vector<c_type> ToLittleEndian(const std::vector<c_type>& input) {
  std::vector<c_type> data(input.size());
  std::transform(input.begin(), input.end(), data.begin(), [](const c_type& value) {
    return ::arrow::bit_util::ToLittleEndian(value);
  });
  return data;
}

std::shared_ptr<ColumnDescriptor> FLBAColumnDescriptor(int type_length) {
  auto node =
      schema::PrimitiveNode::Make("", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                                  ConvertedType::NONE, type_length);
  return std::make_shared<ColumnDescriptor>(std::move(node), /*max_definition_level=*/0,
                                            /*max_repetition_level=*/0);
}

template <typename Type>
void TestByteStreamSplitEncoding<Type>::CheckDecode() {
  if constexpr (std::is_same_v<c_type, FLBA>) {
    // FIXED_LEN_BYTE_ARRAY
    // - type_length = 3
    {
      const std::vector<uint8_t> data{0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                                      0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC};
      const std::vector<uint8_t> raw_expected_output{0x11, 0x55, 0x99, 0x22, 0x66, 0xAA,
                                                     0x33, 0x77, 0xBB, 0x44, 0x88, 0xCC};
      const std::vector<FLBA> expected_output{
          FLBA{&raw_expected_output[0]}, FLBA{&raw_expected_output[3]},
          FLBA{&raw_expected_output[6]}, FLBA{&raw_expected_output[9]}};
      CheckDecode(span{data}, span{expected_output}, FLBAColumnDescriptor(3).get());
    }
    // - type_length = 1
    {
      const std::vector<uint8_t> data{0x11, 0x22, 0x33};
      const std::vector<uint8_t> raw_expected_output{0x11, 0x22, 0x33};
      const std::vector<FLBA> expected_output{FLBA{&raw_expected_output[0]},
                                              FLBA{&raw_expected_output[1]},
                                              FLBA{&raw_expected_output[2]}};
      CheckDecode(span{data}, span{expected_output}, FLBAColumnDescriptor(1).get());
    }
  } else if constexpr (sizeof(c_type) == 4) {
    // INT32, FLOAT
    const std::vector<uint8_t> data{0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                                    0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC};
    const auto expected_output =
        ToLittleEndian<uint32_t>({0xAA774411U, 0xBB885522U, 0xCC996633U});
    CheckDecode(span{data}, span{expected_output});
  } else {
    // INT64, DOUBLE
    const std::vector<uint8_t> data{0xDE, 0xC0, 0x37, 0x13, 0x11, 0x22, 0x33, 0x44,
                                    0xAA, 0xBB, 0xCC, 0xDD, 0x55, 0x66, 0x77, 0x88};
    const auto expected_output =
        ToLittleEndian<uint64_t>({0x7755CCAA331137DEULL, 0x8866DDBB442213C0ULL});
    CheckDecode(span{data}, span{expected_output});
  }
}

template <typename Type>
void TestByteStreamSplitEncoding<Type>::CheckEncode() {
  if constexpr (std::is_same_v<c_type, FLBA>) {
    // FIXED_LEN_BYTE_ARRAY
    // - type_length = 3
    {
      const std::vector<uint8_t> raw_data{0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                                          0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC};
      const std::vector<FLBA> data{FLBA{&raw_data[0]}, FLBA{&raw_data[3]},
                                   FLBA{&raw_data[6]}, FLBA{&raw_data[9]}};
      const std::vector<uint8_t> expected_output{0x11, 0x44, 0x77, 0xAA, 0x22, 0x55,
                                                 0x88, 0xBB, 0x33, 0x66, 0x99, 0xCC};
      CheckEncode(span{data}, span{expected_output}, FLBAColumnDescriptor(3).get());
    }
    // - type_length = 1
    {
      const std::vector<uint8_t> raw_data{0x11, 0x22, 0x33};
      const std::vector<FLBA> data{FLBA{&raw_data[0]}, FLBA{&raw_data[1]},
                                   FLBA{&raw_data[2]}};
      const std::vector<uint8_t> expected_output{0x11, 0x22, 0x33};
      CheckEncode(span{data}, span{expected_output}, FLBAColumnDescriptor(1).get());
    }
  } else if constexpr (sizeof(c_type) == 4) {
    // INT32, FLOAT
    const auto data = ToLittleEndian<uint32_t>({0xaabbccddUL, 0x11223344UL});
    const std::vector<uint8_t> expected_output{0xdd, 0x44, 0xcc, 0x33,
                                               0xbb, 0x22, 0xaa, 0x11};
    CheckEncode(span{data}, span{expected_output});
  } else {
    // INT64, DOUBLE
    const auto data = ToLittleEndian<uint64_t>(
        {0x4142434445464748ULL, 0x0102030405060708ULL, 0xb1b2b3b4b5b6b7b8ULL});
    const std::vector<uint8_t> expected_output{
        0x48, 0x08, 0xb8, 0x47, 0x07, 0xb7, 0x46, 0x06, 0xb6, 0x45, 0x05, 0xb5,
        0x44, 0x04, 0xb4, 0x43, 0x03, 0xb3, 0x42, 0x02, 0xb2, 0x41, 0x01, 0xb1,
    };
    CheckEncode(span{data}, span{expected_output});
  }
}

using ByteStreamSplitTypes =
    ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType, FLBAType>;
TYPED_TEST_SUITE(TestByteStreamSplitEncoding, ByteStreamSplitTypes);

TYPED_TEST(TestByteStreamSplitEncoding, BasicRoundTrip) {
  for (int values = 0; values < 32; ++values) {
    ASSERT_NO_FATAL_FAILURE(this->Execute(values, 1));
  }

  // We need to test with different sizes to guarantee that the SIMD implementation
  // can handle both inputs with size divisible by 4/8 and sizes which would
  // require a scalar loop for the suffix.
  constexpr size_t kSuffixSize = 7;
  constexpr size_t kAvx2Size = 32;    // sizeof(__m256i) for AVX2
  constexpr size_t kAvx512Size = 64;  // sizeof(__m512i) for AVX512
  constexpr size_t kMultiSimdSize = kAvx512Size * 7;

  // Exercise only one SIMD loop. SSE and AVX2 covered in above loop.
  ASSERT_NO_FATAL_FAILURE(this->Execute(kAvx512Size, 1));
  // Exercise one SIMD loop with suffix. SSE covered in above loop.
  ASSERT_NO_FATAL_FAILURE(this->Execute(kAvx2Size + kSuffixSize, 1));
  ASSERT_NO_FATAL_FAILURE(this->Execute(kAvx512Size + kSuffixSize, 1));
  // Exercise multi SIMD loop.
  ASSERT_NO_FATAL_FAILURE(this->Execute(kMultiSimdSize, 1));
  // Exercise multi SIMD loop with suffix.
  ASSERT_NO_FATAL_FAILURE(this->Execute(kMultiSimdSize + kSuffixSize, 1));
}

TYPED_TEST(TestByteStreamSplitEncoding, RoundTripSingleElement) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(1, 1));
}

TYPED_TEST(TestByteStreamSplitEncoding, RoundTripSpace) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(10000, 1));

  // Spaced test with different sizes and offset to guarantee SIMD implementation
  constexpr int kAvx512Size = 64;         // sizeof(__m512i) for Avx512
  constexpr int kSimdSize = kAvx512Size;  // Current the max is Avx512
  constexpr int kMultiSimdSize = kSimdSize * 33;

  for (auto null_prob : {0.001, 0.1, 0.5, 0.9, 0.999}) {
    // Test with both size and offset up to 3 Simd block
    for (auto i = 1; i < kSimdSize * 3; i++) {
      ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(i, 1, 0, null_prob));
      ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(i, 1, i + 1, null_prob));
    }
    // Large block and offset
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize, 1, 0, null_prob));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize + 33, 1, 0, null_prob));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize, 1, 33, null_prob));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(kMultiSimdSize + 33, 1, 33, null_prob));
  }
}

TYPED_TEST(TestByteStreamSplitEncoding, CheckOnlyDecode) {
  ASSERT_NO_FATAL_FAILURE(this->CheckDecode());
}

TYPED_TEST(TestByteStreamSplitEncoding, CheckOnlyEncode) {
  ASSERT_NO_FATAL_FAILURE(this->CheckEncode());
}

TEST(ByteStreamSplitEncodeDecode, InvalidDataTypes) {
  // First check encoders.
  ASSERT_THROW(MakeTypedEncoder<Int96Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<BooleanType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<ByteArrayType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);

  // Then check decoders.
  ASSERT_THROW(MakeTypedDecoder<Int96Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<BooleanType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<ByteArrayType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
}

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED encode/decode tests.

template <typename Type>
class TestDeltaBitPackEncoding : public TestEncodingBase<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;
  static constexpr size_t kNumRoundTrips = 3;
  const std::vector<int> kReadBatchSizes = {1, 11};

  void InitBoundData(int nvalues, int repeats, c_type half_range) {
    num_values_ = nvalues * repeats;
    input_bytes_.resize(num_values_ * sizeof(c_type));
    output_bytes_.resize(num_values_ * sizeof(c_type));
    draws_ = reinterpret_cast<c_type*>(input_bytes_.data());
    decode_buf_ = reinterpret_cast<c_type*>(output_bytes_.data());
    GenerateBoundData<c_type>(nvalues, draws_, -half_range, half_range, &data_buffer_);

    // add some repeated values
    for (int j = 1; j < repeats; ++j) {
      for (int i = 0; i < nvalues; ++i) {
        draws_[nvalues * j + i] = draws_[i];
      }
    }
  }

  void ExecuteBound(int nvalues, int repeats, c_type half_range) {
    InitBoundData(nvalues, repeats, half_range);
    CheckRoundtrip();
  }

  void ExecuteSpacedBound(int nvalues, int repeats, int64_t valid_bits_offset,
                          double null_probability, c_type half_range) {
    InitBoundData(nvalues, repeats, half_range);

    int64_t size = num_values_ + valid_bits_offset;
    auto rand = ::arrow::random::RandomArrayGenerator(1923);
    const auto array = rand.UInt8(size, 0, 100, null_probability);
    const auto valid_bits = array->null_bitmap_data();
    CheckRoundtripSpaced(valid_bits, valid_bits_offset);
  }

  void CheckDecoding() {
    auto decoder = MakeTypedDecoder<Type>(Encoding::DELTA_BINARY_PACKED, descr_.get());
    auto read_batch_sizes = kReadBatchSizes;
    read_batch_sizes.push_back(num_values_);
    // Exercise different batch sizes
    for (const int read_batch_size : read_batch_sizes) {
      decoder->SetData(num_values_, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));

      int values_decoded = 0;
      while (values_decoded < num_values_) {
        values_decoded += decoder->Decode(decode_buf_ + values_decoded, read_batch_size);
      }
      ASSERT_EQ(num_values_, values_decoded);
      ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
    }
  }

  void CheckRoundtrip() override {
    auto encoder = MakeTypedEncoder<Type>(Encoding::DELTA_BINARY_PACKED,
                                          /*use_dictionary=*/false, descr_.get());
    // Encode a number of times to exercise the flush logic
    for (size_t i = 0; i < kNumRoundTrips; ++i) {
      encoder->Put(draws_, num_values_);
      encode_buffer_ = encoder->FlushValues();
      CheckDecoding();
    }
  }

  void CheckRoundtripSpaced(const uint8_t* valid_bits,
                            int64_t valid_bits_offset) override {
    auto encoder = MakeTypedEncoder<Type>(Encoding::DELTA_BINARY_PACKED,
                                          /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(Encoding::DELTA_BINARY_PACKED, descr_.get());
    int null_count = 0;
    for (auto i = 0; i < num_values_; i++) {
      if (!bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
        null_count++;
      }
    }

    for (size_t i = 0; i < kNumRoundTrips; ++i) {
      encoder->PutSpaced(draws_, num_values_, valid_bits, valid_bits_offset);
      encode_buffer_ = encoder->FlushValues();
      decoder->SetData(num_values_, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));
      auto values_decoded = decoder->DecodeSpaced(decode_buf_, num_values_, null_count,
                                                  valid_bits, valid_bits_offset);
      ASSERT_EQ(num_values_, values_decoded);
      ASSERT_NO_FATAL_FAILURE(VerifyResultsSpaced<c_type>(
          decode_buf_, draws_, num_values_, valid_bits, valid_bits_offset));
    }
  }

 protected:
  USING_BASE_MEMBERS();
  std::vector<uint8_t> input_bytes_;
  std::vector<uint8_t> output_bytes_;
};

using TestDeltaBitPackEncodingTypes = ::testing::Types<Int32Type, Int64Type>;
TYPED_TEST_SUITE(TestDeltaBitPackEncoding, TestDeltaBitPackEncodingTypes);

TYPED_TEST(TestDeltaBitPackEncoding, BasicRoundTrip) {
  using T = typename TypeParam::c_type;
  int values_per_block = 128;
  int values_per_mini_block = 32;

  // Size a multiple of miniblock size
  ASSERT_NO_FATAL_FAILURE(this->Execute(values_per_mini_block * 10, 10));
  // Size a multiple of block size
  ASSERT_NO_FATAL_FAILURE(this->Execute(values_per_block * 10, 10));
  // Size multiple of neither miniblock nor block size
  ASSERT_NO_FATAL_FAILURE(
      this->Execute((values_per_mini_block * values_per_block) + 1, 10));
  ASSERT_NO_FATAL_FAILURE(this->Execute(0, 0));
  ASSERT_NO_FATAL_FAILURE(this->Execute(65, 1));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 0.1));

  // All identical values
  ASSERT_NO_FATAL_FAILURE(
      this->ExecuteBound(/*nvalues*/ 2000, /*repeats*/ 50, /*half_range*/ 0));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpacedBound(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 0.1,
      /*half_range*/ 0));

  // Various delta bitwidths, including the full datatype width
  const int max_bitwidth = sizeof(T) * 8;
  std::vector<int> bitwidths = {
      1, 2, 3, 5, 8, 11, 16, max_bitwidth - 8, max_bitwidth - 1, max_bitwidth};
  for (int bitwidth : bitwidths) {
    T half_range =
        std::numeric_limits<T>::max() >> static_cast<uint32_t>(max_bitwidth - bitwidth);

    ASSERT_NO_FATAL_FAILURE(
        this->ExecuteBound(/*nvalues*/ 2000, /*repeats*/ 50, half_range));
    ASSERT_NO_FATAL_FAILURE(this->ExecuteSpacedBound(
        /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
        /*null_probability*/ 0.1,
        /*half_range*/ half_range));
  }
}

TYPED_TEST(TestDeltaBitPackEncoding, NonZeroPaddedMiniblockBitWidth) {
  // GH-14923: depending on the number of encoded values, some of the miniblock
  // bitwidths are actually padding bytes that may take non-conformant values
  // according to the Parquet spec.

  // Same values as in DeltaBitPackEncoder
  constexpr int kValuesPerBlock =
      std::is_same_v<int32_t, typename TypeParam::c_type> ? 128 : 256;
  constexpr int kMiniBlocksPerBlock = 4;
  constexpr int kValuesPerMiniBlock = kValuesPerBlock / kMiniBlocksPerBlock;

  // num_values must be kept small enough for kHeaderLength below
  for (const int num_values : {2, 62, 63, 64, 65, 95, 96, 97, 127}) {
    ARROW_SCOPED_TRACE("num_values = ", num_values);

    // Generate input data with a small half_range to make the header length
    // deterministic (see kHeaderLength).
    this->InitBoundData(num_values, /*repeats=*/1, /*half_range=*/31);
    ASSERT_EQ(this->num_values_, num_values);

    auto encoder = MakeTypedEncoder<TypeParam>(
        Encoding::DELTA_BINARY_PACKED, /*use_dictionary=*/false, this->descr_.get());
    encoder->Put(this->draws_, this->num_values_);
    auto encoded = encoder->FlushValues();
    const auto encoded_size = encoded->size();

    // Make mutable copy of encoded buffer
    this->encode_buffer_ = AllocateBuffer(default_memory_pool(), encoded_size);
    uint8_t* data = this->encode_buffer_->mutable_data();
    memcpy(data, encoded->data(), encoded_size);

    // The number of padding bytes at the end of the miniblock bitwidths array.
    // We subtract 1 from num_values because the first data value is encoded
    // in the header, thus does not participate in miniblock encoding.
    const int num_padding_bytes =
        (kValuesPerBlock - num_values + 1) / kValuesPerMiniBlock;
    ARROW_SCOPED_TRACE("num_padding_bytes = ", num_padding_bytes);

    // The header length is:
    // - 2 bytes for ULEB128-encoded block size (== kValuesPerBlock)
    // - 1 byte for ULEB128-encoded miniblocks per block (== kMiniBlocksPerBlock)
    // - 1 byte for ULEB128-encoded num_values
    // - 1 byte for ULEB128-encoded first value
    // (this assumes that num_values and the first value are narrow enough)
    constexpr int kHeaderLength = 5;
    // After the header, there is a zigzag ULEB128-encoded min delta for the first block,
    // then the miniblock bitwidths for the first block.
    // Given a narrow enough range, the zigzag ULEB128-encoded min delta is 1 byte long.
    uint8_t* mini_block_bitwidths = data + kHeaderLength + 1;

    // Garble padding bytes; decoding should succeed.
    for (int i = 0; i < num_padding_bytes; ++i) {
      mini_block_bitwidths[kMiniBlocksPerBlock - i - 1] = 0xFFU;
    }
    ASSERT_NO_THROW(this->CheckDecoding());

    // Not a padding byte but an actual miniblock bitwidth; decoding should error out.
    mini_block_bitwidths[kMiniBlocksPerBlock - num_padding_bytes - 1] = 0xFFU;
    EXPECT_THROW(this->CheckDecoding(), ParquetException);
  }
}

// Test that the DELTA_BINARY_PACKED encoding works properly in the presence of values
// that will cause integer overflow (see GH-37939).
TYPED_TEST(TestDeltaBitPackEncoding, DeltaBitPackedWrapping) {
  using T = typename TypeParam::c_type;

  // Values that should wrap when converted to deltas, and then when converted to the
  // frame of reference.
  std::vector<T> int_values = {std::numeric_limits<T>::min(),
                               std::numeric_limits<T>::max(),
                               std::numeric_limits<T>::min(),
                               std::numeric_limits<T>::max(),
                               0,
                               -1,
                               0,
                               1,
                               -1,
                               1};
  const int num_values = static_cast<int>(int_values.size());

  auto const encoder = MakeTypedEncoder<TypeParam>(
      Encoding::DELTA_BINARY_PACKED, /*use_dictionary=*/false, this->descr_.get());
  encoder->Put(int_values, num_values);
  auto const encoded = encoder->FlushValues();

  auto const decoder =
      MakeTypedDecoder<TypeParam>(Encoding::DELTA_BINARY_PACKED, this->descr_.get());

  std::vector<T> decoded(num_values);
  decoder->SetData(num_values, encoded->data(), static_cast<int>(encoded->size()));

  const int values_decoded = decoder->Decode(decoded.data(), num_values);

  ASSERT_EQ(num_values, values_decoded);
  ASSERT_NO_FATAL_FAILURE(
      VerifyResults<T>(decoded.data(), int_values.data(), num_values));
}

// Test that the DELTA_BINARY_PACKED encoding does not use more bits to encode than
// necessary (see GH-37939).
TYPED_TEST(TestDeltaBitPackEncoding, DeltaBitPackedSize) {
  using T = typename TypeParam::c_type;
  constexpr int num_values = 128;
  // 128 values should be <= 1 block of values encoded with 2 bits
  // delta header should be 0x8001|0x8002 0x04 0x8001 0x02 (6 bytes)
  // mini-block header should be 0x01 0x02020202 (5 bytes)
  constexpr int encoded_size = 2 * num_values / 8 + 6 + 5;

  // Create a run of {1, 0, -1, 0, 1, 0, ...}.
  // min_delta is -1, max_delta is 1, max_delta - min_delta is 2, so this requires 2 bits
  // to encode.
  std::vector<T> int_values(num_values);
  std::iota(int_values.begin(), int_values.end(), 0);
  std::transform(int_values.begin(), int_values.end(), int_values.begin(), [](T idx) {
    return (idx % 2) == 1 ? 0 : (idx % 4) == 0 ? 1 : -1;
  });

  auto const encoder = MakeTypedEncoder<TypeParam>(
      Encoding::DELTA_BINARY_PACKED, /*use_dictionary=*/false, this->descr_.get());
  encoder->Put(int_values, num_values);
  auto const encoded = encoder->FlushValues();

  ASSERT_EQ(encoded->size(), encoded_size);
}

// ----------------------------------------------------------------------
// Rle for Boolean encode/decode tests.

class TestRleBooleanEncoding : public TestEncodingBase<BooleanType> {
 public:
  using c_type = bool;
  static constexpr int TYPE = Type::BOOLEAN;

  virtual void CheckRoundtrip() {
    auto encoder = MakeTypedEncoder<BooleanType>(Encoding::RLE,
                                                 /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<BooleanType>(Encoding::RLE, descr_.get());

    for (int i = 0; i < 3; ++i) {
      encoder->Put(draws_, num_values_);
      encode_buffer_ = encoder->FlushValues();

      decoder->SetData(num_values_, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));
      int values_decoded = decoder->Decode(decode_buf_, num_values_);
      ASSERT_EQ(num_values_, values_decoded);
      ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
    }
  }

  void CheckRoundtripSpaced(const uint8_t* valid_bits, int64_t valid_bits_offset) {
    auto encoder = MakeTypedEncoder<BooleanType>(Encoding::RLE,
                                                 /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<BooleanType>(Encoding::RLE, descr_.get());
    int null_count = 0;
    for (auto i = 0; i < num_values_; i++) {
      if (!bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
        null_count++;
      }
    }
    for (int i = 0; i < 3; ++i) {
      encoder->PutSpaced(draws_, num_values_, valid_bits, valid_bits_offset);
      encode_buffer_ = encoder->FlushValues();
      decoder->SetData(num_values_ - null_count, encode_buffer_->data(),
                       static_cast<int>(encode_buffer_->size()));
      auto values_decoded = decoder->DecodeSpaced(decode_buf_, num_values_, null_count,
                                                  valid_bits, valid_bits_offset);
      ASSERT_EQ(num_values_, values_decoded);
      ASSERT_NO_FATAL_FAILURE(VerifyResultsSpaced<c_type>(
          decode_buf_, draws_, num_values_, valid_bits, valid_bits_offset));
    }
  }
};

TEST_F(TestRleBooleanEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(0, 0));
  ASSERT_NO_FATAL_FAILURE(this->Execute(2000, 200));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 0.1));
}

TEST_F(TestRleBooleanEncoding, AllNull) {
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 1));
}

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY encode/decode tests.

template <typename Type>
class TestDeltaLengthByteArrayEncoding : public TestEncodingBase<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;

  virtual Encoding::type GetEncoding() { return Encoding::DELTA_LENGTH_BYTE_ARRAY; }

  virtual void CheckRoundtrip() {
    auto encoding = GetEncoding();
    auto encoder = MakeTypedEncoder<Type>(encoding,
                                          /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(encoding, descr_.get());

    encoder->Put(draws_, num_values_);
    encode_buffer_ = encoder->FlushValues();
    if constexpr (std::is_same_v<Type, ByteArrayType>) {
      ASSERT_EQ(encoder->ReportUnencodedDataBytes(),
                this->unencoded_byte_array_data_bytes_);
    }

    decoder->SetData(num_values_, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    int values_decoded = decoder->Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
  }

  void CheckRoundtripSpaced(const uint8_t* valid_bits, int64_t valid_bits_offset) {
    auto encoding = GetEncoding();
    auto encoder = MakeTypedEncoder<Type>(encoding,
                                          /*use_dictionary=*/false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(encoding, descr_.get());
    int null_count = 0;
    for (auto i = 0; i < num_values_; i++) {
      if (!bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
        null_count++;
      }
    }

    encoder->PutSpaced(draws_, num_values_, valid_bits, valid_bits_offset);
    encode_buffer_ = encoder->FlushValues();
    decoder->SetData(num_values_, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    auto values_decoded = decoder->DecodeSpaced(decode_buf_, num_values_, null_count,
                                                valid_bits, valid_bits_offset);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResultsSpaced<c_type>(decode_buf_, draws_, num_values_,
                                                        valid_bits, valid_bits_offset));
  }

 protected:
  USING_BASE_MEMBERS();
};

typedef ::testing::Types<ByteArrayType> TestDeltaLengthByteArrayEncodingTypes;
TYPED_TEST_SUITE(TestDeltaLengthByteArrayEncoding, TestDeltaLengthByteArrayEncodingTypes);

TYPED_TEST(TestDeltaLengthByteArrayEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(0, 0));
  ASSERT_NO_FATAL_FAILURE(this->Execute(2000, 200));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 0.1));
}

TYPED_TEST(TestDeltaLengthByteArrayEncoding, AllNulls) {
  this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 1);
}

std::shared_ptr<Buffer> DeltaEncode(std::vector<int32_t> lengths) {
  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::DELTA_BINARY_PACKED);
  encoder->Put(lengths.data(), static_cast<int>(lengths.size()));
  return encoder->FlushValues();
}

std::shared_ptr<Buffer> DeltaEncode(::arrow::util::span<const int32_t> lengths) {
  auto encoder = MakeTypedEncoder<Int32Type>(Encoding::DELTA_BINARY_PACKED);
  encoder->Put(lengths.data(), static_cast<int>(lengths.size()));
  return encoder->FlushValues();
}

std::shared_ptr<Buffer> DeltaEncode(std::shared_ptr<::arrow::Array>& lengths) {
  auto data = ::arrow::internal::checked_pointer_cast<const ::arrow::Int32Array>(lengths);
  auto span = ::arrow::util::span<const int32_t>{data->raw_values(),
                                                 static_cast<size_t>(lengths->length())};
  return DeltaEncode(span);
}

TEST(TestDeltaLengthByteArrayEncoding, AdHocRoundTrip) {
  const std::shared_ptr<::arrow::Array> cases[] = {
      ::arrow::ArrayFromJSON(::arrow::utf8(), R"([])"),
      ::arrow::ArrayFromJSON(::arrow::utf8(), R"(["abc", "de", ""])"),
      ::arrow::ArrayFromJSON(::arrow::utf8(), R"(["", "", ""])"),
      ::arrow::ArrayFromJSON(::arrow::utf8(), R"(["abc", "", "xyz"])")->Slice(1),
  };

  std::string expected_encoded_vals[] = {
      DeltaEncode(std::vector<int>({}))->ToString(),
      DeltaEncode(std::vector<int>({3, 2, 0}))->ToString() + "abcde",
      DeltaEncode(std::vector<int>({0, 0, 0}))->ToString(),
      DeltaEncode(std::vector<int>({0, 3}))->ToString() + "xyz",
  };

  auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                                 /*use_dictionary=*/false);
  auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);
  for (int i = 0; i < 4; ++i) {
    auto array = cases[i];
    auto expected_encoded = expected_encoded_vals[i];

    encoder->Put(*array);
    std::shared_ptr<Buffer> buffer = encoder->FlushValues();

    ASSERT_EQ(expected_encoded, buffer->ToString());

    int num_values = static_cast<int>(array->length() - array->null_count());
    decoder->SetData(num_values, buffer->data(), static_cast<int>(buffer->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    acc.builder.reset(new ::arrow::StringBuilder);

    int values_decoded = decoder->DecodeArrow(num_values, 0, nullptr, 0, &acc);
    ASSERT_EQ(values_decoded, num_values);

    std::shared_ptr<::arrow::Array> roundtripped_array;
    ASSERT_OK(acc.builder->Finish(&roundtripped_array));
    ASSERT_ARRAYS_EQUAL(*array, *roundtripped_array);
  }
}

TEST(DeltaLengthByteArrayEncoding, RejectBadBuffer) {
  auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);

  // Missing length delta header
  Buffer empty_buffer = Buffer("");
  ASSERT_THROW(decoder->SetData(10, empty_buffer.data(), 0), ParquetException);

  // Incomplete header
  std::shared_ptr<Buffer> partial_buffer =
      DeltaEncode({3, 2, 0})->CopySlice(0, 2).ValueOrDie();
  ASSERT_THROW(decoder->SetData(10, partial_buffer->data(),
                                static_cast<int>(partial_buffer->size())),
               ParquetException);

  typename EncodingTraits<ByteArrayType>::Accumulator acc;
  acc.builder.reset(new ::arrow::StringBuilder);

  // buffer only has lengths
  std::shared_ptr<Buffer> buffer = DeltaEncode({3, 2, 0});
  decoder->SetData(3, buffer->data(), static_cast<int>(buffer->size()));
  acc.builder.reset(new ::arrow::StringBuilder);
  ASSERT_THROW(decoder->DecodeArrow(3, 0, nullptr, 0, &acc), ParquetException);

  // bytes are too short for lengths
  std::shared_ptr<Buffer> short_buffer =
      ::arrow::ConcatenateBuffers(
          {DeltaEncode({2, 4, 6}), std::make_shared<Buffer>("short")})
          .ValueOrDie();
  decoder->SetData(3, short_buffer->data(), static_cast<int>(short_buffer->size()));
  acc.builder.reset(new ::arrow::StringBuilder);
  ASSERT_THROW(decoder->DecodeArrow(3, 0, nullptr, 0, &acc), ParquetException);
}

TEST(DeltaLengthByteArrayEncodingAdHoc, ArrowBinaryDirectPut) {
  const int64_t size = 50;
  const int32_t min_length = 0;
  const int32_t max_length = 10;
  const int32_t num_unique = 10;
  const double null_probability = 0.25;
  auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);
  auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);

  auto CheckRoundtrip = [&](std::shared_ptr<::arrow::Array> values,
                            int64_t total_data_size) {
    ASSERT_NO_THROW(encoder->Put(*values));
    // For DeltaLength encoding, the estimated size should be at least the total byte size
    EXPECT_GE(encoder->EstimatedDataEncodedSize(), total_data_size)
        << "Estimated size should be at least the total byte size";
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    ASSERT_OK_AND_ASSIGN(acc.builder, ::arrow::MakeBuilder(values->type()));
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(values->length(), result->length());
    ASSERT_OK(result->ValidateFull());

    ::arrow::AssertArraysEqual(*values, *result);
  };

  auto CheckRoundtripAllTypes = [&](std::shared_ptr<::arrow::Array> values) {
    auto* binary_array = checked_cast<const ::arrow::BinaryArray*>(values.get());
    const auto total_data_size = binary_array->total_values_length();
    for (auto type : binary_like_types_for_dense_decoding()) {
      ARROW_SCOPED_TRACE("type = ", *type);
      ASSERT_OK_AND_ASSIGN(auto cast_values, ::arrow::compute::Cast(*values, type));
      CheckRoundtrip(cast_values, total_data_size);
    }
  };

  ::arrow::random::RandomArrayGenerator rag(42);
  auto values = rag.String(0, min_length, max_length, null_probability);
  CheckRoundtripAllTypes(values);
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    rag = ::arrow::random::RandomArrayGenerator(seed);

    values = rag.String(size, min_length, max_length, null_probability);
    CheckRoundtripAllTypes(values);

    values =
        rag.BinaryWithRepeats(size, num_unique, min_length, max_length, null_probability);
    CheckRoundtripAllTypes(values);
  }
}

TEST(DeltaLengthByteArrayEncodingAdHoc, ArrowDirectPut) {
  auto CheckEncode = [](std::shared_ptr<::arrow::Array> values,
                        std::shared_ptr<::arrow::Array> lengths) {
    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();

    auto lengths_encoder = MakeTypedEncoder<Int32Type>(Encoding::DELTA_BINARY_PACKED);
    ASSERT_NO_THROW(lengths_encoder->Put(*lengths));
    auto lengths_buf = lengths_encoder->FlushValues();

    auto encoded_lengths_buf = SliceBuffer(buf, 0, lengths_buf->size());
    auto encoded_values_buf = SliceBuffer(buf, lengths_buf->size());

    ASSERT_TRUE(encoded_lengths_buf->Equals(*lengths_buf));
    if (::arrow::is_base_binary_like(values->type_id())) {
      // The encoded values (after the delta-encoded lengths) must be the raw data,
      // which is also the data buffer of a Binary / LargeBinary array.
      ASSERT_TRUE(encoded_values_buf->Equals(*values->data()->buffers[2]));
    }
  };

  auto CheckDecode = [](std::shared_ptr<Buffer> buf,
                        std::shared_ptr<::arrow::Array> values) {
    int num_values = static_cast<int>(values->length());
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    ASSERT_OK_AND_ASSIGN(acc.builder, ::arrow::MakeBuilder(values->type()));
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(num_values, result->length());
    ASSERT_OK(result->ValidateFull());

    ::arrow::AssertArraysEqual(*values, *result);
  };

  auto values = R"(["Hello", "World", "Foobar", "ADBCEF"])";
  auto lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([5, 5, 6, 6])");
  auto encoded =
      ::arrow::ConcatenateBuffers(
          {DeltaEncode({5, 5, 6, 6}), std::make_shared<Buffer>("HelloWorldFoobarADBCEF")})
          .ValueOrDie();

  auto types = binary_like_types_for_dense_decoding();

  for (const auto& type : types) {
    ARROW_SCOPED_TRACE("type = ", *type);
    auto array = ::arrow::ArrayFromJSON(type, values);
    CheckEncode(array, lengths);
    CheckDecode(encoded, array);
  }
}

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY encode/decode tests.

template <typename Type>
class TestDeltaByteArrayEncoding : public TestDeltaLengthByteArrayEncoding<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;
  static constexpr double prefixed_probability = 0.5;

  void InitData(int nvalues, int repeats) override {
    num_values_ = nvalues * repeats;
    input_bytes_.resize(num_values_ * sizeof(c_type));
    output_bytes_.resize(num_values_ * sizeof(c_type));
    draws_ = reinterpret_cast<c_type*>(input_bytes_.data());
    decode_buf_ = reinterpret_cast<c_type*>(output_bytes_.data());
    GeneratePrefixedData<c_type>(nvalues, draws_, &data_buffer_, prefixed_probability);

    // add some repeated values
    for (int j = 1; j < repeats; ++j) {
      for (int i = 0; i < nvalues; ++i) {
        draws_[nvalues * j + i] = draws_[i];
      }
    }

    TestEncodingBase<Type>::InitUnencodedByteArrayDataBytes();
  }

  Encoding::type GetEncoding() override { return Encoding::DELTA_BYTE_ARRAY; }

 protected:
  USING_BASE_MEMBERS();
  std::vector<uint8_t> input_bytes_;
  std::vector<uint8_t> output_bytes_;
};

using TestDeltaByteArrayEncodingTypes = ::testing::Types<ByteArrayType, FLBAType>;
TYPED_TEST_SUITE(TestDeltaByteArrayEncoding, TestDeltaByteArrayEncodingTypes);

TYPED_TEST(TestDeltaByteArrayEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(0, /*repeats=*/0));
  ASSERT_NO_FATAL_FAILURE(this->Execute(250, 5));
  ASSERT_NO_FATAL_FAILURE(this->Execute(2000, 1));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64, /*null_probability*/
      0));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 10, /*valid_bits_offset*/ 64,
      /*null_probability*/ 0.5));
}

template <typename Type>
class TestDeltaByteArrayEncodingDirectPut : public TestEncodingBase<Type> {
  using Accumulator = typename EncodingTraits<Type>::Accumulator;

 public:
  void MakeEncoderDecoder(const ::arrow::DataType& type) {
    schema::NodePtr node;
    if constexpr (std::is_same_v<Type, FLBAType>) {
      node = schema::PrimitiveNode::Make(
          "name", Repetition::OPTIONAL, ::parquet::Type::FIXED_LEN_BYTE_ARRAY,
          ConvertedType::NONE,
          checked_cast<const ::arrow::FixedSizeBinaryType&>(type).byte_width());
    } else {
      static_assert(std::is_same_v<Type, ByteArrayType>);
      node = schema::PrimitiveNode::Make("name", Repetition::OPTIONAL,
                                         ::parquet::Type::BYTE_ARRAY);
    }
    this->descr_ = std::make_shared<ColumnDescriptor>(node, 0, 0);
    this->encoder = MakeTypedEncoder<Type>(Encoding::DELTA_BYTE_ARRAY,
                                           /*use_dictionary=*/false, this->descr_.get());
    this->decoder =
        MakeTypedDecoder<Type>(Encoding::DELTA_BYTE_ARRAY, this->descr_.get());
  }

  std::unique_ptr<TypedEncoder<Type>> encoder;
  std::unique_ptr<TypedDecoder<Type>> decoder;

  void CheckDirectPut(std::shared_ptr<::arrow::Array> array);

  void CheckRoundtrip() override;

 protected:
  USING_BASE_MEMBERS();
};

template <>
void TestDeltaByteArrayEncodingDirectPut<ByteArrayType>::CheckDirectPut(
    std::shared_ptr<::arrow::Array> array) {
  MakeEncoderDecoder(*array->type());

  ASSERT_NO_THROW(encoder->Put(*array));
  auto buf = encoder->FlushValues();

  int num_values = static_cast<int>(array->length() - array->null_count());
  decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

  Accumulator acc;
  ASSERT_OK_AND_ASSIGN(acc.builder, ::arrow::MakeBuilder(array->type()));

  ASSERT_EQ(num_values,
            decoder->DecodeArrow(static_cast<int>(array->length()),
                                 static_cast<int>(array->null_count()),
                                 array->null_bitmap_data(), array->offset(), &acc));

  ASSERT_EQ(acc.chunks.size(), 0) << "Accumulator shouldn't have overflowed chunks";
  ASSERT_OK_AND_ASSIGN(auto result, acc.builder->Finish());
  ASSERT_EQ(array->length(), result->length());
  ASSERT_OK(result->ValidateFull());

  ::arrow::AssertArraysEqual(*array, *result);
}

template <>
void TestDeltaByteArrayEncodingDirectPut<FLBAType>::CheckDirectPut(
    std::shared_ptr<::arrow::Array> array) {
  MakeEncoderDecoder(*array->type());

  ASSERT_NO_THROW(encoder->Put(*array));
  auto buf = encoder->FlushValues();

  int num_values = static_cast<int>(array->length() - array->null_count());
  decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

  Accumulator acc(array->type(), default_memory_pool());

  ASSERT_EQ(num_values,
            decoder->DecodeArrow(static_cast<int>(array->length()),
                                 static_cast<int>(array->null_count()),
                                 array->null_bitmap_data(), array->offset(), &acc));

  ASSERT_OK_AND_ASSIGN(auto result, acc.Finish());
  ASSERT_EQ(array->length(), result->length());
  ASSERT_OK(result->ValidateFull());

  ::arrow::AssertArraysEqual(*array, *result);
}

template <>
void TestDeltaByteArrayEncodingDirectPut<ByteArrayType>::CheckRoundtrip() {
  constexpr int64_t kSize = 500;
  constexpr int32_t kMinLength = 0;
  constexpr int32_t kMaxLength = 10;
  constexpr int32_t kNumUnique = 10;
  constexpr double kNullProbability = 0.25;
  constexpr int kSeed = 42;
  ::arrow::random::RandomArrayGenerator rag{kSeed};
  std::shared_ptr<::arrow::Array> values = rag.BinaryWithRepeats(
      /*size=*/1, /*unique=*/1, kMinLength, kMaxLength, kNullProbability);
  CheckDirectPut(values);

  for (int i = 0; i < 10; ++i) {
    values = rag.BinaryWithRepeats(kSize, kNumUnique, kMinLength, kMaxLength,
                                   kNullProbability);
    CheckDirectPut(values);
  }
}

template <>
void TestDeltaByteArrayEncodingDirectPut<FLBAType>::CheckRoundtrip() {
  constexpr int64_t kSize = 50;
  constexpr int kSeed = 42;
  constexpr int kByteWidth = 4;
  ::arrow::random::RandomArrayGenerator rag{kSeed};
  std::shared_ptr<::arrow::Array> values =
      rag.FixedSizeBinary(/*size=*/0, /*byte_width=*/kByteWidth);
  CheckDirectPut(values);

  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    values = rag.FixedSizeBinary(kSize + seed, kByteWidth);
    CheckDirectPut(values);
  }
}

TYPED_TEST_SUITE(TestDeltaByteArrayEncodingDirectPut, TestDeltaByteArrayEncodingTypes);

TYPED_TEST(TestDeltaByteArrayEncodingDirectPut, DirectPut) {
  ASSERT_NO_FATAL_FAILURE(this->CheckRoundtrip());
}

TEST(DeltaByteArrayEncodingAdHoc, ArrowDirectPut) {
  auto CheckEncode = [](const std::shared_ptr<::arrow::Array>& values,
                        const std::shared_ptr<Buffer>& encoded) {
    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::DELTA_BYTE_ARRAY);
    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();
    ASSERT_TRUE(encoded->Equals(*buf));
  };

  auto CheckDecode = [](std::shared_ptr<Buffer> buf,
                        std::shared_ptr<::arrow::Array> values) {
    int num_values = static_cast<int>(values->length());
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_BYTE_ARRAY);
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    ASSERT_OK_AND_ASSIGN(acc.builder, ::arrow::MakeBuilder(values->type()));
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(num_values, result->length());
    ASSERT_OK(result->ValidateFull());

    ::arrow::AssertArraysEqual(*values, *result);
  };

  auto CheckEncodeDecode = [&](std::string_view values,
                               std::shared_ptr<::arrow::Array> prefix_lengths,
                               std::shared_ptr<::arrow::Array> suffix_lengths,
                               std::string_view suffix_data) {
    auto encoded = ::arrow::ConcatenateBuffers({DeltaEncode(prefix_lengths),
                                                DeltaEncode(suffix_lengths),
                                                std::make_shared<Buffer>(suffix_data)})
                       .ValueOrDie();

    auto types = binary_like_types_for_dense_decoding();

    for (const auto& type : types) {
      ARROW_SCOPED_TRACE("type = ", *type);
      auto values_array = ::arrow::ArrayFromJSON(::arrow::utf8(), values);
      CheckEncode(values_array, encoded);
      CheckDecode(encoded, values_array);
    }
  };

  {
    auto values = R"(["axis", "axle", "babble", "babyhood"])";
    auto prefix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([0, 2, 0, 3])");
    auto suffix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([4, 2, 6, 5])");

    constexpr std::string_view suffix_data = "axislebabbleyhood";
    CheckEncodeDecode(values, prefix_lengths, suffix_lengths, suffix_data);
  }

  {
    auto values = R"(["axis", "axis", "axis", "axis"])";
    auto prefix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([0, 4, 4, 4])");
    auto suffix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([4, 0, 0, 0])");

    constexpr std::string_view suffix_data = "axis";
    CheckEncodeDecode(values, prefix_lengths, suffix_lengths, suffix_data);
  }

  {
    auto values = R"(["axisba", "axis", "axis", "axis"])";
    auto prefix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([0, 4, 4, 4])");
    auto suffix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([6, 0, 0, 0])");

    constexpr std::string_view suffix_data = "axisba";
    CheckEncodeDecode(values, prefix_lengths, suffix_lengths, suffix_data);
  }

  {
    auto values = R"(["baaxis", "axis", "axis", "axis"])";
    auto prefix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([0, 0, 4, 4])");
    auto suffix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([6, 4, 0, 0])");

    constexpr std::string_view suffix_data = "baaxisaxis";
    CheckEncodeDecode(values, prefix_lengths, suffix_lengths, suffix_data);
  }

  {
    auto values = R"(["καλημέρα", "καμηλιέρη", "καμηλιέρη", "καλημέρα"])";
    auto prefix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([0, 5, 18, 5])");
    auto suffix_lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([16, 13, 0, 11])");
    const std::string suffix_data = "καλημέρα\xbcηλιέρη\xbbημέρα";
    CheckEncodeDecode(values, prefix_lengths, suffix_lengths, suffix_data);
  }
}

}  // namespace parquet::test
