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
#include "arrow/compute/cast.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/string.h"
#include "parquet/encoding.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;
using arrow::internal::checked_cast;

namespace bit_util = arrow::bit_util;

namespace parquet {

namespace test {

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

// ----------------------------------------------------------------------
// test data generation

template <typename T>
void GenerateData(int num_values, T* out, std::vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_numbers(num_values, 0, std::numeric_limits<T>::min(),
                 std::numeric_limits<T>::max(), out);
}

template <typename T>
void GenerateBoundData(int num_values, T* out, T min, T max, std::vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_numbers(num_values, 0, min, max, out);
}

template <>
void GenerateData<bool>(int num_values, bool* out, std::vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_bools(num_values, 0.5, 0, out);
}

template <>
void GenerateData<Int96>(int num_values, Int96* out, std::vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_Int96_numbers(num_values, 0, std::numeric_limits<int32_t>::min(),
                       std::numeric_limits<int32_t>::max(), out);
}

template <>
void GenerateData<ByteArray>(int num_values, ByteArray* out, std::vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  int max_byte_array_len = 12;
  heap->resize(num_values * max_byte_array_len);
  random_byte_array(num_values, 0, heap->data(), out, 2, max_byte_array_len);
}

static int flba_length = 8;

template <>
void GenerateData<FLBA>(int num_values, FLBA* out, std::vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  heap->resize(num_values * flba_length);
  random_fixed_byte_array(num_values, 0, heap->data(), flba_length, out);
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
    ASSERT_EQ(0, memcmp(expected[i].ptr, result[i].ptr, flba_length)) << i;
  }
}

template <>
void VerifyResultsSpaced<FLBA>(FLBA* result, FLBA* expected, int num_values,
                               const uint8_t* valid_bits, int64_t valid_bits_offset) {
  for (auto i = 0; i < num_values; ++i) {
    if (bit_util::GetBit(valid_bits, valid_bits_offset + i)) {
      ASSERT_EQ(0, memcmp(expected[i].ptr, result[i].ptr, flba_length)) << i;
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
  auto node = schema::PrimitiveNode::Make("name", Repetition::OPTIONAL,
                                          Type::FIXED_LEN_BYTE_ARRAY,
                                          ConvertedType::DECIMAL, flba_length, 10, 2);
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
    allocator_ = default_memory_pool();
  }

  void TearDown() {}

  void InitData(int nvalues, int repeats) {
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

class TestArrowBuilderDecoding : public ::testing::Test {
 public:
  using DenseBuilder = ::arrow::internal::ChunkedBinaryBuilder;
  using DictBuilder = ::arrow::BinaryDictionary32Builder;

  void SetUp() override { null_probabilities_ = {0.0, 0.5, 1.0}; }
  void TearDown() override {}

  void InitTestCase(double null_probability) {
    GenerateInputData(null_probability);
    SetupEncoderDecoder();
  }

  void GenerateInputData(double null_probability) {
    constexpr int num_unique = 100;
    constexpr int repeat = 100;
    constexpr int64_t min_length = 2;
    constexpr int64_t max_length = 10;
    ::arrow::random::RandomArrayGenerator rag(0);
    expected_dense_ = rag.BinaryWithRepeats(repeat * num_unique, num_unique, min_length,
                                            max_length, null_probability);

    num_values_ = static_cast<int>(expected_dense_->length());
    null_count_ = static_cast<int>(expected_dense_->null_count());
    valid_bits_ = expected_dense_->null_bitmap_data();

    auto builder = CreateDictBuilder();
    ASSERT_OK(builder->AppendArray(*expected_dense_));
    ASSERT_OK(builder->Finish(&expected_dict_));

    // Initialize input_data_ for the encoder from the expected_array_ values
    const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(*expected_dense_);
    input_data_.resize(binary_array.length());

    for (int64_t i = 0; i < binary_array.length(); ++i) {
      auto view = binary_array.GetView(i);
      input_data_[i] = {static_cast<uint32_t>(view.length()),
                        reinterpret_cast<const uint8_t*>(view.data())};
    }
  }

  std::unique_ptr<DictBuilder> CreateDictBuilder() {
    return std::make_unique<DictBuilder>(default_memory_pool());
  }

  // Setup encoder/decoder pair for testing with
  virtual void SetupEncoderDecoder() = 0;

  void CheckDense(int actual_num_values, const ::arrow::Array& chunk) {
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    ASSERT_ARRAYS_EQUAL(chunk, *expected_dense_);
  }

  template <typename Builder>
  void CheckDict(int actual_num_values, Builder& builder) {
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    std::shared_ptr<::arrow::Array> actual;
    ASSERT_OK(builder.Finish(&actual));
    ASSERT_ARRAYS_EQUAL(*actual, *expected_dict_);
  }

  void CheckDecodeArrowUsingDenseBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);

      typename EncodingTraits<ByteArrayType>::Accumulator acc;
      acc.builder.reset(new ::arrow::BinaryBuilder);
      auto actual_num_values =
          decoder_->DecodeArrow(num_values_, null_count_, valid_bits_, 0, &acc);

      std::shared_ptr<::arrow::Array> chunk;
      ASSERT_OK(acc.builder->Finish(&chunk));
      CheckDense(actual_num_values, *chunk);
    }
  }

  void CheckDecodeArrowUsingDictBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);
      auto builder = CreateDictBuilder();
      auto actual_num_values =
          decoder_->DecodeArrow(num_values_, null_count_, valid_bits_, 0, builder.get());
      CheckDict(actual_num_values, *builder);
    }
  }

  void CheckDecodeArrowNonNullUsingDenseBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);
      if (null_count_ > 0) {
        continue;
      }
      typename EncodingTraits<ByteArrayType>::Accumulator acc;
      acc.builder.reset(new ::arrow::BinaryBuilder);
      auto actual_num_values = decoder_->DecodeArrowNonNull(num_values_, &acc);
      std::shared_ptr<::arrow::Array> chunk;
      ASSERT_OK(acc.builder->Finish(&chunk));
      CheckDense(actual_num_values, *chunk);
    }
  }

  void CheckDecodeArrowNonNullUsingDictBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);
      if (null_count_ > 0) {
        continue;
      }
      auto builder = CreateDictBuilder();
      auto actual_num_values = decoder_->DecodeArrowNonNull(num_values_, builder.get());
      CheckDict(actual_num_values, *builder);
    }
  }

 protected:
  std::vector<double> null_probabilities_;
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
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    acc.builder.reset(new ::arrow::StringBuilder);
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

  void Plain(int seed) {
    auto values = GetValues(seed);
    auto encoder = MakeTypedEncoder<ParquetType>(
        Encoding::PLAIN, /*use_dictionary=*/false, column_descr());
    auto decoder = MakeTypedDecoder<ParquetType>(Encoding::PLAIN, column_descr());

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
    ::arrow::AssertArraysEqual(*values, *result, /*verbose=*/true);
  }

  void ByteStreamSplit(int seed) {
    if (!std::is_same<ParquetType, FloatType>::value &&
        !std::is_same<ParquetType, DoubleType>::value) {
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
    auto owned_decoder = MakeDictDecoder<ParquetType>();

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
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    this->Plain(seed);
  }
}

TYPED_TEST(EncodingAdHocTyped, ByteStreamSplitArrowDirectPut) {
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    this->ByteStreamSplit(seed);
  }
}

TYPED_TEST(EncodingAdHocTyped, DeltaBitPackArrowDirectPut) {
  // TODO: test with nulls once DeltaBitPackDecoder::DecodeArrow supports them
  this->null_probability_ = 0;
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    this->DeltaBitPack(seed);
  }
}

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

TYPED_TEST(EncodingAdHocTyped, DictArrowDirectPut) { this->Dict(0); }

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

TYPED_TEST(EncodingAdHocTyped, DictArrowDirectPutIndices) { this->DictPutIndices(); }

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
    InitTestCase(np);
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
  InitTestCase(/*null_probability=*/0.0);
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

  void CheckDecode(const uint8_t* encoded_data, const int64_t encoded_data_size,
                   const c_type* expected_decoded_data, const int num_elements) {
    std::unique_ptr<TypedDecoder<Type>> decoder =
        MakeTypedDecoder<Type>(Encoding::BYTE_STREAM_SPLIT);
    decoder->SetData(num_elements, encoded_data, static_cast<int>(encoded_data_size));
    std::vector<c_type> decoded_data(num_elements);
    int num_decoded_elements = decoder->Decode(decoded_data.data(), num_elements);
    ASSERT_EQ(num_elements, num_decoded_elements);
    for (size_t i = 0U; i < decoded_data.size(); ++i) {
      ASSERT_EQ(expected_decoded_data[i], decoded_data[i]);
    }
    ASSERT_EQ(0, decoder->values_left());
  }

  void CheckEncode(const c_type* data, const int num_elements,
                   const uint8_t* expected_encoded_data,
                   const int64_t encoded_data_size) {
    std::unique_ptr<TypedEncoder<Type>> encoder =
        MakeTypedEncoder<Type>(Encoding::BYTE_STREAM_SPLIT);
    encoder->Put(data, num_elements);
    auto encoded_data = encoder->FlushValues();
    ASSERT_EQ(encoded_data_size, encoded_data->size());
    const uint8_t* encoded_data_raw = encoded_data->data();
    for (int64_t i = 0; i < encoded_data->size(); ++i) {
      ASSERT_EQ(expected_encoded_data[i], encoded_data_raw[i]);
    }
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

static_assert(sizeof(float) == sizeof(uint32_t),
              "BYTE_STREAM_SPLIT encoding tests assume float / uint32_t type sizes");
static_assert(sizeof(double) == sizeof(uint64_t),
              "BYTE_STREAM_SPLIT encoding tests assume double / uint64_t type sizes");

template <>
void TestByteStreamSplitEncoding<FloatType>::CheckDecode() {
  const uint8_t data[] = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                          0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC};
  const auto expected_output =
      ToLittleEndian<uint32_t>({0xAA774411U, 0xBB885522U, 0xCC996633U});
  CheckDecode(data, static_cast<int64_t>(sizeof(data)),
              reinterpret_cast<const float*>(expected_output.data()),
              static_cast<int>(sizeof(data) / sizeof(float)));
}

template <>
void TestByteStreamSplitEncoding<DoubleType>::CheckDecode() {
  const uint8_t data[] = {0xDE, 0xC0, 0x37, 0x13, 0x11, 0x22, 0x33, 0x44,
                          0xAA, 0xBB, 0xCC, 0xDD, 0x55, 0x66, 0x77, 0x88};
  const auto expected_output =
      ToLittleEndian<uint64_t>({0x7755CCAA331137DEULL, 0x8866DDBB442213C0ULL});
  CheckDecode(data, static_cast<int64_t>(sizeof(data)),
              reinterpret_cast<const double*>(expected_output.data()),
              static_cast<int>(sizeof(data) / sizeof(double)));
}

template <>
void TestByteStreamSplitEncoding<DoubleType>::CheckEncode() {
  const auto data = ToLittleEndian<uint64_t>(
      {0x4142434445464748ULL, 0x0102030405060708ULL, 0xb1b2b3b4b5b6b7b8ULL});
  const uint8_t expected_output[24] = {
      0x48, 0x08, 0xb8, 0x47, 0x07, 0xb7, 0x46, 0x06, 0xb6, 0x45, 0x05, 0xb5,
      0x44, 0x04, 0xb4, 0x43, 0x03, 0xb3, 0x42, 0x02, 0xb2, 0x41, 0x01, 0xb1,
  };
  CheckEncode(reinterpret_cast<const double*>(data.data()), static_cast<int>(data.size()),
              expected_output, sizeof(expected_output));
}

template <>
void TestByteStreamSplitEncoding<FloatType>::CheckEncode() {
  const auto data = ToLittleEndian<uint32_t>({0xaabbccdd, 0x11223344});
  const uint8_t expected_output[8] = {0xdd, 0x44, 0xcc, 0x33, 0xbb, 0x22, 0xaa, 0x11};
  CheckEncode(reinterpret_cast<const float*>(data.data()), static_cast<int>(data.size()),
              expected_output, sizeof(expected_output));
}

typedef ::testing::Types<FloatType, DoubleType> ByteStreamSplitTypes;
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
  ASSERT_THROW(MakeTypedEncoder<Int32Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<Int64Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<Int96Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<BooleanType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<ByteArrayType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedEncoder<FLBAType>(Encoding::BYTE_STREAM_SPLIT), ParquetException);

  // Then check decoders.
  ASSERT_THROW(MakeTypedDecoder<Int32Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<Int64Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<Int96Type>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<BooleanType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<ByteArrayType>(Encoding::BYTE_STREAM_SPLIT),
               ParquetException);
  ASSERT_THROW(MakeTypedDecoder<FLBAType>(Encoding::BYTE_STREAM_SPLIT), ParquetException);
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
      decoder->SetData(num_values_ - null_count, encode_buffer_->data(),
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
  constexpr int kValuesPerBlock = 128;
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

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY encode/decode tests.

template <typename Type>
class TestDeltaLengthByteArrayEncoding : public TestEncodingBase<Type> {
 public:
  using c_type = typename Type::c_type;
  static constexpr int TYPE = Type::type_num;

  virtual void CheckRoundtrip() {
    auto encoder = MakeTypedEncoder<Type>(Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                          /*use_dictionary=*/false, descr_.get());
    auto decoder =
        MakeTypedDecoder<Type>(Encoding::DELTA_LENGTH_BYTE_ARRAY, descr_.get());

    encoder->Put(draws_, num_values_);
    encode_buffer_ = encoder->FlushValues();

    decoder->SetData(num_values_, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    int values_decoded = decoder->Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResults<c_type>(decode_buf_, draws_, num_values_));
  }

  void CheckRoundtripSpaced(const uint8_t* valid_bits, int64_t valid_bits_offset) {
    auto encoder = MakeTypedEncoder<Type>(Encoding::DELTA_LENGTH_BYTE_ARRAY,
                                          /*use_dictionary=*/false, descr_.get());
    auto decoder =
        MakeTypedDecoder<Type>(Encoding::DELTA_LENGTH_BYTE_ARRAY, descr_.get());
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

typedef ::testing::Types<ByteArrayType> TestDeltaLengthByteArrayEncodingTypes;
TYPED_TEST_SUITE(TestDeltaLengthByteArrayEncoding, TestDeltaLengthByteArrayEncodingTypes);

TYPED_TEST(TestDeltaLengthByteArrayEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(0, 0));
  ASSERT_NO_FATAL_FAILURE(this->Execute(2000, 200));
  ASSERT_NO_FATAL_FAILURE(this->ExecuteSpaced(
      /*nvalues*/ 1234, /*repeats*/ 1, /*valid_bits_offset*/ 64,
      /*null_probability*/ 0.1));
}

std::shared_ptr<::arrow::Array> CastBinaryTypesHelper(
    std::shared_ptr<::arrow::Array> result, std::shared_ptr<::arrow::DataType> type) {
  if (::arrow::is_large_binary_like(type->id())) {
    ::arrow::compute::CastOptions options;
    if (::arrow::is_string(type->id())) {
      options.to_type = ::arrow::large_utf8();
    } else {
      options.to_type = ::arrow::large_binary();
    }
    EXPECT_OK_AND_ASSIGN(
        auto tmp, CallFunction("cast", {::arrow::Datum{result}}, &options, nullptr));
    result = tmp.make_array();
  }
  return result;
}

TEST(DeltaLengthByteArrayEncodingAdHoc, ArrowBinaryDirectPut) {
  const int64_t size = 50;
  const int32_t min_length = 0;
  const int32_t max_length = 10;
  const int32_t num_unique = 10;
  const double null_probability = 0.25;
  auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);
  auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);

  auto CheckSeed = [&](std::shared_ptr<::arrow::Array> values) {
    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    if (::arrow::is_string(values->type()->id())) {
      acc.builder = std::make_unique<::arrow::StringBuilder>();
    } else {
      acc.builder = std::make_unique<::arrow::BinaryBuilder>();
    }
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(values->length(), result->length());
    ASSERT_OK(result->ValidateFull());

    auto upcast_result = CastBinaryTypesHelper(result, values->type());
    ::arrow::AssertArraysEqual(*values, *result);
  };

  ::arrow::random::RandomArrayGenerator rag(42);
  auto values = rag.String(0, min_length, max_length, null_probability);
  CheckSeed(values);
  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    rag = ::arrow::random::RandomArrayGenerator(seed);

    values = rag.String(size, min_length, max_length, null_probability);
    CheckSeed(values);

    values =
        rag.BinaryWithRepeats(size, num_unique, min_length, max_length, null_probability);
    CheckSeed(values);
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
    ASSERT_TRUE(encoded_values_buf->Equals(*values->data()->buffers[2]));
  };

  auto CheckDecode = [](std::shared_ptr<Buffer> buf,
                        std::shared_ptr<::arrow::Array> values) {
    int num_values = static_cast<int>(values->length());
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::DELTA_LENGTH_BYTE_ARRAY);
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    typename EncodingTraits<ByteArrayType>::Accumulator acc;
    if (::arrow::is_string(values->type()->id())) {
      acc.builder = std::make_unique<::arrow::StringBuilder>();
    } else {
      acc.builder = std::make_unique<::arrow::BinaryBuilder>();
    }

    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(num_values, result->length());
    ASSERT_OK(result->ValidateFull());

    auto upcast_result = CastBinaryTypesHelper(result, values->type());
    ::arrow::AssertArraysEqual(*values, *upcast_result);
  };

  auto values = R"(["Hello", "World", "Foobar", "ADBCEF"])";
  auto lengths = ::arrow::ArrayFromJSON(::arrow::int32(), R"([5, 5, 6, 6])");

  CheckEncode(::arrow::ArrayFromJSON(::arrow::utf8(), values), lengths);
  CheckEncode(::arrow::ArrayFromJSON(::arrow::large_utf8(), values), lengths);
  CheckEncode(::arrow::ArrayFromJSON(::arrow::binary(), values), lengths);
  CheckEncode(::arrow::ArrayFromJSON(::arrow::large_binary(), values), lengths);

  const uint8_t data[] = {
      0x80, 0x01, 0x04, 0x04, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00,
      0x00, 0x00, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x57, 0x6f, 0x72, 0x6c, 0x64,
      0x46, 0x6f, 0x6f, 0x62, 0x61, 0x72, 0x41, 0x44, 0x42, 0x43, 0x45, 0x46,
  };
  auto encoded = Buffer::Wrap(data, sizeof(data));

  CheckDecode(encoded, ::arrow::ArrayFromJSON(::arrow::utf8(), values));
  CheckDecode(encoded, ::arrow::ArrayFromJSON(::arrow::large_utf8(), values));
  CheckDecode(encoded, ::arrow::ArrayFromJSON(::arrow::binary(), values));
  CheckDecode(encoded, ::arrow::ArrayFromJSON(::arrow::large_binary(), values));
}

}  // namespace test
}  // namespace parquet
