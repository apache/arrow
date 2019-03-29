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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/test-common.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

// TODO(hatemhelal): investigate whether this can be replaced with GTEST_SKIP in a future
// gtest release that contains https://github.com/google/googletest/pull/1544
#define SKIP_TEST_IF(condition) \
  if (condition) {              \
    return;                     \
  }

namespace parquet {

namespace test {

TEST(VectorBooleanTest, TestEncodeDecode) {
  // PARQUET-454
  int nvalues = 10000;
  int nbytes = static_cast<int>(::arrow::BitUtil::BytesForBits(nvalues));

  // seed the prng so failure is deterministic
  std::vector<bool> draws = flip_coins_seed(nvalues, 0.5, 0);

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
    ASSERT_EQ(draws[i], ::arrow::BitUtil::GetBit(decode_data, i)) << i;
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

template <>
void VerifyResults<FLBA>(FLBA* result, FLBA* expected, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    ASSERT_EQ(0, memcmp(expected[i].ptr, result[i].ptr, flba_length)) << i;
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
                                          LogicalType::DECIMAL, flba_length, 10, 2);
  return std::make_shared<ColumnDescriptor>(node, 0, 0);
}

// ----------------------------------------------------------------------
// Plain encoding tests

template <typename Type>
class TestEncodingBase : public ::testing::Test {
 public:
  typedef typename Type::c_type T;
  static constexpr int TYPE = Type::type_num;

  void SetUp() {
    descr_ = ExampleDescr<Type>();
    type_length_ = descr_->type_length();
    allocator_ = default_memory_pool();
  }

  void TearDown() {}

  void InitData(int nvalues, int repeats) {
    num_values_ = nvalues * repeats;
    input_bytes_.resize(num_values_ * sizeof(T));
    output_bytes_.resize(num_values_ * sizeof(T));
    draws_ = reinterpret_cast<T*>(input_bytes_.data());
    decode_buf_ = reinterpret_cast<T*>(output_bytes_.data());
    GenerateData<T>(nvalues, draws_, &data_buffer_);

    // add some repeated values
    for (int j = 1; j < repeats; ++j) {
      for (int i = 0; i < nvalues; ++i) {
        draws_[nvalues * j + i] = draws_[i];
      }
    }
  }

  virtual void CheckRoundtrip() = 0;

  void Execute(int nvalues, int repeats) {
    InitData(nvalues, repeats);
    CheckRoundtrip();
  }

 protected:
  MemoryPool* allocator_;

  int num_values_;
  int type_length_;
  T* draws_;
  T* decode_buf_;
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
  using TestEncodingBase<Type>::decode_buf_

template <typename Type>
class TestPlainEncoding : public TestEncodingBase<Type> {
 public:
  typedef typename Type::c_type T;
  static constexpr int TYPE = Type::type_num;

  virtual void CheckRoundtrip() {
    auto encoder = MakeTypedEncoder<Type>(Encoding::PLAIN, false, descr_.get());
    auto decoder = MakeTypedDecoder<Type>(Encoding::PLAIN, descr_.get());
    encoder->Put(draws_, num_values_);
    encode_buffer_ = encoder->FlushValues();

    decoder->SetData(num_values_, encode_buffer_->data(),
                     static_cast<int>(encode_buffer_->size()));
    int values_decoded = decoder->Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResults<T>(decode_buf_, draws_, num_values_));
  }

 protected:
  USING_BASE_MEMBERS();
};

TYPED_TEST_CASE(TestPlainEncoding, ParquetTypes);

TYPED_TEST(TestPlainEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(10000, 1));
}

// ----------------------------------------------------------------------
// Dictionary encoding tests

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                         ByteArrayType, FLBAType>
    DictEncodedTypes;

template <typename Type>
class TestDictionaryEncoding : public TestEncodingBase<Type> {
 public:
  typedef typename Type::c_type T;
  static constexpr int TYPE = Type::type_num;

  void CheckRoundtrip() {
    std::vector<uint8_t> valid_bits(::arrow::BitUtil::BytesForBits(num_values_) + 1, 255);

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
    ASSERT_NO_THROW(spaced_encoder->PutSpaced(draws_, num_values_, valid_bits.data(), 0));
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
    ASSERT_NO_FATAL_FAILURE(VerifyResults<T>(decode_buf_, draws_, num_values_));

    // Also test spaced decoding
    decoder->SetData(num_values_, indices->data(), static_cast<int>(indices->size()));
    values_decoded =
        decoder->DecodeSpaced(decode_buf_, num_values_, 0, valid_bits.data(), 0);
    ASSERT_EQ(num_values_, values_decoded);
    ASSERT_NO_FATAL_FAILURE(VerifyResults<T>(decode_buf_, draws_, num_values_));
  }

 protected:
  USING_BASE_MEMBERS();
  std::shared_ptr<ResizableBuffer> dict_buffer_;
};

TYPED_TEST_CASE(TestDictionaryEncoding, DictEncodedTypes);

TYPED_TEST(TestDictionaryEncoding, BasicRoundTrip) {
  ASSERT_NO_FATAL_FAILURE(this->Execute(2500, 2));
}

TEST(TestDictionaryEncoding, CannotDictDecodeBoolean) {
  ASSERT_THROW(MakeDictDecoder<BooleanType>(nullptr), ParquetException);
}

// ----------------------------------------------------------------------
// Shared arrow builder decode tests
template <typename T>
struct BuilderTraits {};

template <>
struct BuilderTraits<::arrow::BinaryType> {
  using DenseArrayBuilder = ::arrow::internal::ChunkedBinaryBuilder;
  using DictArrayBuilder = ::arrow::BinaryDictionaryBuilder;
};

template <>
struct BuilderTraits<::arrow::StringType> {
  using DenseArrayBuilder = ::arrow::internal::ChunkedStringBuilder;
  using DictArrayBuilder = ::arrow::StringDictionaryBuilder;
};

template <typename DType>
class TestArrowBuilderDecoding : public ::testing::Test {
 public:
  using DenseBuilder = typename BuilderTraits<DType>::DenseArrayBuilder;
  using DictBuilder = typename BuilderTraits<DType>::DictArrayBuilder;

  void SetUp() override { null_probabilities_ = {0.0, 0.5, 1.0}; }
  void TearDown() override {}

  void InitTestCase(double null_probability) {
    GenerateInputData(null_probability);
    SetupEncoderDecoder();
  }

  void GenerateInputData(double null_probability) {
    constexpr int num_unique = 100;
    constexpr int repeat = 10;
    constexpr int64_t min_length = 2;
    constexpr int64_t max_length = 10;
    ::arrow::random::RandomArrayGenerator rag(0);
    expected_dense_ = rag.StringWithRepeats(repeat * num_unique, num_unique, min_length,
                                            max_length, null_probability);

    std::shared_ptr<::arrow::DataType> data_type = std::make_shared<DType>();

    if (data_type->id() == ::arrow::BinaryType::type_id) {
      // TODO(hatemhelal):  this is a kludge.  Probably best to extend the
      // RandomArrayGenerator to also generate BinaryType arrays.
      auto data = expected_dense_->data()->Copy();
      data->type = data_type;
      expected_dense_ = std::make_shared<::arrow::BinaryArray>(data);
    }

    num_values_ = static_cast<int>(expected_dense_->length());
    null_count_ = static_cast<int>(expected_dense_->null_count());
    valid_bits_ = expected_dense_->null_bitmap()->data();

    auto builder = CreateDictBuilder();
    ASSERT_OK(builder->AppendArray(*expected_dense_));
    ASSERT_OK(builder->Finish(&expected_dict_));

    // Initialize input_data_ for the encoder from the expected_array_ values
    const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(*expected_dense_);
    input_data_.reserve(binary_array.length());

    for (int64_t i = 0; i < binary_array.length(); ++i) {
      auto view = binary_array.GetView(i);
      input_data_[i] = {static_cast<uint32_t>(view.length()),
                        reinterpret_cast<const uint8_t*>(view.data())};
    }
  }

  std::unique_ptr<DenseBuilder> CreateDenseBuilder() {
    // Use same default chunk size of 16MB as used in ByteArrayChunkedRecordReader
    constexpr int32_t kChunkSize = 1 << 24;
    return std::unique_ptr<DenseBuilder>(
        new DenseBuilder(kChunkSize, default_memory_pool()));
  }

  std::unique_ptr<DictBuilder> CreateDictBuilder() {
    return std::unique_ptr<DictBuilder>(new DictBuilder(default_memory_pool()));
  }

  // Setup encoder/decoder pair for testing with
  virtual void SetupEncoderDecoder() = 0;

  template <typename Builder>
  void CheckDense(int actual_num_values, Builder& builder) {
    ASSERT_EQ(actual_num_values, num_values_);
    ::arrow::ArrayVector actual_vec;
    ASSERT_OK(builder.Finish(&actual_vec));
    ASSERT_EQ(actual_vec.size(), 1);
    ASSERT_ARRAYS_EQUAL(*actual_vec[0], *expected_dense_);
  }

  template <typename Builder>
  void CheckDict(int actual_num_values, Builder& builder) {
    ASSERT_EQ(actual_num_values, num_values_);
    std::shared_ptr<::arrow::Array> actual;
    ASSERT_OK(builder.Finish(&actual));
    ASSERT_ARRAYS_EQUAL(*actual, *expected_dict_);
  }

  void CheckDecodeArrowUsingDenseBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);
      auto builder = CreateDenseBuilder();
      auto actual_num_values =
          decoder_->DecodeArrow(num_values_, null_count_, valid_bits_, 0, builder.get());
      CheckDense(actual_num_values, *builder);
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
      SKIP_TEST_IF(null_count_ > 0)
      auto builder = CreateDenseBuilder();
      auto actual_num_values = decoder_->DecodeArrowNonNull(num_values_, builder.get());
      CheckDense(actual_num_values, *builder);
    }
  }

  void CheckDecodeArrowNonNullUsingDictBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);
      SKIP_TEST_IF(null_count_ > 0)
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
  std::unique_ptr<ByteArrayDecoder> decoder_;
  std::shared_ptr<Buffer> buffer_;
};

#define TEST_ARROW_BUILDER_BASE_MEMBERS()             \
  using TestArrowBuilderDecoding<DType>::encoder_;    \
  using TestArrowBuilderDecoding<DType>::decoder_;    \
  using TestArrowBuilderDecoding<DType>::input_data_; \
  using TestArrowBuilderDecoding<DType>::valid_bits_; \
  using TestArrowBuilderDecoding<DType>::num_values_; \
  using TestArrowBuilderDecoding<DType>::buffer_

template <typename DType>
class PlainEncoding : public TestArrowBuilderDecoding<DType> {
 public:
  void SetupEncoderDecoder() override {
    encoder_ = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    decoder_ = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    ASSERT_NO_THROW(encoder_->PutSpaced(input_data_.data(), num_values_, valid_bits_, 0));
    buffer_ = encoder_->FlushValues();
    decoder_->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
  }

 protected:
  TEST_ARROW_BUILDER_BASE_MEMBERS();
};

using BuilderArrayTypes = ::testing::Types<::arrow::BinaryType, ::arrow::StringType>;
// using BuilderArrayTypes = ::testing::Types<::arrow::StringType>;
TYPED_TEST_CASE(PlainEncoding, BuilderArrayTypes);

TYPED_TEST(PlainEncoding, CheckDecodeArrowUsingDenseBuilder) {
  this->CheckDecodeArrowUsingDenseBuilder();
}

TYPED_TEST(PlainEncoding, CheckDecodeArrowUsingDictBuilder) {
  this->CheckDecodeArrowUsingDictBuilder();
}

TYPED_TEST(PlainEncoding, CheckDecodeArrowNonNullDenseBuilder) {
  this->CheckDecodeArrowNonNullUsingDenseBuilder();
}

TYPED_TEST(PlainEncoding, CheckDecodeArrowNonNullDictBuilder) {
  this->CheckDecodeArrowNonNullUsingDictBuilder();
}

template <typename DType>
class DictEncoding : public TestArrowBuilderDecoding<DType> {
 public:
  void SetupEncoderDecoder() override {
    auto node = schema::ByteArray("name");
    descr_ = std::unique_ptr<ColumnDescriptor>(new ColumnDescriptor(node, 0, 0));
    encoder_ = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN, /*use_dictionary=*/true,
                                               descr_.get());
    ASSERT_NO_THROW(encoder_->PutSpaced(input_data_.data(), num_values_, valid_bits_, 0));
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
    decoder_ = std::unique_ptr<ByteArrayDecoder>(
        dynamic_cast<ByteArrayDecoder*>(dict_decoder_.release()));
  }

 protected:
  TEST_ARROW_BUILDER_BASE_MEMBERS();
  std::unique_ptr<ColumnDescriptor> descr_;
  std::unique_ptr<ByteArrayDecoder> plain_decoder_;
  std::unique_ptr<DictDecoder<ByteArrayType>> dict_decoder_;
  std::shared_ptr<Buffer> dict_buffer_;
};

TYPED_TEST_CASE(DictEncoding, BuilderArrayTypes);

TYPED_TEST(DictEncoding, CheckDecodeArrowUsingDenseBuilder) {
  this->CheckDecodeArrowUsingDenseBuilder();
}

TYPED_TEST(DictEncoding, CheckDecodeArrowUsingDictBuilder) {
  this->CheckDecodeArrowUsingDictBuilder();
}

TYPED_TEST(DictEncoding, CheckDecodeArrowNonNullDenseBuilder) {
  this->CheckDecodeArrowNonNullUsingDenseBuilder();
}

TYPED_TEST(DictEncoding, CheckDecodeArrowNonNullDictBuilder) {
  this->CheckDecodeArrowNonNullUsingDictBuilder();
}

}  // namespace test

}  // namespace parquet
