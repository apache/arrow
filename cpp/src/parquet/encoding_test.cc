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
#include <limits>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "parquet/encoding.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

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
  int nbytes = static_cast<int>(BitUtil::BytesForBits(nvalues));

  std::vector<bool> draws;
  arrow::random_is_valid(nvalues, 0.5 /* null prob */, &draws, 0 /* seed */);

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
    ASSERT_EQ(draws[i], arrow::BitUtil::GetBit(decode_data, i)) << i;
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
                                          ConvertedType::DECIMAL, flba_length, 10, 2);
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
    std::vector<uint8_t> valid_bits(arrow::BitUtil::BytesForBits(num_values_) + 1, 255);

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

class TestArrowBuilderDecoding : public ::testing::Test {
 public:
  using DenseBuilder = arrow::internal::ChunkedBinaryBuilder;
  using DictBuilder = arrow::BinaryDictionary32Builder;

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
    arrow::random::RandomArrayGenerator rag(0);
    expected_dense_ = rag.BinaryWithRepeats(repeat * num_unique, num_unique, min_length,
                                            max_length, null_probability);

    num_values_ = static_cast<int>(expected_dense_->length());
    null_count_ = static_cast<int>(expected_dense_->null_count());
    valid_bits_ = expected_dense_->null_bitmap()->data();

    auto builder = CreateDictBuilder();
    ASSERT_OK(builder->AppendArray(*expected_dense_));
    ASSERT_OK(builder->Finish(&expected_dict_));

    // Initialize input_data_ for the encoder from the expected_array_ values
    const auto& binary_array = static_cast<const arrow::BinaryArray&>(*expected_dense_);
    input_data_.resize(binary_array.length());

    for (int64_t i = 0; i < binary_array.length(); ++i) {
      auto view = binary_array.GetView(i);
      input_data_[i] = {static_cast<uint32_t>(view.length()),
                        reinterpret_cast<const uint8_t*>(view.data())};
    }
  }

  std::unique_ptr<DictBuilder> CreateDictBuilder() {
    return std::unique_ptr<DictBuilder>(new DictBuilder(default_memory_pool()));
  }

  // Setup encoder/decoder pair for testing with
  virtual void SetupEncoderDecoder() = 0;

  void CheckDense(int actual_num_values, const arrow::Array& chunk) {
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    ASSERT_ARRAYS_EQUAL(chunk, *expected_dense_);
  }

  template <typename Builder>
  void CheckDict(int actual_num_values, Builder& builder) {
    ASSERT_EQ(actual_num_values, num_values_ - null_count_);
    std::shared_ptr<arrow::Array> actual;
    ASSERT_OK(builder.Finish(&actual));
    ASSERT_ARRAYS_EQUAL(*actual, *expected_dict_);
  }

  void CheckDecodeArrowUsingDenseBuilder() {
    for (auto np : null_probabilities_) {
      InitTestCase(np);

      ArrowBinaryAccumulator acc;
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
      SKIP_TEST_IF(null_count_ > 0)
      ArrowBinaryAccumulator acc;
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
      SKIP_TEST_IF(null_count_ > 0)
      auto builder = CreateDictBuilder();
      auto actual_num_values = decoder_->DecodeArrowNonNull(num_values_, builder.get());
      CheckDict(actual_num_values, *builder);
    }
  }

 protected:
  std::vector<double> null_probabilities_;
  std::shared_ptr<arrow::Array> expected_dict_;
  std::shared_ptr<arrow::Array> expected_dense_;
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
    ASSERT_NO_THROW(encoder_->PutSpaced(input_data_.data(), num_values_, valid_bits_, 0));
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
    arrow::random::RandomArrayGenerator rag(seed);
    auto values = rag.String(size, min_length, max_length, null_probability);

    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);

    ASSERT_NO_THROW(encoder->Put(*values));
    auto buf = encoder->FlushValues();

    int num_values = static_cast<int>(values->length() - values->null_count());
    decoder->SetData(num_values, buf->data(), static_cast<int>(buf->size()));

    ArrowBinaryAccumulator acc;
    acc.builder.reset(new arrow::StringBuilder);
    ASSERT_EQ(num_values,
              decoder->DecodeArrow(static_cast<int>(values->length()),
                                   static_cast<int>(values->null_count()),
                                   values->null_bitmap_data(), values->offset(), &acc));

    std::shared_ptr<::arrow::Array> result;
    ASSERT_OK(acc.builder->Finish(&result));
    ASSERT_EQ(50, result->length());
    arrow::AssertArraysEqual(*values, *result);

    // Type checked
    auto i32_values = rag.Int32(size, 0, 10, null_probability);
    ASSERT_THROW(encoder->Put(*i32_values), ParquetException);
  };

  for (auto seed : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    CheckSeed(seed);
  }
}

void GetBinaryDictDecoder(DictEncoder<ByteArrayType>* encoder, int64_t num_values,
                          std::shared_ptr<Buffer>* out_values,
                          std::shared_ptr<Buffer>* out_dict,
                          std::unique_ptr<ByteArrayDecoder>* out_decoder) {
  auto decoder = MakeDictDecoder<ByteArrayType>();
  auto buf = encoder->FlushValues();
  auto dict_buf = AllocateBuffer(default_memory_pool(), encoder->dict_encoded_size());
  encoder->WriteDict(dict_buf->mutable_data());

  auto dict_decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
  dict_decoder->SetData(encoder->num_entries(), dict_buf->data(),
                        static_cast<int>(dict_buf->size()));

  decoder->SetData(static_cast<int>(num_values), buf->data(),
                   static_cast<int>(buf->size()));
  decoder->SetDict(dict_decoder.get());

  *out_values = buf;
  *out_dict = dict_buf;
  *out_decoder = std::unique_ptr<ByteArrayDecoder>(
      dynamic_cast<ByteArrayDecoder*>(decoder.release()));
}

TEST(DictEncodingAdHoc, ArrowBinaryDirectPut) {
  // Implemented as part of ARROW-3246
  const int64_t size = 50;
  const int64_t min_length = 0;
  const int64_t max_length = 10;
  const double null_probability = 0.1;
  arrow::random::RandomArrayGenerator rag(0);
  auto values = rag.String(size, min_length, max_length, null_probability);

  auto owned_encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN,
                                                       /*use_dictionary=*/true);

  auto encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(owned_encoder.get());

  ASSERT_NO_THROW(encoder->Put(*values));

  std::unique_ptr<ByteArrayDecoder> decoder;
  std::shared_ptr<Buffer> buf, dict_buf;
  int num_values = static_cast<int>(values->length() - values->null_count());
  GetBinaryDictDecoder(encoder, num_values, &buf, &dict_buf, &decoder);

  ArrowBinaryAccumulator acc;
  acc.builder.reset(new arrow::StringBuilder);
  ASSERT_EQ(num_values,
            decoder->DecodeArrow(static_cast<int>(values->length()),
                                 static_cast<int>(values->null_count()),
                                 values->null_bitmap_data(), values->offset(), &acc));

  std::shared_ptr<::arrow::Array> result;
  ASSERT_OK(acc.builder->Finish(&result));
  arrow::AssertArraysEqual(*values, *result);
}

TEST(DictEncodingAdHoc, PutDictionaryPutIndices) {
  // Part of ARROW-3246
  auto dict_values = arrow::ArrayFromJSON(arrow::binary(), "[\"foo\", \"bar\", \"baz\"]");
  auto indices = arrow::ArrayFromJSON(arrow::int32(), "[0, 1, 2]");
  auto indices_nulls = arrow::ArrayFromJSON(arrow::int32(), "[null, 0, 1, null, 2]");

  auto expected = arrow::ArrayFromJSON(arrow::binary(),
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
  GetBinaryDictDecoder(encoder, num_values, &buf, &dict_buf, &decoder);

  ArrowBinaryAccumulator acc;
  acc.builder.reset(new arrow::BinaryBuilder);
  ASSERT_EQ(num_values,
            decoder->DecodeArrow(static_cast<int>(expected->length()),
                                 static_cast<int>(expected->null_count()),
                                 expected->null_bitmap_data(), expected->offset(), &acc));

  std::shared_ptr<::arrow::Array> result;
  ASSERT_OK(acc.builder->Finish(&result));
  arrow::AssertArraysEqual(*expected, *result);
}

class DictEncoding : public TestArrowBuilderDecoding {
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
    auto actual_num_values = dict_decoder_->DecodeIndicesSpaced(
        num_values_, null_count_, valid_bits_, 0, builder.get());
    CheckDict(actual_num_values, *builder);
  }
}

TEST_F(DictEncoding, CheckDecodeIndicesNoNulls) {
  InitTestCase(/*null_probability=*/0.0);
  auto builder = CreateDictBuilder();
  dict_decoder_->InsertDictionary(builder.get());
  auto actual_num_values = dict_decoder_->DecodeIndices(num_values_, builder.get());
  CheckDict(actual_num_values, *builder);
}

}  // namespace test
}  // namespace parquet
