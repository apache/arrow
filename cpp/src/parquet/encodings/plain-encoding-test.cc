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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "parquet/schema/descriptor.h"
#include "parquet/encodings/plain-encoding.h"
#include "parquet/types.h"
#include "parquet/schema/types.h"
#include "parquet/util/bit-util.h"
#include "parquet/util/buffer.h"
#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace test {

TEST(VectorBooleanTest, TestEncodeDecode) {
  // PARQUET-454
  size_t nvalues = 10000;
  size_t nbytes = BitUtil::Ceil(nvalues, 8);

  // seed the prng so failure is deterministic
  vector<bool> draws = flip_coins_seed(nvalues, 0.5, 0);

  PlainEncoder<Type::BOOLEAN> encoder(nullptr);
  PlainDecoder<Type::BOOLEAN> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws, nvalues, &dst);

  std::shared_ptr<Buffer> encode_buffer = dst.GetBuffer();
  ASSERT_EQ(nbytes, encode_buffer->size());

  vector<uint8_t> decode_buffer(nbytes);
  const uint8_t* decode_data = &decode_buffer[0];

  decoder.SetData(nvalues, encode_buffer->data(), encode_buffer->size());
  size_t values_decoded = decoder.Decode(&decode_buffer[0], nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], BitUtil::GetArrayBit(decode_data, i)) << i;
  }
}

template<typename T, int TYPE>
class EncodeDecode{
 public:
  void init_data(int nvalues) {
    num_values_ = nvalues;
    input_bytes_.resize(num_values_ * sizeof(T));
    output_bytes_.resize(num_values_ * sizeof(T));
    draws_ = reinterpret_cast<T*>(input_bytes_.data());
    decode_buf_ = reinterpret_cast<T*>(output_bytes_.data());
  }

  void generate_data() {
    // seed the prng so failure is deterministic
    random_numbers(num_values_, 0, std::numeric_limits<T>::min(),
       std::numeric_limits<T>::max(), draws_);
  }

  void encode_decode(ColumnDescriptor *d) {
    PlainEncoder<TYPE> encoder(d);
    PlainDecoder<TYPE> decoder(d);

    InMemoryOutputStream dst;
    encoder.Encode(draws_, num_values_, &dst);

    encode_buffer_ = dst.GetBuffer();

    decoder.SetData(num_values_, encode_buffer_->data(),
        encode_buffer_->size());
    size_t values_decoded = decoder.Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);
  }

  void verify_results() {
    for (size_t i = 0; i < num_values_; ++i) {
      ASSERT_EQ(draws_[i], decode_buf_[i]) << i;
    }
  }

  void execute(int nvalues, ColumnDescriptor *d) {
    init_data(nvalues);
    generate_data();
    encode_decode(d);
    verify_results();
  }

 private:
  int num_values_;
  T* draws_;
  T* decode_buf_;
  vector<uint8_t> input_bytes_;
  vector<uint8_t> output_bytes_;
  vector<uint8_t> data_buffer_;

  std::shared_ptr<Buffer> encode_buffer_;
};

template<>
void EncodeDecode<bool, Type::BOOLEAN>::generate_data() {
  // seed the prng so failure is deterministic
  random_bools(num_values_, 0.5, 0, draws_);
}

template<>
void EncodeDecode<Int96, Type::INT96>::generate_data() {
  // seed the prng so failure is deterministic
    random_Int96_numbers(num_values_, 0, std::numeric_limits<int32_t>::min(),
       std::numeric_limits<int32_t>::max(), draws_);
}

template<>
void EncodeDecode<Int96, Type::INT96>::verify_results() {
  for (size_t i = 0; i < num_values_; ++i) {
    ASSERT_EQ(draws_[i].value[0], decode_buf_[i].value[0]) << i;
    ASSERT_EQ(draws_[i].value[1], decode_buf_[i].value[1]) << i;
    ASSERT_EQ(draws_[i].value[2], decode_buf_[i].value[2]) << i;
  }
}

template<>
void EncodeDecode<ByteArray, Type::BYTE_ARRAY>::generate_data() {
  // seed the prng so failure is deterministic
  int max_byte_array_len = 12;
  int num_bytes = max_byte_array_len + sizeof(uint32_t);
  size_t nbytes = num_values_ * num_bytes;
  data_buffer_.resize(nbytes);
  random_byte_array(num_values_, 0, data_buffer_.data(), draws_,
      max_byte_array_len);
}

template<>
void EncodeDecode<ByteArray, Type::BYTE_ARRAY>::verify_results() {
  for (size_t i = 0; i < num_values_; ++i) {
    ASSERT_EQ(draws_[i].len, decode_buf_[i].len) << i;
    ASSERT_EQ(0, memcmp(draws_[i].ptr, decode_buf_[i].ptr, draws_[i].len)) << i;
  }
}

static int flba_length = 8;
template<>
void EncodeDecode<FLBA, Type::FIXED_LEN_BYTE_ARRAY>::generate_data() {
  // seed the prng so failure is deterministic
  size_t nbytes = num_values_ * flba_length;
  data_buffer_.resize(nbytes);
  ASSERT_EQ(nbytes, data_buffer_.size());
  random_fixed_byte_array(num_values_, 0, data_buffer_.data(), flba_length, draws_);
}

template<>
void EncodeDecode<FLBA, Type::FIXED_LEN_BYTE_ARRAY>::verify_results() {
  for (size_t i = 0; i < num_values_; ++i) {
    ASSERT_EQ(0, memcmp(draws_[i].ptr, decode_buf_[i].ptr, flba_length)) << i;
  }
}

int num_values = 10000;

TEST(BoolEncodeDecode, TestEncodeDecode) {
  EncodeDecode<bool, Type::BOOLEAN> obj;
  obj.execute(num_values, nullptr);
}

TEST(Int32EncodeDecode, TestEncodeDecode) {
  EncodeDecode<int32_t, Type::INT32> obj;
  obj.execute(num_values, nullptr);
}

TEST(Int64EncodeDecode, TestEncodeDecode) {
  EncodeDecode<int64_t, Type::INT64> obj;
  obj.execute(num_values, nullptr);
}

TEST(FloatEncodeDecode, TestEncodeDecode) {
  EncodeDecode<float, Type::FLOAT> obj;
  obj.execute(num_values, nullptr);
}

TEST(DoubleEncodeDecode, TestEncodeDecode) {
  EncodeDecode<double, Type::DOUBLE> obj;
  obj.execute(num_values, nullptr);
}

TEST(Int96EncodeDecode, TestEncodeDecode) {
  EncodeDecode<Int96, Type::INT96> obj;
  obj.execute(num_values, nullptr);
}

TEST(BAEncodeDecode, TestEncodeDecode) {
  EncodeDecode<ByteArray, Type::BYTE_ARRAY> obj;
  obj.execute(num_values, nullptr);
}

TEST(FLBAEncodeDecode, TestEncodeDecode) {
  schema::NodePtr node;
  node = schema::PrimitiveNode::MakeFLBA("name", Repetition::OPTIONAL,
      flba_length, LogicalType::UTF8);
  ColumnDescriptor d(node, 0, 0);
  EncodeDecode<FixedLenByteArray, Type::FIXED_LEN_BYTE_ARRAY> obj;
  obj.execute(num_values, &d);
}

} // namespace test
} // namespace parquet_cpp
