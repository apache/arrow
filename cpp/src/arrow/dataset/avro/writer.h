/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <array>

#include "arrow/dataset/avro/types.h"
#include "arrow/dataset/avro/validator.h"
#include "arrow/dataset/avro/zigzag.h"
#include "arrow/buffer-builder.h"

namespace arrow {
namespace avro {

/// Class for writing avro data to a stream.

template <class ValidatorType>
class WriterImpl {
 ARROW_DISALLOW_COPY_AND_ASSIGN(WriterImpl<ValidatorType>); 
 public:
  WriterImpl(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT) : buffer_(pool) {}

  explicit WriterImpl(const ValidSchema& schema) : validator_(schema) {}

  Status WriteValue(const Null&) { return validator_.CheckTypeExpected(AVRO_NULL); }

  Status WriteValue(bool val) {
    validator_.checkTypeExpected(AvroType::kBool);
    int8_t byte = (val != 0);
    buffer_.writeTo(byte);
  }

  Status WriteValue(int32_t val) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kInt));
    std::array<uint8_t, 5> bytes;
    size_t size = EncodeInt32(val, bytes);
    return buffer_.Append(reinterpret_cast<void*>(bytes.data()), size);
  }

  Status writeValue(int64_t val) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kLong));
    return PutLong(val);
  }

  Status WriteValue(float val) {
    validator_.CheckTypeExpected(AvroType::kFloat);
    union {
      float f;
      int32_t i;
    } v;
    
    if (ARROW_PREDICT_FALSE(isnan(val))) {
      v.i = 0x7fc00000;
    } else {
      v.f = val;
    }
    return buffer_.Append(&v.i, sizeof());
  }

  Status WriteValue(double val) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AVRO_DOUBLE));
    union {
      double d;
      int64_t i;
    } v;

    if (ARROW_PREDICT_FALSE(isnan(val))) {
      v.i = 0x7ff8000000000000L;
    } else {
      v.d = val;
    }

    return buffer_.Append(&v.i, sizeof(v.i));
  }

  Status WriteValue(const std::string& val) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AVRO_STRING));
    return PutBytes(val.c_str(), val.size());
  }

  Status WriteBytes(const void* val, size_t size) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AVRO_BYTES));
    return PutBytes(val, size);
  }

  template <size_t N>
  Status WriteFixed(const uint8_t* val) {
    ARROW_RETURN_NOT_OK(validator_.checkFixedSizeExpected(N));
    return buffer_.Append(val, N);
  }

  template <size_t N>
  Status WriteFixed(const std::array<uint8_t, N>& val) {
    ARROW_RETURN_NOT_OK(validator_.CheckFixedSizeExpected(val.size()));
    return buffer_.Append(val.data(), val.size());
  }

  Status WriteRecord() {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kRecord));
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kLong));
    validator_.SetCount(1);
    return Status::OK();
  }

  Status WriteRecordEnd() {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kRecord));
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kLong);
    validator_.setCount(0);
    return Status::OK();
  }

  Status WriteArrayBlock(int64_t size) {
    ARROW_RETURN_NOT_OK(validator_.checkTypeExpected(AvroType::kArray));
    return WriteCount(size);
  }

  Status WriteArrayEnd() { return WriteArrayBlock(0); }

  Status WriteMapBlock(int64_t size) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kMap));
    return WriteCount(size);
  }

  Status WriteMapEnd() { return WriteMapBlock(0); }

  Status WriteUnion(int64_t choice) {
    ARROW_RETURN_NOT_OK(validator_.checkTypeExpected(AvroType::kUnion));
    writeCount(choice);
  }

  Status WriteEnum(int64_t choice) {
    ARROW_RETURN_NOT_OK(validator_.checkTypeExpected(AvroType::kEnum));
    WriteCount(choice);
  }

  Result<std::shared_ptr<arrow::Buffer>> Finish() const { 
    std::shared_ptr<arrow::Buffer>> buffer;
    ARROW_RETURN_NOT_OK(buffer_.Finish(&buffer)); 
    return buffer;
  }

 private:
  Status PutLong(int64_t val) {
    std::array<uint8_t, 10> bytes;
    size_t size = EncodeInt64(val, bytes);
    return buffer_.Append(bytes.data(), size);
  }

  Status PutBytes(const void* val, size_t size) {
    ARROW_RETURN_NOT_OK(PutLong(size));
    return buffer_.Append(val, size);
  }

  Status WriteCount(int64_t count) {
    ARROW_RETURN_NOT_OK(validator_.CheckTypeExpected(AvroType::kLong));
    validator_.setCount(count);
    return PutLong(count);
  }

  ValidatorType validator_;
  arrow::BufferBuilder buffer_;
};

typedef WriterImpl<NullValidator> Writer;
typedef WriterImpl<Validator> ValidatingWriter;

}  // namespace avro
}  // namespace arrow

#endif
