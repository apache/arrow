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

#include "parquet/arrow/fuzz_encoding_internal.h"

#include <string.h>
#include <cstdint>
#include <cstring>
#include <limits>
#include <sstream>
#include <string_view>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer_builder.h"
#include "arrow/compare.h"
#include "arrow/io/memory.h"
#include "arrow/type.h"
#include "arrow/util/fuzz_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/visit_type_inline.h"

namespace parquet::fuzzing::internal {

using ::arrow::Array;
using ::arrow::ArrayData;
using ::arrow::BufferBuilder;
using ::arrow::DataType;
using ::arrow::MemoryPool;
using ::arrow::Result;
using ::arrow::Status;
using ::arrow::TypedBufferBuilder;
using ::parquet::arrow::FileReader;

ColumnDescriptor MakeColumnDescriptor(Type::type type, int type_length) {
  // Repetition and max def/rep levels can take dummy values as they are not directly
  // used by encoders and decoders.
  auto node = schema::PrimitiveNode::Make("", Repetition::OPTIONAL, type,
                                          ConvertedType::NONE, type_length);
  return ColumnDescriptor(node, /*max_definition_level=*/1, /*max_repetition_level=*/0);
}

namespace {

constexpr auto kPackedEncodingHeaderSize = 64;

ARROW_PACKED_START(struct, PackedEncodingHeader) {
  ARROW_PACKED_START(struct, Header) {
    uint8_t source_encoding_id;
    uint8_t roundtrip_encoding_id;
    uint8_t type_id;
    int32_t type_length;
    int32_t num_values;
  };
  ARROW_PACKED_END

  static_assert(sizeof(Header) == 3 * 1 + 2 * 4);
  using Reserved = std::array<uint8_t, kPackedEncodingHeaderSize - sizeof(Header)>;

  Header header;
  Reserved reserved;
};
ARROW_PACKED_END

static_assert(sizeof(PackedEncodingHeader) == kPackedEncodingHeaderSize);

}  // namespace

FuzzEncodingHeader::FuzzEncodingHeader(Encoding::type source_encoding,
                                       Encoding::type roundtrip_encoding, Type::type type,
                                       int type_length, int num_values)
    : source_encoding(source_encoding),
      roundtrip_encoding(roundtrip_encoding),
      type(type),
      type_length(type_length),
      num_values(num_values) {}

FuzzEncodingHeader::FuzzEncodingHeader(Encoding::type source_encoding,
                                       Encoding::type roundtrip_encoding,
                                       const ColumnDescriptor* descr, int num_values)
    : FuzzEncodingHeader(source_encoding, roundtrip_encoding, descr->physical_type(),
                         descr->type_length(), num_values) {}

std::string FuzzEncodingHeader::Serialize() const {
  PackedEncodingHeader packed{};
  packed.header.source_encoding_id = static_cast<uint8_t>(source_encoding);
  packed.header.roundtrip_encoding_id = static_cast<uint8_t>(roundtrip_encoding);
  packed.header.type_id = static_cast<uint8_t>(type);
  packed.header.type_length = type_length;
  packed.header.num_values = num_values;
  return std::string(reinterpret_cast<const char*>(&packed), kPackedEncodingHeaderSize);
}

::arrow::Result<FuzzEncodingHeader::ParseResult> FuzzEncodingHeader::Parse(
    std::span<const uint8_t> payload) {
  auto invalid_payload = []() {
    return Status::Invalid("Invalid fuzz encoding payload");
  };

  if (payload.size() < kPackedEncodingHeaderSize) {
    return invalid_payload();
  }
  PackedEncodingHeader packed;
  std::memcpy(&packed, payload.data(), kPackedEncodingHeaderSize);
  // We are strict in what we accept because we don't want the fuzzer to go
  // explore pointless variations.
  if (packed.reserved !=
      PackedEncodingHeader::Reserved{}) {  // reserved bytes should be zero
    return invalid_payload();
  }
  const auto& ph = packed.header;
  if (ph.source_encoding_id >= static_cast<uint8_t>(Encoding::UNDEFINED) ||
      ph.roundtrip_encoding_id >= static_cast<uint8_t>(Encoding::UNDEFINED) ||
      ph.type_id >= static_cast<uint8_t>(Type::UNDEFINED)) {
    return invalid_payload();
  }
  FuzzEncodingHeader header(static_cast<Encoding::type>(ph.source_encoding_id),
                            static_cast<Encoding::type>(ph.roundtrip_encoding_id),
                            static_cast<Type::type>(ph.type_id), ph.type_length,
                            ph.num_values);
  if ((header.type == Type::FIXED_LEN_BYTE_ARRAY) ? (header.type_length <= 0)
                                                  : (header.type_length != -1)) {
    return invalid_payload();
  }
  if (header.num_values < 0) {
    return invalid_payload();
  }
  return ParseResult{header, payload.subspan(kPackedEncodingHeaderSize)};
}

namespace {

// Just to use std::vector<T> while avoiding std::vector<bool>
using BooleanSlot = std::array<uint8_t, sizeof(bool)>;

template <typename DType>
struct TypedFuzzEncoding {
  static constexpr Type::type kType = DType::type_num;

  using c_type =
      std::conditional_t<kType == Type::BOOLEAN, BooleanSlot, typename DType::c_type>;
  using EncoderType = typename EncodingTraits<DType>::Encoder;
  using DecoderType = typename EncodingTraits<DType>::Decoder;
  using Accumulator = EncodingTraits<DType>::Accumulator;

  TypedFuzzEncoding(Encoding::type source_encoding, Encoding::type roundtrip_encoding,
                    const ColumnDescriptor* descr, int num_values,
                    std::span<const uint8_t> encoded_data)
      : source_encoding_(source_encoding),
        roundtrip_encoding_(roundtrip_encoding),
        descr_(descr),
        num_values_(num_values),
        encoded_data_(encoded_data) {}

  Result<std::vector<c_type>> Decode(Encoding::type encoding,
                                     std::span<const uint8_t> encoded_data,
                                     int chunk_size) {
    std::vector<c_type> values;
    int total_values = 0;

    BEGIN_PARQUET_CATCH_EXCEPTIONS
    auto decoder = MakeDecoder(encoding);
    // NOTE: In real API usage, the `num_values` given to SetData() is read from
    // the data page header and can include a number of nulls, so it's merely an
    // upper bound for the number of physical values.
    // However, Decode() calls are not supposed to ask more than the actual number
    // of physical values.
    decoder->SetData(num_values_, encoded_data.data(),
                     static_cast<int>(encoded_data.size()));
    while (total_values < num_values_) {
      const int read_size = std::min(num_values_ - total_values, chunk_size);
      values.resize(total_values + read_size);
      int values_read;
      if constexpr (kType == Type::BOOLEAN) {
        values_read = decoder->Decode(
            reinterpret_cast<bool*>(values.data() + total_values), read_size);
      } else {
        values_read = decoder->Decode(values.data() + total_values, read_size);
      }
      total_values += values_read;
      if (values_read < read_size) {
        values.resize(total_values);
        break;
      }
    }
    END_PARQUET_CATCH_EXCEPTIONS

    return values;
  }

  Result<std::shared_ptr<Array>> DecodeArrow(Encoding::type encoding,
                                             std::span<const uint8_t> encoded_data) {
    ARROW_ASSIGN_OR_RAISE(auto arrow_type, ArrowType());
    auto decoder = MakeDecoder(encoding);
    decoder->SetData(num_values_, encoded_data.data(),
                     static_cast<int>(encoded_data.size()));

    if constexpr (kType == Type::BYTE_ARRAY) {
      Accumulator acc;
      acc.builder = std::make_unique<::arrow::BinaryBuilder>(pool());
      BEGIN_PARQUET_CATCH_EXCEPTIONS
      decoder->DecodeArrowNonNull(num_values_, &acc);
      END_PARQUET_CATCH_EXCEPTIONS
      ARROW_CHECK_EQ(acc.chunks.size(), 0);
      return acc.builder->Finish();
    } else {
      Accumulator builder(arrow_type, pool());
      BEGIN_PARQUET_CATCH_EXCEPTIONS
      decoder->DecodeArrowNonNull(num_values_, &builder);
      END_PARQUET_CATCH_EXCEPTIONS
      return builder.Finish();
    }
  }

  Status Fuzz() {
    const std::vector<int> chunk_sizes = {(num_values_ + 1), 1 << 8, 1 << 12};

    // Decode using source encoding
    if constexpr (arrow_supported()) {
      // Read as Arrow directly and use that as reference
      ARROW_ASSIGN_OR_RAISE(reference_array_,
                            DecodeArrow(source_encoding_, encoded_data_));
      ARROW_CHECK_OK(reference_array_->ValidateFull());
    } else {
      // Persist raw reference values, they shouldn't carry embedded pointers
      // to short-lived decoder buffers.
      static_assert(kType != Type::FIXED_LEN_BYTE_ARRAY && kType != Type::BYTE_ARRAY);
      ARROW_ASSIGN_OR_RAISE(reference_values_,
                            Decode(source_encoding_, encoded_data_, num_values_));
    }

    // Re-encode and re-decode using roundtrip encoding
    {
      auto encoder = MakeEncoder(roundtrip_encoding_);
      BEGIN_PARQUET_CATCH_EXCEPTIONS
      if constexpr (arrow_supported()) {
        encoder->Put(*reference_array_);
        auto reencoded_buffer = encoder->FlushValues();
        auto reencoded_data = reencoded_buffer->template span_as<uint8_t>();
        auto array = DecodeArrow(roundtrip_encoding_, reencoded_data).ValueOrDie();
        ARROW_CHECK_OK(array->ValidateFull());
        ARROW_CHECK_OK(CompareAgainstReference(array));
        // Compare with reading raw values
        for (const int chunk_size : chunk_sizes) {
          auto values =
              Decode(roundtrip_encoding_, reencoded_data, chunk_size).ValueOrDie();
          ARROW_CHECK_OK(CompareAgainstReference(values));
        }
      } else {
        encoder->Put(reference_values_);
        auto reencoded_buffer = encoder->FlushValues();
        auto reencoded_data = reencoded_buffer->template span_as<uint8_t>();
        auto values =
            Decode(roundtrip_encoding_, reencoded_data, /*chunk_size=*/num_values_ + 1)
                .ValueOrDie();
        ARROW_CHECK_OK(CompareAgainstReference(values));
        // Try other chunk sizes
        for (const int chunk_size : chunk_sizes) {
          auto values =
              Decode(roundtrip_encoding_, reencoded_data, chunk_size).ValueOrDie();
          ARROW_CHECK_OK(CompareAgainstReference(values));
        }
      }
      END_PARQUET_CATCH_EXCEPTIONS
    }

    return Status::OK();
  }

 protected:
  Result<std::shared_ptr<Array>> MakeArrow(std::span<const c_type> values) {
    ARROW_ASSIGN_OR_RAISE(auto arrow_type, ArrowType());
    const int64_t length = static_cast<int64_t>(values.size());

    if constexpr (kType == Type::FIXED_LEN_BYTE_ARRAY) {
      // Use a buffer builder instead of FixedSizeBinaryBuilder to avoid the cost
      // of generating a trivial validity bitmap bit by bit.
      const int32_t byte_width = descr_->type_length();
      BufferBuilder data_builder(pool());
      RETURN_NOT_OK(data_builder.Reserve(length * byte_width));
      for (const FLBA item : values) {
        data_builder.UnsafeAppend(item.ptr, byte_width);
      }
      ARROW_ASSIGN_OR_RAISE(auto data_buffer, data_builder.Finish());
      auto data = ArrayData::Make(arrow_type, length, {nullptr, data_buffer},
                                  /*null_count=*/0);
      return ::arrow::MakeArray(data);
    } else if constexpr (kType == Type::BYTE_ARRAY) {
      TypedBufferBuilder<int32_t> offsets_builder(pool());
      BufferBuilder data_builder(pool());
      int64_t total_data_size = 0;
      for (const ByteArray item : values) {
        total_data_size += item.len;
      }
      RETURN_NOT_OK(offsets_builder.Reserve(length + 1));
      RETURN_NOT_OK(data_builder.Reserve(total_data_size));
      int32_t offset = 0;
      for (const ByteArray item : values) {
        offsets_builder.UnsafeAppend(offset);
        data_builder.UnsafeAppend(item.ptr, static_cast<int32_t>(item.len));
        offset += static_cast<int32_t>(item.len);
      }
      offsets_builder.UnsafeAppend(offset);
      ARROW_CHECK_EQ(offset, total_data_size);
      ARROW_ASSIGN_OR_RAISE(auto offsets_buffer, offsets_builder.Finish());
      ARROW_ASSIGN_OR_RAISE(auto data_buffer, data_builder.Finish());
      auto data =
          ArrayData::Make(arrow_type, length, {nullptr, offsets_buffer, data_buffer},
                          /*null_count=*/0);
      return ::arrow::MakeArray(data);
    } else if constexpr (kType == Type::BOOLEAN) {
      // Convert C++ bools into validity bitmap
      ::arrow::BooleanBuilder builder(pool());
      auto bool_data = reinterpret_cast<const bool*>(values.data());
      RETURN_NOT_OK(builder.AppendValues(bool_data, bool_data + values.size()));
      return builder.Finish();
    } else {
      auto data_buffer = std::make_shared<Buffer>(
          reinterpret_cast<const uint8_t*>(values.data()), length * sizeof(c_type));
      auto data = ArrayData::Make(arrow_type, length, {nullptr, data_buffer},
                                  /*null_count=*/0);
      return ::arrow::MakeArray(data);
    }
  }

  Status CompareAgainstReference(const std::shared_ptr<Array>& array) {
    std::stringstream ss;
    auto options = ::arrow::EqualOptions{}.nans_equal(true).diff_sink(&ss);
    if (!reference_array_->Equals(array, options)) {
      return Status::Invalid("Arrays unequal: ", ss.str());
    }
    return Status::OK();
  }

  Status CompareAgainstReference(std::span<const c_type> values) {
    if constexpr (arrow_supported()) {
      ARROW_CHECK_OK(CompareAgainstReference(reference_array_));
    } else {
      static_assert(kType == Type::INT96);
      if (reference_values_.size() != values.size() ||
          memcmp(reference_values_.data(), values.data(), values.size_bytes()) != 0) {
        return Status::Invalid("Values unequal");
      }
    }
    return Status::OK();
  }

  Result<std::shared_ptr<DataType>> ArrowType() const {
    switch (kType) {
      case Type::BOOLEAN:
        return ::arrow::boolean();
      case Type::INT32:
        return ::arrow::int32();
      case Type::INT64:
        return ::arrow::int64();
      case Type::FLOAT:
        return ::arrow::float32();
      case Type::DOUBLE:
        return ::arrow::float64();
      case Type::BYTE_ARRAY:
        return ::arrow::binary();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return ::arrow::fixed_size_binary(descr_->type_length());
      default:
        return Status::NotImplemented("Physical type does not have Arrow equivalent");
    }
  }

  std::shared_ptr<DecoderType> MakeDecoder(Encoding::type encoding) {
    auto decoder = std::dynamic_pointer_cast<DecoderType>(
        std::shared_ptr(::parquet::MakeDecoder(kType, encoding, descr_, pool())));
    ARROW_CHECK_NE(decoder, nullptr);
    return decoder;
  }

  std::shared_ptr<EncoderType> MakeEncoder(Encoding::type encoding) {
    auto encoder =
        std::dynamic_pointer_cast<EncoderType>(std::shared_ptr(::parquet::MakeEncoder(
            kType, encoding, /*use_dictionary=*/false, descr_, pool())));
    ARROW_CHECK_NE(encoder, nullptr);
    return encoder;
  }

  MemoryPool* pool() { return ::arrow::internal::fuzzing_memory_pool(); }

  static constexpr bool arrow_supported() { return kType != Type::INT96; }

  const Encoding::type source_encoding_, roundtrip_encoding_;
  const ColumnDescriptor* descr_;
  const int num_values_;
  const std::span<const uint8_t> encoded_data_;

  std::shared_ptr<Array> reference_array_;
  // Only for INT96 as there is no strictly equivalent Arrow type
  std::vector<c_type> reference_values_;
};

}  // namespace

Status FuzzEncoding(const uint8_t* data, int64_t size) {
  constexpr auto kInt32Max = std::numeric_limits<int32_t>::max();

  ARROW_ASSIGN_OR_RAISE(const auto parse_result,
                        FuzzEncodingHeader::Parse(std::span(data, size)));
  auto& [header, encoded_data] = parse_result;
  if (encoded_data.size() > static_cast<size_t>(kInt32Max)) {
    // Unlikely but who knows?
    return Status::Invalid("Fuzz payload too large");
  }
  const auto descr = MakeColumnDescriptor(header.type, header.type_length);

  BEGIN_PARQUET_CATCH_EXCEPTIONS

  auto typed_fuzz = [&](auto* dtype) {
    using DType = std::decay_t<decltype(*dtype)>;
    TypedFuzzEncoding<DType> typed_fuzz{header.source_encoding, header.roundtrip_encoding,
                                        &descr, header.num_values, encoded_data};
    return typed_fuzz.Fuzz();
  };
  RETURN_NOT_OK(VisitType(header.type, typed_fuzz));

  END_PARQUET_CATCH_EXCEPTIONS
  return Status::OK();
}

}  // namespace parquet::fuzzing::internal
