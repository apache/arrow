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
#include <functional>
#include <limits>
#include <sstream>
#include <string_view>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer_builder.h"
#include "arrow/compare.h"
#include "arrow/io/memory.h"
#include "arrow/pretty_print.h"
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
  using Accumulator = typename EncodingTraits<DType>::Accumulator;

  TypedFuzzEncoding(Encoding::type source_encoding, Encoding::type roundtrip_encoding,
                    const ColumnDescriptor* descr, int num_values,
                    std::span<const uint8_t> encoded_data)
      : source_encoding_(source_encoding),
        roundtrip_encoding_(roundtrip_encoding),
        descr_(descr),
        num_values_(num_values),
        encoded_data_(encoded_data) {}

  // Decoders of string-like types return pointers into the
  // decoder's internal scratch space, which get invalidated on the
  // following decoder call. We circumvent the issue by executing a
  // functor on each decoded chunk before moving to the next one.
  Status RunOnDecodedChunks(Encoding::type encoding,
                            std::span<const uint8_t> encoded_data, int chunk_size,
                            std::function<Status(int offset, std::vector<c_type>)> func) {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    int total_values = 0;
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
      // ARROW_ASSIGN_OR_RAISE(auto chunk_values, DecodeChunk(read_size));
      std::vector<c_type> chunk_values(read_size);
      int values_read;
      if constexpr (kType == Type::BOOLEAN) {
        values_read =
            decoder->Decode(reinterpret_cast<bool*>(chunk_values.data()), read_size);
      } else {
        values_read = decoder->Decode(chunk_values.data(), read_size);
      }
      ARROW_CHECK_LE(values_read, read_size);
      chunk_values.resize(values_read);
      RETURN_NOT_OK(func(total_values, std::move(chunk_values)));
      total_values += values_read;
      if (values_read < chunk_size) {
        break;
      }
    }
    if (total_values < num_values_) {
      return Status::Invalid("Read less values than expected");
    }
    END_PARQUET_CATCH_EXCEPTIONS
    return Status::OK();
  }

  Result<std::vector<c_type>> Decode(Encoding::type encoding,
                                     std::span<const uint8_t> encoded_data,
                                     int chunk_size) {
    // Decoded chunk values shouldn't embed pointers to decoder scratch space.
    static_assert(decoded_values_can_be_persisted());
    std::vector<c_type> values;
    auto accumulate_chunk = [&](int offset, std::vector<c_type> chunk_values) {
      values.insert(values.end(), chunk_values.begin(), chunk_values.end());
      return Status::OK();
    };
    RETURN_NOT_OK(
        RunOnDecodedChunks(encoding, encoded_data, chunk_size, accumulate_chunk));
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
    // Decode using source encoding
    if constexpr (arrow_supported()) {
      // Read as Arrow directly and use that as reference
      ARROW_ASSIGN_OR_RAISE(reference_array_,
                            DecodeArrow(source_encoding_, encoded_data_));
      ARROW_CHECK_OK(reference_array_->ValidateFull());
    } else {
      ARROW_ASSIGN_OR_RAISE(reference_values_,
                            Decode(source_encoding_, encoded_data_, num_values_));
    }

    // Re-encode and re-decode using roundtrip encoding
    {
      auto compare_chunk = [&](int offset, std::vector<c_type> chunk_values) {
        return CompareChunkAgainstReference(offset, chunk_values);
      };
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
        for (const int chunk_size : chunk_sizes()) {
          ARROW_CHECK_OK(RunOnDecodedChunks(roundtrip_encoding_, reencoded_data,
                                            chunk_size, compare_chunk));
        }
      } else {
        encoder->Put(reference_values_);
        auto reencoded_buffer = encoder->FlushValues();
        auto reencoded_data = reencoded_buffer->template span_as<uint8_t>();
        // Vary chunk sizes
        for (const int chunk_size : chunk_sizes()) {
          ARROW_CHECK_OK(RunOnDecodedChunks(roundtrip_encoding_, reencoded_data,
                                            chunk_size, compare_chunk));
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
      // Convert C++ bools into bitmap
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

  Status CompareChunkAgainstReference(int chunk_offset,
                                      const std::shared_ptr<Array>& array) {
    auto options = ::arrow::EqualOptions{}.nans_equal(true);
    ARROW_ASSIGN_OR_RAISE(auto expected,
                          reference_array_->SliceSafe(chunk_offset, array->length()));
    if (expected->length() != array->length()) {
      return Status::Invalid("Array lengths unequal: expected ", expected->length(),
                             ", got ", array->length());
    }
    if (!expected->Equals(array, options)) {
      std::stringstream ss;
      ::arrow::PrettyPrintOptions options(/*indent=*/2);
      options.window = 50;
      ss << "Arrays unequal, expected:\n";
      ARROW_UNUSED(PrettyPrint(*expected, options, &ss));
      ss << "Got:\n";
      ARROW_UNUSED(PrettyPrint(*array, options, &ss));
      return Status::Invalid("Arrays unequal: ", ss.str());
    }
    return Status::OK();
  }

  Status CompareAgainstReference(const std::shared_ptr<Array>& array) {
    return CompareChunkAgainstReference(/*chunk_offset=*/0, array);
  }

  Status CompareChunkAgainstReference(int chunk_offset, std::span<const c_type> values) {
    if constexpr (arrow_supported()) {
      ARROW_ASSIGN_OR_RAISE(auto array, MakeArrow(values));
      RETURN_NOT_OK(CompareChunkAgainstReference(chunk_offset, array));
    } else {
      static_assert(kType == Type::INT96);
      if (reference_values_.size() - static_cast<size_t>(chunk_offset) < values.size() ||
          memcmp(reference_values_.data() + chunk_offset, values.data(),
                 values.size_bytes()) != 0) {
        return Status::Invalid("Values unequal");
      }
    }
    return Status::OK();
  }

  Status CompareAgainstReference(std::span<const c_type> values) {
    return CompareChunkAgainstReference(/*chunk_offset=*/0, values);
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

  static constexpr bool decoded_values_can_be_persisted() {
    // Decoding string-like types returns pointers into the decoder's scratch space,
    // which can not assume to remain valid after another decoder call.
    return kType != Type::FIXED_LEN_BYTE_ARRAY && kType != Type::BYTE_ARRAY;
  }

  std::vector<int> chunk_sizes() const { return {(num_values_ + 1), 1 << 8, 1 << 12}; }

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
  const auto header = parse_result.first;
  const auto encoded_data = parse_result.second;
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
