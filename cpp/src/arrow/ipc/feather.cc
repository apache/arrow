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

#include "arrow/ipc/feather.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/util.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

#include "generated/feather_generated.h"

namespace arrow {

using internal::checked_cast;

class ExtensionType;

namespace ipc {
namespace feather {

namespace {

using FBB = flatbuffers::FlatBufferBuilder;

constexpr const char* kFeatherV1MagicBytes = "FEA1";
constexpr const int kFeatherDefaultAlignment = 8;
const uint8_t kPaddingBytes[kFeatherDefaultAlignment] = {0};

inline int64_t PaddedLength(int64_t nbytes) {
  static const int64_t alignment = kFeatherDefaultAlignment;
  return ((nbytes + alignment - 1) / alignment) * alignment;
}

Status WritePaddedWithOffset(io::OutputStream* stream, const uint8_t* data,
                             int64_t bit_offset, const int64_t length,
                             int64_t* bytes_written) {
  data = data + bit_offset / 8;
  uint8_t bit_shift = static_cast<uint8_t>(bit_offset % 8);
  if (bit_offset == 0) {
    RETURN_NOT_OK(stream->Write(data, length));
  } else {
    constexpr int64_t buffersize = 256;
    uint8_t buffer[buffersize];
    const uint8_t lshift = static_cast<uint8_t>(8 - bit_shift);
    const uint8_t* buffer_end = buffer + buffersize;
    uint8_t* buffer_it = buffer;

    for (const uint8_t* end = data + length; data != end;) {
      uint8_t r = static_cast<uint8_t>(*data++ >> bit_shift);
      uint8_t l = static_cast<uint8_t>(*data << lshift);
      uint8_t value = l | r;
      *buffer_it++ = value;
      if (buffer_it == buffer_end) {
        RETURN_NOT_OK(stream->Write(buffer, buffersize));
        buffer_it = buffer;
      }
    }
    if (buffer_it != buffer) {
      RETURN_NOT_OK(stream->Write(buffer, buffer_it - buffer));
    }
  }

  int64_t remainder = PaddedLength(length) - length;
  if (remainder != 0) {
    RETURN_NOT_OK(stream->Write(kPaddingBytes, remainder));
  }
  *bytes_written = length + remainder;
  return Status::OK();
}

Status WritePadded(io::OutputStream* stream, const uint8_t* data, int64_t length,
                   int64_t* bytes_written) {
  return WritePaddedWithOffset(stream, data, /*bit_offset=*/0, length, bytes_written);
}

struct ColumnType {
  enum type { PRIMITIVE, CATEGORY, TIMESTAMP, DATE, TIME };
};

inline TimeUnit::type FromFlatbufferEnum(fbs::TimeUnit unit) {
  return static_cast<TimeUnit::type>(static_cast<int>(unit));
}

/// For compatibility, we need to write any data sometimes just to keep producing
/// files that can be read with an older reader.
Status WritePaddedBlank(io::OutputStream* stream, int64_t length,
                        int64_t* bytes_written) {
  const uint8_t null = 0;
  for (int64_t i = 0; i < length; i++) {
    RETURN_NOT_OK(stream->Write(&null, 1));
  }
  int64_t remainder = PaddedLength(length) - length;
  if (remainder != 0) {
    RETURN_NOT_OK(stream->Write(kPaddingBytes, remainder));
  }
  *bytes_written = length + remainder;
  return Status::OK();
}

// ----------------------------------------------------------------------
// ReaderV1

class ReaderV1 : public Reader {
 public:
  Status Open(const std::shared_ptr<io::RandomAccessFile>& source) {
    source_ = source;

    ARROW_ASSIGN_OR_RAISE(int64_t size, source->GetSize());
    int magic_size = static_cast<int>(strlen(kFeatherV1MagicBytes));
    int footer_size = magic_size + static_cast<int>(sizeof(uint32_t));

    // Now get the footer and verify
    ARROW_ASSIGN_OR_RAISE(auto buffer, source->ReadAt(size - footer_size, footer_size));

    if (memcmp(buffer->data() + sizeof(uint32_t), kFeatherV1MagicBytes, magic_size)) {
      return Status::Invalid("Feather file footer incomplete");
    }

    uint32_t metadata_length = *reinterpret_cast<const uint32_t*>(buffer->data());
    if (size < magic_size + footer_size + metadata_length) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }
    ARROW_ASSIGN_OR_RAISE(
        metadata_buffer_,
        source->ReadAt(size - footer_size - metadata_length, metadata_length));

    metadata_ = fbs::GetCTable(metadata_buffer_->data());
    return ReadSchema();
  }

  Status ReadSchema() {
    std::vector<std::shared_ptr<Field>> fields;
    for (int i = 0; i < static_cast<int>(metadata_->columns()->size()); ++i) {
      const fbs::Column* col = metadata_->columns()->Get(i);
      std::shared_ptr<DataType> type;
      RETURN_NOT_OK(
          GetDataType(col->values(), col->metadata_type(), col->metadata(), &type));
      fields.push_back(::arrow::field(col->name()->str(), type));
    }
    schema_ = ::arrow::schema(std::move(fields));
    return Status::OK();
  }

  Status GetDataType(const fbs::PrimitiveArray* values, fbs::TypeMetadata metadata_type,
                     const void* metadata, std::shared_ptr<DataType>* out) {
#define PRIMITIVE_CASE(CAP_TYPE, FACTORY_FUNC) \
  case fbs::Type::CAP_TYPE:                    \
    *out = FACTORY_FUNC();                     \
    break;

    switch (metadata_type) {
      case fbs::TypeMetadata::CategoryMetadata: {
        auto meta = static_cast<const fbs::CategoryMetadata*>(metadata);

        std::shared_ptr<DataType> index_type, dict_type;
        RETURN_NOT_OK(GetDataType(values, fbs::TypeMetadata::NONE, nullptr, &index_type));
        RETURN_NOT_OK(
            GetDataType(meta->levels(), fbs::TypeMetadata::NONE, nullptr, &dict_type));
        *out = dictionary(index_type, dict_type, meta->ordered());
        break;
      }
      case fbs::TypeMetadata::TimestampMetadata: {
        auto meta = static_cast<const fbs::TimestampMetadata*>(metadata);
        TimeUnit::type unit = FromFlatbufferEnum(meta->unit());
        std::string tz;
        // flatbuffer non-null
        if (meta->timezone() != 0) {
          tz = meta->timezone()->str();
        } else {
          tz = "";
        }
        *out = timestamp(unit, tz);
      } break;
      case fbs::TypeMetadata::DateMetadata:
        *out = date32();
        break;
      case fbs::TypeMetadata::TimeMetadata: {
        auto meta = static_cast<const fbs::TimeMetadata*>(metadata);
        *out = time32(FromFlatbufferEnum(meta->unit()));
      } break;
      default:
        switch (values->type()) {
          PRIMITIVE_CASE(BOOL, boolean);
          PRIMITIVE_CASE(INT8, int8);
          PRIMITIVE_CASE(INT16, int16);
          PRIMITIVE_CASE(INT32, int32);
          PRIMITIVE_CASE(INT64, int64);
          PRIMITIVE_CASE(UINT8, uint8);
          PRIMITIVE_CASE(UINT16, uint16);
          PRIMITIVE_CASE(UINT32, uint32);
          PRIMITIVE_CASE(UINT64, uint64);
          PRIMITIVE_CASE(FLOAT, float32);
          PRIMITIVE_CASE(DOUBLE, float64);
          PRIMITIVE_CASE(UTF8, utf8);
          PRIMITIVE_CASE(BINARY, binary);
          PRIMITIVE_CASE(LARGE_UTF8, large_utf8);
          PRIMITIVE_CASE(LARGE_BINARY, large_binary);
          default:
            return Status::Invalid("Unrecognized type");
        }
        break;
    }

#undef PRIMITIVE_CASE

    return Status::OK();
  }

  int64_t GetOutputLength(int64_t nbytes) {
    // XXX: Hack for Feather 0.3.0 for backwards compatibility with old files
    // Size in-file of written byte buffer
    if (version() < 2) {
      // Feather files < 0.3.0
      return nbytes;
    } else {
      return PaddedLength(nbytes);
    }
  }

  // Retrieve a primitive array from the data source
  //
  // @returns: a Buffer instance, the precise type will depend on the kind of
  // input data source (which may or may not have memory-map like semantics)
  Status LoadValues(std::shared_ptr<DataType> type, const fbs::PrimitiveArray* meta,
                    fbs::TypeMetadata metadata_type, const void* metadata,
                    std::shared_ptr<ArrayData>* out) {
    std::vector<std::shared_ptr<Buffer>> buffers;

    // Buffer data from the source (may or may not perform a copy depending on
    // input source)
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          source_->ReadAt(meta->offset(), meta->total_bytes()));

    int64_t offset = 0;

    if (type->id() == Type::DICTIONARY) {
      // Load the index type values
      type = checked_cast<const DictionaryType&>(*type).index_type();
    }

    // If there are nulls, the null bitmask is first
    if (meta->null_count() > 0) {
      int64_t null_bitmap_size = GetOutputLength(bit_util::BytesForBits(meta->length()));
      buffers.push_back(SliceBuffer(buffer, offset, null_bitmap_size));
      offset += null_bitmap_size;
    } else {
      buffers.push_back(nullptr);
    }

    if (is_binary_like(type->id())) {
      int64_t offsets_size = GetOutputLength((meta->length() + 1) * sizeof(int32_t));
      buffers.push_back(SliceBuffer(buffer, offset, offsets_size));
      offset += offsets_size;
    } else if (is_large_binary_like(type->id())) {
      int64_t offsets_size = GetOutputLength((meta->length() + 1) * sizeof(int64_t));
      buffers.push_back(SliceBuffer(buffer, offset, offsets_size));
      offset += offsets_size;
    }

    buffers.push_back(SliceBuffer(buffer, offset, buffer->size() - offset));

    *out = ArrayData::Make(type, meta->length(), std::move(buffers), meta->null_count());
    return Status::OK();
  }

  int version() const override { return metadata_->version(); }
  int64_t num_rows() const { return metadata_->num_rows(); }

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status GetDictionary(int field_index, std::shared_ptr<ArrayData>* out) {
    const fbs::Column* col_meta = metadata_->columns()->Get(field_index);
    auto dict_meta = col_meta->metadata_as<fbs::CategoryMetadata>();
    const auto& dict_type =
        checked_cast<const DictionaryType&>(*schema_->field(field_index)->type());

    return LoadValues(dict_type.value_type(), dict_meta->levels(),
                      fbs::TypeMetadata::NONE, nullptr, out);
  }

  Status GetColumn(int field_index, std::shared_ptr<ChunkedArray>* out) {
    const fbs::Column* col_meta = metadata_->columns()->Get(field_index);
    std::shared_ptr<ArrayData> data;

    auto type = schema_->field(field_index)->type();
    RETURN_NOT_OK(LoadValues(type, col_meta->values(), col_meta->metadata_type(),
                             col_meta->metadata(), &data));

    if (type->id() == Type::DICTIONARY) {
      RETURN_NOT_OK(GetDictionary(field_index, &data->dictionary));
      data->type = type;
    }
    *out = std::make_shared<ChunkedArray>(MakeArray(data));
    return Status::OK();
  }

  Status Read(std::shared_ptr<Table>* out) override {
    std::vector<std::shared_ptr<ChunkedArray>> columns;
    for (int i = 0; i < static_cast<int>(metadata_->columns()->size()); ++i) {
      columns.emplace_back();
      RETURN_NOT_OK(GetColumn(i, &columns.back()));
    }
    *out = Table::Make(this->schema(), std::move(columns), this->num_rows());
    return Status::OK();
  }

  Status Read(const std::vector<int>& indices, std::shared_ptr<Table>* out) override {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;

    auto my_schema = this->schema();
    for (auto field_index : indices) {
      if (field_index < 0 || field_index >= my_schema->num_fields()) {
        return Status::Invalid("Field index ", field_index, " is out of bounds");
      }
      columns.emplace_back();
      RETURN_NOT_OK(GetColumn(field_index, &columns.back()));
      fields.push_back(my_schema->field(field_index));
    }
    *out = Table::Make(::arrow::schema(std::move(fields)), std::move(columns),
                       this->num_rows());
    return Status::OK();
  }

  Status Read(const std::vector<std::string>& names,
              std::shared_ptr<Table>* out) override {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;

    std::shared_ptr<Schema> sch = this->schema();
    for (auto name : names) {
      int field_index = sch->GetFieldIndex(name);
      if (field_index == -1) {
        return Status::Invalid("Field named ", name, " is not found");
      }
      columns.emplace_back();
      RETURN_NOT_OK(GetColumn(field_index, &columns.back()));
      fields.push_back(sch->field(field_index));
    }
    *out = Table::Make(::arrow::schema(std::move(fields)), std::move(columns),
                       this->num_rows());
    return Status::OK();
  }

 private:
  std::shared_ptr<io::RandomAccessFile> source_;
  std::shared_ptr<Buffer> metadata_buffer_;
  const fbs::CTable* metadata_;
  std::shared_ptr<Schema> schema_;
};

// ----------------------------------------------------------------------
// WriterV1

struct ArrayMetadata {
  fbs::Type type;
  int64_t offset;
  int64_t length;
  int64_t null_count;
  int64_t total_bytes;
};

#define TO_FLATBUFFER_CASE(TYPE) \
  case Type::TYPE:               \
    return fbs::Type::TYPE;

Result<fbs::Type> ToFlatbufferType(const DataType& type) {
  switch (type.id()) {
    TO_FLATBUFFER_CASE(BOOL);
    TO_FLATBUFFER_CASE(INT8);
    TO_FLATBUFFER_CASE(INT16);
    TO_FLATBUFFER_CASE(INT32);
    TO_FLATBUFFER_CASE(INT64);
    TO_FLATBUFFER_CASE(UINT8);
    TO_FLATBUFFER_CASE(UINT16);
    TO_FLATBUFFER_CASE(UINT32);
    TO_FLATBUFFER_CASE(UINT64);
    TO_FLATBUFFER_CASE(FLOAT);
    TO_FLATBUFFER_CASE(DOUBLE);
    TO_FLATBUFFER_CASE(LARGE_BINARY);
    TO_FLATBUFFER_CASE(BINARY);
    case Type::STRING:
      return fbs::Type::UTF8;
    case Type::LARGE_STRING:
      return fbs::Type::LARGE_UTF8;
    case Type::DATE32:
      return fbs::Type::INT32;
    case Type::TIMESTAMP:
      return fbs::Type::INT64;
    case Type::TIME32:
      return fbs::Type::INT32;
    case Type::TIME64:
      return fbs::Type::INT64;
    default:
      return Status::TypeError("Unsupported Feather V1 type: ", type.ToString(),
                               ". Use V2 format to serialize all Arrow types.");
  }
}

inline flatbuffers::Offset<fbs::PrimitiveArray> GetPrimitiveArray(
    FBB& fbb, const ArrayMetadata& array) {
  return fbs::CreatePrimitiveArray(fbb, array.type, fbs::Encoding::PLAIN, array.offset,
                                   array.length, array.null_count, array.total_bytes);
}

// Convert Feather enums to Flatbuffer enums
inline fbs::TimeUnit ToFlatbufferEnum(TimeUnit::type unit) {
  return static_cast<fbs::TimeUnit>(static_cast<int>(unit));
}

const fbs::TypeMetadata COLUMN_TYPE_ENUM_MAPPING[] = {
    fbs::TypeMetadata::NONE,               // PRIMITIVE
    fbs::TypeMetadata::CategoryMetadata,   // CATEGORY
    fbs::TypeMetadata::TimestampMetadata,  // TIMESTAMP
    fbs::TypeMetadata::DateMetadata,       // DATE
    fbs::TypeMetadata::TimeMetadata        // TIME
};

inline fbs::TypeMetadata ToFlatbufferEnum(ColumnType::type column_type) {
  return COLUMN_TYPE_ENUM_MAPPING[column_type];
}

struct ColumnMetadata {
  flatbuffers::Offset<void> WriteMetadata(FBB& fbb) {  // NOLINT
    switch (this->meta_type) {
      case ColumnType::PRIMITIVE:
        // flatbuffer void
        return 0;
      case ColumnType::CATEGORY: {
        auto cat_meta = fbs::CreateCategoryMetadata(
            fbb, GetPrimitiveArray(fbb, this->category_levels), this->category_ordered);
        return cat_meta.Union();
      }
      case ColumnType::TIMESTAMP: {
        // flatbuffer void
        flatbuffers::Offset<flatbuffers::String> tz = 0;
        if (!this->timezone.empty()) {
          tz = fbb.CreateString(this->timezone);
        }

        auto ts_meta =
            fbs::CreateTimestampMetadata(fbb, ToFlatbufferEnum(this->temporal_unit), tz);
        return ts_meta.Union();
      }
      case ColumnType::DATE: {
        auto date_meta = fbs::CreateDateMetadata(fbb);
        return date_meta.Union();
      }
      case ColumnType::TIME: {
        auto time_meta =
            fbs::CreateTimeMetadata(fbb, ToFlatbufferEnum(this->temporal_unit));
        return time_meta.Union();
      }
      default:
        // null
        DCHECK(false);
        return 0;
    }
  }

  ArrayMetadata values;
  ColumnType::type meta_type;

  ArrayMetadata category_levels;
  bool category_ordered;

  TimeUnit::type temporal_unit;

  // A timezone name known to the Olson timezone database. For display purposes
  // because the actual data is all UTC
  std::string timezone;
};

Status WriteArrayV1(const Array& values, io::OutputStream* dst, ArrayMetadata* meta);

struct ArrayWriterV1 {
  const Array& values;
  io::OutputStream* dst;
  ArrayMetadata* meta;

  Status WriteBuffer(const uint8_t* buffer, int64_t length, int64_t bit_offset) {
    int64_t bytes_written = 0;
    if (buffer) {
      RETURN_NOT_OK(
          WritePaddedWithOffset(dst, buffer, bit_offset, length, &bytes_written));
    } else {
      RETURN_NOT_OK(WritePaddedBlank(dst, length, &bytes_written));
    }
    meta->total_bytes += bytes_written;
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<
      is_nested_type<T>::value || is_null_type<T>::value || is_decimal_type<T>::value ||
          std::is_same<DictionaryType, T>::value || is_duration_type<T>::value ||
          is_interval_type<T>::value || is_fixed_size_binary_type<T>::value ||
          std::is_same<Date64Type, T>::value || std::is_same<Time64Type, T>::value ||
          std::is_same<ExtensionType, T>::value,
      Status>::type
  Visit(const T& type) {
    return Status::NotImplemented(type.ToString());
  }

  template <typename T>
  typename std::enable_if<is_number_type<T>::value ||
                              std::is_same<Date32Type, T>::value ||
                              std::is_same<Time32Type, T>::value ||
                              is_timestamp_type<T>::value || is_boolean_type<T>::value,
                          Status>::type
  Visit(const T&) {
    const auto& prim_values = checked_cast<const PrimitiveArray&>(values);
    const auto& fw_type = checked_cast<const FixedWidthType&>(*values.type());

    if (prim_values.values()) {
      const uint8_t* buffer =
          prim_values.values()->data() + (prim_values.offset() * fw_type.bit_width() / 8);
      int64_t bit_offset = (prim_values.offset() * fw_type.bit_width()) % 8;
      return WriteBuffer(buffer,
                         bit_util::BytesForBits(values.length() * fw_type.bit_width()),
                         bit_offset);
    } else {
      return Status::OK();
    }
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    const auto& ty_values = checked_cast<const ArrayType&>(values);

    using offset_type = typename T::offset_type;
    const offset_type* offsets_data = nullptr;
    int64_t values_bytes = 0;
    if (ty_values.value_offsets()) {
      offsets_data = ty_values.raw_value_offsets();
      // All of the data has to be written because we don't have offset
      // shifting implemented here as with the IPC format
      values_bytes = offsets_data[values.length()];
    }
    RETURN_NOT_OK(WriteBuffer(reinterpret_cast<const uint8_t*>(offsets_data),
                              sizeof(offset_type) * (values.length() + 1),
                              /*bit_offset=*/0));

    const uint8_t* values_buffer = nullptr;
    if (ty_values.value_data()) {
      values_buffer = ty_values.value_data()->data();
    }
    return WriteBuffer(values_buffer, values_bytes, /*bit_offset=*/0);
  }

  Status Write() {
    if (values.type_id() == Type::DICTIONARY) {
      return WriteArrayV1(*(checked_cast<const DictionaryArray&>(values).indices()), dst,
                          meta);
    }

    ARROW_ASSIGN_OR_RAISE(meta->type, ToFlatbufferType(*values.type()));
    ARROW_ASSIGN_OR_RAISE(meta->offset, dst->Tell());
    meta->length = values.length();
    meta->null_count = values.null_count();
    meta->total_bytes = 0;

    // Write the null bitmask
    if (values.null_count() > 0) {
      RETURN_NOT_OK(WriteBuffer(values.null_bitmap_data(),
                                bit_util::BytesForBits(values.length()),
                                values.offset()));
    }
    // Write data buffer(s)
    return VisitTypeInline(*values.type(), this);
  }
};

Status WriteArrayV1(const Array& values, io::OutputStream* dst, ArrayMetadata* meta) {
  std::shared_ptr<Array> sanitized;
  if (values.type_id() == Type::NA) {
    // As long as R doesn't support NA, we write this as a StringColumn
    // to ensure stable roundtrips.
    sanitized = std::make_shared<StringArray>(values.length(), nullptr, nullptr,
                                              values.null_bitmap(), values.null_count());
  } else {
    sanitized = MakeArray(values.data());
  }
  ArrayWriterV1 visitor{*sanitized, dst, meta};
  return visitor.Write();
}

Status WriteColumnV1(const ChunkedArray& values, io::OutputStream* dst,
                     ColumnMetadata* out) {
  if (values.num_chunks() > 1) {
    return Status::Invalid("Writing chunked arrays not supported in Feather V1");
  }
  const Array& chunk = *values.chunk(0);
  RETURN_NOT_OK(WriteArrayV1(chunk, dst, &out->values));
  switch (chunk.type_id()) {
    case Type::DICTIONARY: {
      out->meta_type = ColumnType::CATEGORY;
      auto dictionary = checked_cast<const DictionaryArray&>(chunk).dictionary();
      RETURN_NOT_OK(WriteArrayV1(*dictionary, dst, &out->category_levels));
      out->category_ordered =
          checked_cast<const DictionaryType&>(*chunk.type()).ordered();
    } break;
    case Type::DATE32:
      out->meta_type = ColumnType::DATE;
      break;
    case Type::TIME32: {
      out->meta_type = ColumnType::TIME;
      out->temporal_unit = checked_cast<const Time32Type&>(*chunk.type()).unit();
    } break;
    case Type::TIMESTAMP: {
      const auto& ts_type = checked_cast<const TimestampType&>(*chunk.type());
      out->meta_type = ColumnType::TIMESTAMP;
      out->temporal_unit = ts_type.unit();
      out->timezone = ts_type.timezone();
    } break;
    default:
      out->meta_type = ColumnType::PRIMITIVE;
      break;
  }
  return Status::OK();
}

Status WriteFeatherV1(const Table& table, io::OutputStream* dst) {
  // Preamble
  int64_t bytes_written;
  RETURN_NOT_OK(WritePadded(dst, reinterpret_cast<const uint8_t*>(kFeatherV1MagicBytes),
                            strlen(kFeatherV1MagicBytes), &bytes_written));

  // Write columns
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<flatbuffers::Offset<fbs::Column>> fb_columns;
  for (int i = 0; i < table.num_columns(); ++i) {
    ColumnMetadata col;
    RETURN_NOT_OK(WriteColumnV1(*table.column(i), dst, &col));
    auto fb_column = fbs::CreateColumn(
        fbb, fbb.CreateString(table.field(i)->name()), GetPrimitiveArray(fbb, col.values),
        ToFlatbufferEnum(col.meta_type), col.WriteMetadata(fbb),
        /*user_metadata=*/0);
    fb_columns.push_back(fb_column);
  }

  // Finalize file footer
  auto root = fbs::CreateCTable(fbb, /*description=*/0, table.num_rows(),
                                fbb.CreateVector(fb_columns), kFeatherV1Version,
                                /*metadata=*/0);
  fbb.Finish(root);
  auto buffer = std::make_shared<Buffer>(fbb.GetBufferPointer(),
                                         static_cast<int64_t>(fbb.GetSize()));

  // Writer metadata
  RETURN_NOT_OK(WritePadded(dst, buffer->data(), buffer->size(), &bytes_written));
  uint32_t metadata_size = static_cast<uint32_t>(bytes_written);

  // Footer: metadata length, magic bytes
  RETURN_NOT_OK(dst->Write(&metadata_size, sizeof(uint32_t)));
  return dst->Write(kFeatherV1MagicBytes, strlen(kFeatherV1MagicBytes));
}

// ----------------------------------------------------------------------
// Reader V2

class ReaderV2 : public Reader {
 public:
  Status Open(const std::shared_ptr<io::RandomAccessFile>& source,
              const IpcReadOptions& options) {
    source_ = source;
    options_ = options;
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchFileReader::Open(source_, options_));
    schema_ = reader->schema();
    return Status::OK();
  }

  Status Open(const std::shared_ptr<io::RandomAccessFile>& source) {
    return Open(source, IpcReadOptions::Defaults());
  }

  int version() const override { return kFeatherV2Version; }

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status Read(const IpcReadOptions& options, std::shared_ptr<Table>* out) {
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchFileReader::Open(source_, options));
    RecordBatchVector batches(reader->num_record_batches());
    for (int i = 0; i < reader->num_record_batches(); ++i) {
      ARROW_ASSIGN_OR_RAISE(batches[i], reader->ReadRecordBatch(i));
    }

    return Table::FromRecordBatches(reader->schema(), batches).Value(out);
  }

  Status Read(std::shared_ptr<Table>* out) override { return Read(options_, out); }

  Status Read(const std::vector<int>& indices, std::shared_ptr<Table>* out) override {
    auto options = options_;
    options.included_fields = indices;
    return Read(options, out);
  }

  Status Read(const std::vector<std::string>& names,
              std::shared_ptr<Table>* out) override {
    std::vector<int> indices;
    std::shared_ptr<Schema> sch = this->schema();
    for (auto name : names) {
      int field_index = sch->GetFieldIndex(name);
      if (field_index == -1) {
        return Status::Invalid("Field named ", name, " is not found");
      }
      indices.push_back(field_index);
    }
    return Read(indices, out);
  }

 private:
  std::shared_ptr<io::RandomAccessFile> source_;
  std::shared_ptr<Schema> schema_;
  IpcReadOptions options_;
};

}  // namespace

Result<std::shared_ptr<Reader>> Reader::Open(
    const std::shared_ptr<io::RandomAccessFile>& source) {
  return Reader::Open(source, IpcReadOptions::Defaults());
}

Result<std::shared_ptr<Reader>> Reader::Open(
    const std::shared_ptr<io::RandomAccessFile>& source, const IpcReadOptions& options) {
  // Pathological issue where the file is smaller than header and footer
  // combined
  ARROW_ASSIGN_OR_RAISE(int64_t size, source->GetSize());
  if (size < /* 2 * 4 + 4 */ 12) {
    return Status::Invalid("File is too small to be a well-formed file");
  }

  // Determine what kind of file we have. 6 is the max of len(FEA1) and
  // len(ARROW1)
  constexpr int magic_size = 6;
  ARROW_ASSIGN_OR_RAISE(auto buffer, source->ReadAt(0, magic_size));

  if (memcmp(buffer->data(), kFeatherV1MagicBytes, strlen(kFeatherV1MagicBytes)) == 0) {
    std::shared_ptr<ReaderV1> result = std::make_shared<ReaderV1>();
    // IPC Read options are ignored for ReaderV1
    RETURN_NOT_OK(result->Open(source));
    return result;
  } else if (memcmp(buffer->data(), internal::kArrowMagicBytes,
                    strlen(internal::kArrowMagicBytes)) == 0) {
    std::shared_ptr<ReaderV2> result = std::make_shared<ReaderV2>();
    RETURN_NOT_OK(result->Open(source, options));
    return result;
  } else {
    return Status::Invalid("Not a Feather V1 or Arrow IPC file");
  }
}

WriteProperties WriteProperties::Defaults() {
  WriteProperties result;
#ifdef ARROW_WITH_LZ4
  result.compression = Compression::LZ4_FRAME;
#else
  result.compression = Compression::UNCOMPRESSED;
#endif
  return result;
}

Status WriteTable(const Table& table, io::OutputStream* dst,
                  const WriteProperties& properties) {
  if (properties.version == kFeatherV1Version) {
    return WriteFeatherV1(table, dst);
  } else {
    IpcWriteOptions ipc_options = IpcWriteOptions::Defaults();
    ipc_options.unify_dictionaries = true;
    ipc_options.allow_64bit = true;
    ARROW_ASSIGN_OR_RAISE(
        ipc_options.codec,
        util::Codec::Create(properties.compression, properties.compression_level));

    std::shared_ptr<RecordBatchWriter> writer;
    ARROW_ASSIGN_OR_RAISE(writer, MakeFileWriter(dst, table.schema(), ipc_options));
    RETURN_NOT_OK(writer->WriteTable(table, properties.chunksize));
    return writer->Close();
  }
}

}  // namespace feather
}  // namespace ipc
}  // namespace arrow
