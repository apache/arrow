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
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/ipc/feather-internal.h"
#include "arrow/ipc/feather_generated.h"
#include "arrow/loader.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace ipc {
namespace feather {

static const uint8_t kPaddingBytes[kFeatherDefaultAlignment] = {0};

static inline int64_t PaddedLength(int64_t nbytes) {
  static const int64_t alignment = kFeatherDefaultAlignment;
  return ((nbytes + alignment - 1) / alignment) * alignment;
}

// XXX: Hack for Feather 0.3.0 for backwards compatibility with old files
// Size in-file of written byte buffer
static int64_t GetOutputLength(int64_t nbytes) {
  if (kFeatherVersion < 2) {
    // Feather files < 0.3.0
    return nbytes;
  } else {
    return PaddedLength(nbytes);
  }
}

static Status WritePadded(io::OutputStream* stream, const uint8_t* data, int64_t length,
    int64_t* bytes_written) {
  RETURN_NOT_OK(stream->Write(data, length));

  int64_t remainder = PaddedLength(length) - length;
  if (remainder != 0) { RETURN_NOT_OK(stream->Write(kPaddingBytes, remainder)); }
  *bytes_written = length + remainder;
  return Status::OK();
}

// ----------------------------------------------------------------------
// TableBuilder

TableBuilder::TableBuilder(int64_t num_rows) : finished_(false), num_rows_(num_rows) {}

FBB& TableBuilder::fbb() {
  return fbb_;
}

Status TableBuilder::Finish() {
  if (finished_) { return Status::Invalid("can only call this once"); }

  FBString desc = 0;
  if (!description_.empty()) { desc = fbb_.CreateString(description_); }

  flatbuffers::Offset<flatbuffers::String> metadata = 0;

  auto root = fbs::CreateCTable(
      fbb_, desc, num_rows_, fbb_.CreateVector(columns_), kFeatherVersion, metadata);
  fbb_.Finish(root);
  finished_ = true;

  return Status::OK();
}

std::shared_ptr<Buffer> TableBuilder::GetBuffer() const {
  return std::make_shared<Buffer>(
      fbb_.GetBufferPointer(), static_cast<int64_t>(fbb_.GetSize()));
}

void TableBuilder::SetDescription(const std::string& description) {
  description_ = description;
}

void TableBuilder::SetNumRows(int64_t num_rows) {
  num_rows_ = num_rows;
}

void TableBuilder::add_column(const flatbuffers::Offset<fbs::Column>& col) {
  columns_.push_back(col);
}

ColumnBuilder::ColumnBuilder(TableBuilder* parent, const std::string& name)
    : parent_(parent) {
  fbb_ = &parent->fbb();
  name_ = name;
  type_ = ColumnType::PRIMITIVE;
}

flatbuffers::Offset<void> ColumnBuilder::CreateColumnMetadata() {
  switch (type_) {
    case ColumnType::PRIMITIVE:
      // flatbuffer void
      return 0;
    case ColumnType::CATEGORY: {
      auto cat_meta = fbs::CreateCategoryMetadata(
          fbb(), GetPrimitiveArray(fbb(), meta_category_.levels), meta_category_.ordered);
      return cat_meta.Union();
    }
    case ColumnType::TIMESTAMP: {
      // flatbuffer void
      flatbuffers::Offset<flatbuffers::String> tz = 0;
      if (!meta_timestamp_.timezone.empty()) {
        tz = fbb().CreateString(meta_timestamp_.timezone);
      }

      auto ts_meta =
          fbs::CreateTimestampMetadata(fbb(), ToFlatbufferEnum(meta_timestamp_.unit), tz);
      return ts_meta.Union();
    }
    case ColumnType::DATE: {
      auto date_meta = fbs::CreateDateMetadata(fbb());
      return date_meta.Union();
    }
    case ColumnType::TIME: {
      auto time_meta = fbs::CreateTimeMetadata(fbb(), ToFlatbufferEnum(meta_time_.unit));
      return time_meta.Union();
    }
    default:
      // null
      return flatbuffers::Offset<void>();
  }
}

Status ColumnBuilder::Finish() {
  FBB& buf = fbb();

  // values
  auto values = GetPrimitiveArray(buf, values_);
  flatbuffers::Offset<void> metadata = CreateColumnMetadata();

  auto column = fbs::CreateColumn(buf, buf.CreateString(name_), values,
      ToFlatbufferEnum(type_),  // metadata_type
      metadata, buf.CreateString(user_metadata_));

  // bad coupling, but OK for now
  parent_->add_column(column);
  return Status::OK();
}

void ColumnBuilder::SetValues(const ArrayMetadata& values) {
  values_ = values;
}

void ColumnBuilder::SetUserMetadata(const std::string& data) {
  user_metadata_ = data;
}

void ColumnBuilder::SetCategory(const ArrayMetadata& levels, bool ordered) {
  type_ = ColumnType::CATEGORY;
  meta_category_.levels = levels;
  meta_category_.ordered = ordered;
}

void ColumnBuilder::SetTimestamp(TimeUnit unit) {
  type_ = ColumnType::TIMESTAMP;
  meta_timestamp_.unit = unit;
}

void ColumnBuilder::SetTimestamp(TimeUnit unit, const std::string& timezone) {
  SetTimestamp(unit);
  meta_timestamp_.timezone = timezone;
}

void ColumnBuilder::SetDate() {
  type_ = ColumnType::DATE;
}

void ColumnBuilder::SetTime(TimeUnit unit) {
  type_ = ColumnType::TIME;
  meta_time_.unit = unit;
}

FBB& ColumnBuilder::fbb() {
  return *fbb_;
}

std::unique_ptr<ColumnBuilder> TableBuilder::AddColumn(const std::string& name) {
  return std::unique_ptr<ColumnBuilder>(new ColumnBuilder(this, name));
}

// ----------------------------------------------------------------------
// reader.cc

class TableReader::TableReaderImpl {
 public:
  TableReaderImpl() {}

  Status Open(const std::shared_ptr<io::RandomAccessFile>& source) {
    source_ = source;

    int magic_size = static_cast<int>(strlen(kFeatherMagicBytes));
    int footer_size = magic_size + static_cast<int>(sizeof(uint32_t));

    // Pathological issue where the file is smaller than
    int64_t size = 0;
    RETURN_NOT_OK(source->GetSize(&size));
    if (size < magic_size + footer_size) {
      return Status::Invalid("File is too small to be a well-formed file");
    }

    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(source->Read(magic_size, &buffer));

    if (memcmp(buffer->data(), kFeatherMagicBytes, magic_size)) {
      return Status::Invalid("Not a feather file");
    }

    // Now get the footer and verify
    RETURN_NOT_OK(source->ReadAt(size - footer_size, footer_size, &buffer));

    if (memcmp(buffer->data() + sizeof(uint32_t), kFeatherMagicBytes, magic_size)) {
      return Status::Invalid("Feather file footer incomplete");
    }

    uint32_t metadata_length = *reinterpret_cast<const uint32_t*>(buffer->data());
    if (size < magic_size + footer_size + metadata_length) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }
    RETURN_NOT_OK(
        source->ReadAt(size - footer_size - metadata_length, metadata_length, &buffer));

    metadata_.reset(new TableMetadata());
    return metadata_->Open(buffer);
  }

  Status GetDataType(const fbs::PrimitiveArray* values, fbs::TypeMetadata metadata_type,
      const void* metadata, std::shared_ptr<DataType>* out) {
#define PRIMITIVE_CASE(CAP_TYPE, FACTORY_FUNC) \
  case fbs::Type_##CAP_TYPE:                   \
    *out = FACTORY_FUNC();                     \
    break;

    switch (metadata_type) {
      case fbs::TypeMetadata_CategoryMetadata: {
        auto meta = static_cast<const fbs::CategoryMetadata*>(metadata);

        std::shared_ptr<DataType> index_type;
        RETURN_NOT_OK(GetDataType(values, fbs::TypeMetadata_NONE, nullptr, &index_type));

        std::shared_ptr<Array> levels;
        RETURN_NOT_OK(
            LoadValues(meta->levels(), fbs::TypeMetadata_NONE, nullptr, &levels));

        *out = std::make_shared<DictionaryType>(index_type, levels, meta->ordered());
        break;
      }
      case fbs::TypeMetadata_TimestampMetadata: {
        auto meta = static_cast<const fbs::TimestampMetadata*>(metadata);
        TimeUnit unit = FromFlatbufferEnum(meta->unit());
        std::string tz;
        // flatbuffer non-null
        if (meta->timezone() != 0) {
          tz = meta->timezone()->str();
        } else {
          tz = "";
        }
        *out = timestamp(unit, tz);
      } break;
      case fbs::TypeMetadata_DateMetadata:
        *out = date32();
        break;
      case fbs::TypeMetadata_TimeMetadata: {
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
          default:
            return Status::Invalid("Unrecognized type");
        }
        break;
    }

#undef PRIMITIVE_CASE

    return Status::OK();
  }

  // Retrieve a primitive array from the data source
  //
  // @returns: a Buffer instance, the precise type will depend on the kind of
  // input data source (which may or may not have memory-map like semantics)
  Status LoadValues(const fbs::PrimitiveArray* meta, fbs::TypeMetadata metadata_type,
      const void* metadata, std::shared_ptr<Array>* out) {
    std::shared_ptr<DataType> type;
    RETURN_NOT_OK(GetDataType(meta, metadata_type, metadata, &type));

    std::vector<std::shared_ptr<Buffer>> buffers;

    // Buffer data from the source (may or may not perform a copy depending on
    // input source)
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(source_->ReadAt(meta->offset(), meta->total_bytes(), &buffer));

    int64_t offset = 0;

    // If there are nulls, the null bitmask is first
    if (meta->null_count() > 0) {
      int64_t null_bitmap_size = GetOutputLength(BitUtil::BytesForBits(meta->length()));
      buffers.push_back(SliceBuffer(buffer, offset, null_bitmap_size));
      offset += null_bitmap_size;
    } else {
      buffers.push_back(nullptr);
    }

    if (is_binary_like(type->type)) {
      int64_t offsets_size = GetOutputLength((meta->length() + 1) * sizeof(int32_t));
      buffers.push_back(SliceBuffer(buffer, offset, offsets_size));
      offset += offsets_size;
    }

    buffers.push_back(SliceBuffer(buffer, offset, buffer->size() - offset));
    return MakePrimitiveArray(type, buffers, meta->length(), meta->null_count(), 0, out);
  }

  bool HasDescription() const { return metadata_->HasDescription(); }

  std::string GetDescription() const { return metadata_->GetDescription(); }

  int version() const { return metadata_->version(); }
  int64_t num_rows() const { return metadata_->num_rows(); }
  int64_t num_columns() const { return metadata_->num_columns(); }

  std::string GetColumnName(int i) const {
    const fbs::Column* col_meta = metadata_->column(i);
    return col_meta->name()->str();
  }

  Status GetColumn(int i, std::shared_ptr<Column>* out) {
    const fbs::Column* col_meta = metadata_->column(i);

    // auto user_meta = column->user_metadata();
    // if (user_meta->size() > 0) { user_metadata_ = user_meta->str(); }

    std::shared_ptr<Array> values;
    RETURN_NOT_OK(LoadValues(
        col_meta->values(), col_meta->metadata_type(), col_meta->metadata(), &values));
    out->reset(new Column(col_meta->name()->str(), values));
    return Status::OK();
  }

 private:
  std::shared_ptr<io::RandomAccessFile> source_;
  std::unique_ptr<TableMetadata> metadata_;

  std::shared_ptr<Schema> schema_;
};

// ----------------------------------------------------------------------
// TableReader public API

TableReader::TableReader() {
  impl_.reset(new TableReaderImpl());
}

TableReader::~TableReader() {}

Status TableReader::Open(const std::shared_ptr<io::RandomAccessFile>& source) {
  return impl_->Open(source);
}

Status TableReader::OpenFile(
    const std::string& abspath, std::unique_ptr<TableReader>* out) {
  std::shared_ptr<io::MemoryMappedFile> file;
  RETURN_NOT_OK(io::MemoryMappedFile::Open(abspath, io::FileMode::READ, &file));
  out->reset(new TableReader());
  return (*out)->Open(file);
}

bool TableReader::HasDescription() const {
  return impl_->HasDescription();
}

std::string TableReader::GetDescription() const {
  return impl_->GetDescription();
}

int TableReader::version() const {
  return impl_->version();
}

int64_t TableReader::num_rows() const {
  return impl_->num_rows();
}

int64_t TableReader::num_columns() const {
  return impl_->num_columns();
}

std::string TableReader::GetColumnName(int i) const {
  return impl_->GetColumnName(i);
}

Status TableReader::GetColumn(int i, std::shared_ptr<Column>* out) {
  return impl_->GetColumn(i, out);
}

// ----------------------------------------------------------------------
// writer.cc

fbs::Type ToFlatbufferType(Type::type type) {
  switch (type) {
    case Type::BOOL:
      return fbs::Type_BOOL;
    case Type::INT8:
      return fbs::Type_INT8;
    case Type::INT16:
      return fbs::Type_INT16;
    case Type::INT32:
      return fbs::Type_INT32;
    case Type::INT64:
      return fbs::Type_INT64;
    case Type::UINT8:
      return fbs::Type_UINT8;
    case Type::UINT16:
      return fbs::Type_UINT16;
    case Type::UINT32:
      return fbs::Type_UINT32;
    case Type::UINT64:
      return fbs::Type_UINT64;
    case Type::FLOAT:
      return fbs::Type_FLOAT;
    case Type::DOUBLE:
      return fbs::Type_DOUBLE;
    case Type::STRING:
      return fbs::Type_UTF8;
    case Type::BINARY:
      return fbs::Type_BINARY;
    case Type::DATE32:
      return fbs::Type_DATE;
    case Type::TIMESTAMP:
      return fbs::Type_TIMESTAMP;
    case Type::TIME32:
      return fbs::Type_TIME;
    case Type::TIME64:
      return fbs::Type_TIME;
    case Type::DICTIONARY:
      return fbs::Type_CATEGORY;
    default:
      break;
  }
  // prevent compiler warning
  return fbs::Type_MIN;
}

class TableWriter::TableWriterImpl : public ArrayVisitor {
 public:
  TableWriterImpl() : initialized_stream_(false), metadata_(0) {}

  Status Open(const std::shared_ptr<io::OutputStream>& stream) {
    stream_ = stream;
    return Status::OK();
  }

  void SetDescription(const std::string& desc) { metadata_.SetDescription(desc); }

  void SetNumRows(int64_t num_rows) { metadata_.SetNumRows(num_rows); }

  Status Finalize() {
    RETURN_NOT_OK(CheckStarted());
    metadata_.Finish();

    auto buffer = metadata_.GetBuffer();

    // Writer metadata
    int64_t bytes_written;
    RETURN_NOT_OK(
        WritePadded(stream_.get(), buffer->data(), buffer->size(), &bytes_written));
    uint32_t buffer_size = static_cast<uint32_t>(bytes_written);

    // Footer: metadata length, magic bytes
    RETURN_NOT_OK(
        stream_->Write(reinterpret_cast<const uint8_t*>(&buffer_size), sizeof(uint32_t)));
    RETURN_NOT_OK(stream_->Write(reinterpret_cast<const uint8_t*>(kFeatherMagicBytes),
        strlen(kFeatherMagicBytes)));
    return stream_->Close();
  }

  Status LoadArrayMetadata(const Array& values, ArrayMetadata* meta) {
    if (!(is_primitive(values.type_enum()) || is_binary_like(values.type_enum()))) {
      std::stringstream ss;
      ss << "Array is not primitive type: " << values.type()->ToString();
      return Status::Invalid(ss.str());
    }

    meta->type = ToFlatbufferType(values.type_enum());

    RETURN_NOT_OK(stream_->Tell(&meta->offset));

    meta->length = values.length();
    meta->null_count = values.null_count();
    meta->total_bytes = 0;

    return Status::OK();
  }

  Status WriteArray(const Array& values, ArrayMetadata* meta) {
    RETURN_NOT_OK(CheckStarted());
    RETURN_NOT_OK(LoadArrayMetadata(values, meta));

    int64_t bytes_written;

    // Write the null bitmask
    if (values.null_count() > 0) {
      // We assume there is one bit for each value in values.nulls, aligned on a
      // byte boundary, and we write this much data into the stream
      RETURN_NOT_OK(WritePadded(stream_.get(), values.null_bitmap()->data(),
          values.null_bitmap()->size(), &bytes_written));
      meta->total_bytes += bytes_written;
    }

    int64_t values_bytes = 0;

    const uint8_t* values_buffer = nullptr;

    if (is_binary_like(values.type_enum())) {
      const auto& bin_values = static_cast<const BinaryArray&>(values);

      int64_t offset_bytes = sizeof(int32_t) * (values.length() + 1);

      values_bytes = bin_values.raw_value_offsets()[values.length()];

      // Write the variable-length offsets
      RETURN_NOT_OK(WritePadded(stream_.get(),
          reinterpret_cast<const uint8_t*>(bin_values.raw_value_offsets()), offset_bytes,
          &bytes_written))
      meta->total_bytes += bytes_written;

      if (bin_values.data()) { values_buffer = bin_values.data()->data(); }
    } else {
      const auto& prim_values = static_cast<const PrimitiveArray&>(values);
      const auto& fw_type = static_cast<const FixedWidthType&>(*values.type());

      if (values.type_enum() == Type::BOOL) {
        // Booleans are bit-packed
        values_bytes = BitUtil::BytesForBits(values.length());
      } else {
        values_bytes = values.length() * fw_type.bit_width() / 8;
      }

      if (prim_values.data()) { values_buffer = prim_values.data()->data(); }
    }
    RETURN_NOT_OK(
        WritePadded(stream_.get(), values_buffer, values_bytes, &bytes_written));
    meta->total_bytes += bytes_written;

    return Status::OK();
  }

  Status WritePrimitiveValues(const Array& values) {
    // Prepare metadata payload
    ArrayMetadata meta;
    RETURN_NOT_OK(WriteArray(values, &meta));
    current_column_->SetValues(meta);
    return Status::OK();
  }

#define VISIT_PRIMITIVE(TYPE) \
  Status Visit(const TYPE& values) override { return WritePrimitiveValues(values); }

  VISIT_PRIMITIVE(BooleanArray);
  VISIT_PRIMITIVE(Int8Array);
  VISIT_PRIMITIVE(Int16Array);
  VISIT_PRIMITIVE(Int32Array);
  VISIT_PRIMITIVE(Int64Array);
  VISIT_PRIMITIVE(UInt8Array);
  VISIT_PRIMITIVE(UInt16Array);
  VISIT_PRIMITIVE(UInt32Array);
  VISIT_PRIMITIVE(UInt64Array);
  VISIT_PRIMITIVE(FloatArray);
  VISIT_PRIMITIVE(DoubleArray);
  VISIT_PRIMITIVE(BinaryArray);
  VISIT_PRIMITIVE(StringArray);

#undef VISIT_PRIMITIVE

  Status Visit(const DictionaryArray& values) override {
    const auto& dict_type = static_cast<const DictionaryType&>(*values.type());

    if (!is_integer(values.indices()->type_enum())) {
      return Status::Invalid("Category values must be integers");
    }

    RETURN_NOT_OK(WritePrimitiveValues(*values.indices()));

    ArrayMetadata levels_meta;
    RETURN_NOT_OK(WriteArray(*dict_type.dictionary(), &levels_meta));
    current_column_->SetCategory(levels_meta, dict_type.ordered());
    return Status::OK();
  }

  Status Visit(const TimestampArray& values) override {
    RETURN_NOT_OK(WritePrimitiveValues(values));
    const auto& ts_type = static_cast<const TimestampType&>(*values.type());
    current_column_->SetTimestamp(ts_type.unit, ts_type.timezone);
    return Status::OK();
  }

  Status Visit(const Date32Array& values) override {
    RETURN_NOT_OK(WritePrimitiveValues(values));
    current_column_->SetDate();
    return Status::OK();
  }

  Status Visit(const Time32Array& values) override {
    RETURN_NOT_OK(WritePrimitiveValues(values));
    auto unit = static_cast<const Time32Type&>(*values.type()).unit;
    current_column_->SetTime(unit);
    return Status::OK();
  }

  Status Visit(const Time64Array& values) override {
    return Status::NotImplemented("time64");
  }

  Status Append(const std::string& name, const Array& values) {
    current_column_ = metadata_.AddColumn(name);
    RETURN_NOT_OK(values.Accept(this));
    current_column_->Finish();
    return Status::OK();
  }

 private:
  Status CheckStarted() {
    if (!initialized_stream_) {
      int64_t bytes_written_unused;
      RETURN_NOT_OK(
          WritePadded(stream_.get(), reinterpret_cast<const uint8_t*>(kFeatherMagicBytes),
              strlen(kFeatherMagicBytes), &bytes_written_unused));
      initialized_stream_ = true;
    }
    return Status::OK();
  }

  std::shared_ptr<io::OutputStream> stream_;

  bool initialized_stream_;
  TableBuilder metadata_;

  std::unique_ptr<ColumnBuilder> current_column_;

  Status AppendPrimitive(const PrimitiveArray& values, ArrayMetadata* out);
};

TableWriter::TableWriter() {
  impl_.reset(new TableWriterImpl());
}

TableWriter::~TableWriter() {}

Status TableWriter::Open(
    const std::shared_ptr<io::OutputStream>& stream, std::unique_ptr<TableWriter>* out) {
  out->reset(new TableWriter());
  return (*out)->impl_->Open(stream);
}

Status TableWriter::OpenFile(
    const std::string& abspath, std::unique_ptr<TableWriter>* out) {
  std::shared_ptr<io::FileOutputStream> file;
  RETURN_NOT_OK(io::FileOutputStream::Open(abspath, &file));
  out->reset(new TableWriter());
  return (*out)->impl_->Open(file);
}

void TableWriter::SetDescription(const std::string& desc) {
  impl_->SetDescription(desc);
}

void TableWriter::SetNumRows(int64_t num_rows) {
  impl_->SetNumRows(num_rows);
}

Status TableWriter::Append(const std::string& name, const Array& values) {
  return impl_->Append(name, values);
}

Status TableWriter::Finalize() {
  return impl_->Finalize();
}

}  // namespace feather
}  // namespace ipc
}  // namespace arrow
