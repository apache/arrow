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

#include <algorithm>
#include <cstdint>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table_builder.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#include "orc/OrcFile.hh"

// alias to not interfere with nested orc namespace
namespace liborc = orc;

namespace arrow {
namespace adapters {
namespace orc {

#define ORC_THROW_NOT_OK(s)                   \
  do {                                        \
    Status _s = (s);                          \
    if (!_s.ok()) {                           \
      std::stringstream ss;                   \
      ss << "Arrow error: " << _s.ToString(); \
      throw liborc::ParseError(ss.str());     \
    }                                         \
  } while (0)

class ArrowInputFile : public liborc::InputStream {
 public:
  explicit ArrowInputFile(const std::shared_ptr<io::ReadableFileInterface>& file)
      : file_(file) {}

  uint64_t getLength() const override {
    int64_t size;
    ORC_THROW_NOT_OK(file_->GetSize(&size));
    return static_cast<uint64_t>(size);
  }

  uint64_t getNaturalReadSize() const override { return 128 * 1024; }

  void read(void* buf, uint64_t length, uint64_t offset) override {
    int64_t bytes_read;

    ORC_THROW_NOT_OK(file_->ReadAt(offset, length, &bytes_read, buf));

    if (static_cast<uint64_t>(bytes_read) != length) {
      throw liborc::ParseError("Short read from arrow input file");
    }
  }

  const std::string& getName() const override {
    static const std::string filename("ArrowInputFile");
    return filename;
  }

 private:
  std::shared_ptr<io::ReadableFileInterface> file_;
};

struct StripeInformation {
  uint64_t offset;
  uint64_t length;
  uint64_t num_rows;
};

Status get_dtype(const liborc::Type* type, std::shared_ptr<DataType>* out) {
  if (type == nullptr) {
    *out = null();
    return Status::OK();
  }
  liborc::TypeKind kind = type->getKind();
  switch (kind) {
    case liborc::BOOLEAN:
      *out = boolean();
      break;
    case liborc::BYTE:
      *out = int8();
      break;
    case liborc::SHORT:
      *out = int16();
      break;
    case liborc::INT:
      *out = int32();
      break;
    case liborc::LONG:
      *out = int64();
      break;
    case liborc::FLOAT:
      *out = float32();
      break;
    case liborc::DOUBLE:
      *out = float64();
      break;
    case liborc::VARCHAR:
    case liborc::STRING:
      *out = utf8();
      break;
    case liborc::BINARY:
      *out = binary();
      break;
    case liborc::CHAR:
      *out = fixed_size_binary(type->getMaximumLength());
      break;
    case liborc::TIMESTAMP:
      *out = timestamp(TimeUnit::NANO);
      break;
    case liborc::DATE:
      *out = date64();
      break;
    case liborc::DECIMAL: {
      if (type->getPrecision() == 0) {
        // In HIVE 0.11/0.12 precision is set as 0, but means max precision
        *out = decimal(38, 6);
      } else {
        *out = decimal(type->getPrecision(), type->getScale());
      }
      break;
    }
    case liborc::LIST: {
      if (type->getSubtypeCount() != 1) {
        return Status::Invalid("Invalid Orc List type");
      }
      std::shared_ptr<DataType> elemtype;
      RETURN_NOT_OK(get_dtype(type->getSubtype(0), &elemtype));
      *out = list(elemtype);
      break;
    }
    case liborc::MAP: {
      if (type->getSubtypeCount() != 2) {
        return Status::Invalid("Invalid Orc Map type");
      }
      std::shared_ptr<DataType> keytype;
      std::shared_ptr<DataType> valtype;
      RETURN_NOT_OK(get_dtype(type->getSubtype(0), &keytype));
      RETURN_NOT_OK(get_dtype(type->getSubtype(1), &valtype));
      *out = list(struct_({field("key", keytype), field("value", valtype)}));
      break;
    }
    case liborc::STRUCT: {
      int size = type->getSubtypeCount();
      std::vector<std::shared_ptr<Field>> fields;
      for (int child = 0; child < size; ++child) {
        std::shared_ptr<DataType> elemtype;
        RETURN_NOT_OK(get_dtype(type->getSubtype(child), &elemtype));
        std::string name = type->getFieldName(child);
        fields.push_back(field(name, elemtype));
      }
      *out = struct_(fields);
      break;
    }
    case liborc::UNION: {
      int size = type->getSubtypeCount();
      std::vector<std::shared_ptr<Field>> fields;
      std::vector<uint8_t> type_codes;
      for (int child = 0; child < size; ++child) {
        std::shared_ptr<DataType> elemtype;
        RETURN_NOT_OK(get_dtype(type->getSubtype(child), &elemtype));
        fields.push_back(field("_union_" + std::to_string(child), elemtype));
        type_codes.push_back((uint8_t)child);
      }
      *out = union_(fields, type_codes);
      break;
    }
    default: {
      std::stringstream ss;
      ss << "Unknown Orc type kind: " << kind;
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

// The number of rows to read in a ColumnVectorBatch
constexpr int64_t kReadRowsBatch = 1000;

class ORCFileReader::Impl {
 public:
  explicit Impl(MemoryPool* pool, std::unique_ptr<liborc::Reader> reader)
      : pool_(pool), reader_(std::move(reader)) {}

  static Status Open(const std::shared_ptr<io::ReadableFileInterface>& file,
                     MemoryPool* pool, std::unique_ptr<Impl>* impl) {
    std::unique_ptr<ArrowInputFile> io_wrapper(new ArrowInputFile(file));
    liborc::ReaderOptions options;
    std::unique_ptr<liborc::Reader> liborc_reader;
    try {
      liborc_reader = createReader(std::move(io_wrapper), options);
    } catch (const liborc::ParseError& e) {
      return Status::IOError(e.what());
    }
    impl->reset(new Impl(pool, std::move(liborc_reader)));
    RETURN_NOT_OK((*impl)->Init());

    return Status::OK();
  }

  Status Init() {
    int64_t nstripes = reader_->getNumberOfStripes();
    stripes_.resize(nstripes);
    std::unique_ptr<liborc::StripeInformation> stripe;
    for (int i = 0; i < nstripes; ++i) {
      stripe = reader_->getStripe(i);
      stripes_[i] = StripeInformation(
          {stripe->getOffset(), stripe->getLength(), stripe->getNumberOfRows()});
    }
    return Status::OK();
  }

  int64_t NumberOfStripes() { return stripes_.size(); }

  int64_t NumberOfRows() { return reader_->getNumberOfRows(); }

  Status ReadSchema(std::shared_ptr<Schema>* out) {
    const liborc::Type& type = reader_->getType();
    return type_to_schema(type, out);
  }

  Status type_to_schema(const liborc::Type& type, std::shared_ptr<Schema>* out) {
    if (type.getKind() != liborc::STRUCT) {
      return Status::NotImplemented(
          "Only ORC files with a top-level struct "
          "can be handled");
    }
    int size = type.getSubtypeCount();
    std::vector<std::shared_ptr<Field>> fields;
    for (int child = 0; child < size; ++child) {
      std::shared_ptr<DataType> elemtype;
      RETURN_NOT_OK(get_dtype(type.getSubtype(child), &elemtype));
      std::string name = type.getFieldName(child);
      fields.push_back(field(name, elemtype));
    }
    std::list<std::string> keys = reader_->getMetadataKeys();
    std::shared_ptr<KeyValueMetadata> metadata;
    if (!keys.empty()) {
      metadata = std::make_shared<KeyValueMetadata>();
      for (auto it = keys.begin(); it != keys.end(); ++it) {
        metadata->Append(*it, reader_->getMetadataValue(*it));
      }
    }

    *out = std::make_shared<Schema>(fields, metadata);
    return Status::OK();
  }

  Status Read(std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    return read_batch(opts, NumberOfRows(), out);
  }

  Status Read(const std::list<uint64_t>& include_indices,
              std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    opts.includeTypes(include_indices);
    return read_batch(opts, NumberOfRows(), out);
  }

  Status ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(select_stripe(&opts, stripe));
    return read_batch(opts, stripes_[stripe].num_rows, out);
  }

  Status ReadStripe(int64_t stripe, const std::list<uint64_t>& include_indices,
                    std::shared_ptr<RecordBatch>* out) {
    liborc::RowReaderOptions opts;
    RETURN_NOT_OK(select_stripe(&opts, stripe));
    opts.includeTypes(include_indices);
    return read_batch(opts, stripes_[stripe].num_rows, out);
  }

  Status select_stripe(liborc::RowReaderOptions* opts, int64_t stripe) {
    if (stripe < 0 || stripe >= NumberOfStripes()) {
      std::stringstream ss;
      ss << "Out of bounds stripe: " << stripe;
      return Status::Invalid(ss.str());
    }
    opts->range(stripes_[stripe].offset, stripes_[stripe].length);
    return Status::OK();
  }

  Status read_batch(const liborc::RowReaderOptions& opts, int64_t nrows,
                    std::shared_ptr<RecordBatch>* out) {
    std::unique_ptr<liborc::RowReader> rowreader;
    std::unique_ptr<liborc::ColumnVectorBatch> batch;
    try {
      rowreader = reader_->createRowReader(opts);
      batch = rowreader->createRowBatch(std::min(nrows, kReadRowsBatch));
    } catch (const liborc::ParseError& e) {
      return Status::Invalid(e.what());
    }
    const liborc::Type& type = rowreader->getSelectedType();
    std::shared_ptr<Schema> schema;
    RETURN_NOT_OK(type_to_schema(type, &schema));

    std::unique_ptr<RecordBatchBuilder> builder;
    RETURN_NOT_OK(RecordBatchBuilder::Make(schema, pool_, nrows, &builder));

    // The top-level type must be a struct to read into an arrow table
    auto struct_batch = static_cast<liborc::StructVectorBatch*>(batch.get());

    int n_fields = builder->num_fields();
    while (rowreader->next(*batch)) {
      for (int i = 0; i < n_fields; i++) {
        RETURN_NOT_OK(append_batch(type.getSubtype(i), builder->GetField(i),
                                   struct_batch->fields[i], 0, batch->numElements));
      }
    }
    RETURN_NOT_OK(builder->Flush(out));
    return Status::OK();
  }

  Status append_batch(const liborc::Type* type, ArrayBuilder* builder,
                      liborc::ColumnVectorBatch* batch, int64_t offset, int64_t length) {
    if (type == nullptr) {
      return Status::OK();
    }
    liborc::TypeKind kind = type->getKind();
    switch (kind) {
      case liborc::STRUCT:
        return append_struct_batch(type, builder, batch, offset, length);
      case liborc::LIST:
        return append_list_batch(type, builder, batch, offset, length);
      case liborc::MAP:
        return append_map_batch(type, builder, batch, offset, length);
      case liborc::LONG:
        return append_numeric_batch<Int64Builder, liborc::LongVectorBatch, int64_t>(
            builder, batch, offset, length);
      case liborc::INT:
        return append_numeric_batch_cast<Int32Builder, int32_t, liborc::LongVectorBatch,
                                         int64_t>(builder, batch, offset, length);
      case liborc::SHORT:
        return append_numeric_batch_cast<Int16Builder, int16_t, liborc::LongVectorBatch,
                                         int64_t>(builder, batch, offset, length);
      case liborc::BYTE:
        return append_numeric_batch_cast<Int8Builder, int8_t, liborc::LongVectorBatch,
                                         int64_t>(builder, batch, offset, length);
      case liborc::DOUBLE:
        return append_numeric_batch<DoubleBuilder, liborc::DoubleVectorBatch, double>(
            builder, batch, offset, length);
      case liborc::FLOAT:
        return append_numeric_batch_cast<FloatBuilder, float, liborc::DoubleVectorBatch,
                                         double>(builder, batch, offset, length);
      case liborc::BOOLEAN:
        return append_bool_batch(builder, batch, offset, length);
      case liborc::VARCHAR:
      case liborc::STRING:
        return append_binary_batch<StringBuilder>(builder, batch, offset, length);
      case liborc::BINARY:
        return append_binary_batch<BinaryBuilder>(builder, batch, offset, length);
      case liborc::CHAR:
        return append_fixed_binary_batch(builder, batch, offset, length);
      case liborc::DATE:
        return append_date_batch(builder, batch, offset, length);
      case liborc::TIMESTAMP:
        return append_timestamp_batch(builder, batch, offset, length);
      case liborc::DECIMAL:
        return append_decimal_batch(type, builder, batch, offset, length);
      default:
        std::stringstream ss;
        ss << "Not implemented type kind: " << kind;
        return Status::NotImplemented(ss.str());
    }
  }

  Status append_struct_batch(const liborc::Type* type, ArrayBuilder* abuilder,
                             liborc::ColumnVectorBatch* cbatch, int64_t offset,
                             int64_t length) {
    auto builder = static_cast<StructBuilder*>(abuilder);
    auto batch = static_cast<liborc::StructVectorBatch*>(cbatch);

    const uint8_t* valid_bytes = nullptr;
    if (batch->hasNulls) {
      valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
    }
    RETURN_NOT_OK(builder->Append(length, valid_bytes));

    int n_fields = builder->num_fields();
    for (int i = 0; i < n_fields; i++) {
      RETURN_NOT_OK(append_batch(type->getSubtype(i), builder->field_builder(i),
                                 batch->fields[i], offset, length));
    }
    return Status::OK();
  }

  Status append_list_batch(const liborc::Type* type, ArrayBuilder* abuilder,
                           liborc::ColumnVectorBatch* cbatch, int64_t offset,
                           int64_t length) {
    auto builder = static_cast<ListBuilder*>(abuilder);
    auto batch = static_cast<liborc::ListVectorBatch*>(cbatch);
    liborc::ColumnVectorBatch* elements = batch->elements.get();
    const liborc::Type* elemtype = type->getSubtype(0);

    for (int i = offset; i < length + offset; i++) {
      if (!batch->hasNulls || batch->notNull[i]) {
        int64_t start = batch->offsets[i];
        int64_t end = batch->offsets[i + 1];
        RETURN_NOT_OK(builder->Append());
        RETURN_NOT_OK(append_batch(elemtype, builder->value_builder(), elements, start,
                                   end - start));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
    return Status::OK();
  }

  Status append_map_batch(const liborc::Type* type, ArrayBuilder* abuilder,
                          liborc::ColumnVectorBatch* cbatch, int64_t offset,
                          int64_t length) {
    auto list_builder = static_cast<ListBuilder*>(abuilder);
    auto struct_builder = static_cast<StructBuilder*>(list_builder->value_builder());
    auto batch = static_cast<liborc::MapVectorBatch*>(cbatch);
    liborc::ColumnVectorBatch* keys = batch->keys.get();
    liborc::ColumnVectorBatch* vals = batch->elements.get();
    const liborc::Type* keytype = type->getSubtype(0);
    const liborc::Type* valtype = type->getSubtype(1);

    for (int i = offset; i < length + offset; i++) {
      RETURN_NOT_OK(list_builder->Append());
      int64_t start = batch->offsets[i];
      int64_t list_length = batch->offsets[i + 1] - start;
      if (list_length && (!batch->hasNulls || batch->notNull[i])) {
        RETURN_NOT_OK(struct_builder->Append(list_length, nullptr));
        RETURN_NOT_OK(append_batch(keytype, struct_builder->field_builder(0), keys, start,
                                   list_length));
        RETURN_NOT_OK(append_batch(valtype, struct_builder->field_builder(1), vals, start,
                                   list_length));
      }
    }
    return Status::OK();
  }

  template <class builder_type, class batch_type, class elem_type>
  Status append_numeric_batch(ArrayBuilder* abuilder, liborc::ColumnVectorBatch* cbatch,
                              int64_t offset, int64_t length) {
    auto builder = static_cast<builder_type*>(abuilder);
    auto batch = static_cast<batch_type*>(cbatch);

    if (length == 0) {
      return Status::OK();
    }
    const uint8_t* valid_bytes = nullptr;
    if (batch->hasNulls) {
      valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
    }
    const elem_type* source = batch->data.data() + offset;
    RETURN_NOT_OK(builder->Append(source, length, valid_bytes));
    return Status::OK();
  }

  template <class builder_type, class target_type, class batch_type, class source_type>
  Status append_numeric_batch_cast(ArrayBuilder* abuilder,
                                   liborc::ColumnVectorBatch* cbatch, int64_t offset,
                                   int64_t length) {
    auto builder = static_cast<builder_type*>(abuilder);
    auto batch = static_cast<batch_type*>(cbatch);

    if (length == 0) {
      return Status::OK();
    }
    int start = builder->length();

    const uint8_t* valid_bytes = nullptr;
    if (batch->hasNulls) {
      valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
    }
    RETURN_NOT_OK(builder->AppendNulls(valid_bytes, length));

    const source_type* source = batch->data.data() + offset;
    target_type* target = reinterpret_cast<target_type*>(builder->data()->mutable_data());

    for (int i = 0; i < length; i++) {
      target[start + i] = (target_type)source[i];
    }
    return Status::OK();
  }

  Status append_bool_batch(ArrayBuilder* abuilder, liborc::ColumnVectorBatch* cbatch,
                           int64_t offset, int64_t length) {
    auto builder = static_cast<BooleanBuilder*>(abuilder);
    auto batch = static_cast<liborc::LongVectorBatch*>(cbatch);

    if (length == 0) {
      return Status::OK();
    }
    int start = builder->length();

    const uint8_t* valid_bytes = nullptr;
    if (batch->hasNulls) {
      valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
    }
    RETURN_NOT_OK(builder->AppendNulls(valid_bytes, length));

    const int64_t* source = batch->data.data() + offset;
    uint8_t* target = reinterpret_cast<uint8_t*>(builder->data()->mutable_data());

    for (int i = 0; i < length; i++) {
      if (source[i]) {
        BitUtil::SetBit(target, start + i);
      } else {
        BitUtil::ClearBit(target, start + i);
      }
    }
    return Status::OK();
  }

  Status append_timestamp_batch(ArrayBuilder* abuilder, liborc::ColumnVectorBatch* cbatch,
                                int64_t offset, int64_t length) {
    auto builder = static_cast<TimestampBuilder*>(abuilder);
    auto batch = static_cast<liborc::TimestampVectorBatch*>(cbatch);

    if (length == 0) {
      return Status::OK();
    }
    int start = builder->length();

    const uint8_t* valid_bytes = nullptr;
    if (batch->hasNulls) {
      valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
    }
    RETURN_NOT_OK(builder->AppendNulls(valid_bytes, length));

    const int64_t* seconds = batch->data.data() + offset;
    const int64_t* nanos = batch->nanoseconds.data() + offset;
    int64_t* target = reinterpret_cast<int64_t*>(builder->data()->mutable_data());

    for (int i = 0; i < length; i++) {
      target[start + i] = seconds[i] * int64_t(1e9) + nanos[i];
    }
    return Status::OK();
  }

  Status append_date_batch(ArrayBuilder* abuilder, liborc::ColumnVectorBatch* cbatch,
                           int64_t offset, int64_t length) {
    auto builder = static_cast<Date64Builder*>(abuilder);
    auto batch = static_cast<liborc::LongVectorBatch*>(cbatch);

    if (length == 0) {
      return Status::OK();
    }
    int start = builder->length();

    const uint8_t* valid_bytes = nullptr;
    if (batch->hasNulls) {
      valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
    }
    RETURN_NOT_OK(builder->AppendNulls(valid_bytes, length));

    const int64_t* source = batch->data.data() + offset;
    int64_t* target = reinterpret_cast<int64_t*>(builder->data()->mutable_data());

    for (int i = 0; i < length; i++) {
      target[start + i] = source[i] * 24 * 60 * 60 * 1000;
    }
    return Status::OK();
  }

  template <class builder_type>
  Status append_binary_batch(ArrayBuilder* abuilder, liborc::ColumnVectorBatch* cbatch,
                             int64_t offset, int64_t length) {
    auto builder = static_cast<builder_type*>(abuilder);
    auto batch = static_cast<liborc::StringVectorBatch*>(cbatch);

    for (int i = offset; i < length + offset; i++) {
      if (!batch->hasNulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(batch->data[i], batch->length[i]));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
    return Status::OK();
  }

  Status append_fixed_binary_batch(ArrayBuilder* abuilder,
                                   liborc::ColumnVectorBatch* cbatch, int64_t offset,
                                   int64_t length) {
    auto builder = static_cast<FixedSizeBinaryBuilder*>(abuilder);
    auto batch = static_cast<liborc::StringVectorBatch*>(cbatch);

    for (int i = offset; i < length + offset; i++) {
      if (!batch->hasNulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(batch->data[i]));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
    return Status::OK();
  }

  Status append_decimal_batch(const liborc::Type* type, ArrayBuilder* abuilder,
                              liborc::ColumnVectorBatch* cbatch, int64_t offset,
                              int64_t length) {
    auto builder = static_cast<Decimal128Builder*>(abuilder);

    if (type->getPrecision() == 0 || type->getPrecision() > 18) {
      auto batch = static_cast<liborc::Decimal128VectorBatch*>(cbatch);
      for (int i = offset; i < length + offset; i++) {
        if (!batch->hasNulls || batch->notNull[i]) {
          const Decimal128& x =
              Decimal128(batch->values[i].getHighBits(), batch->values[i].getLowBits());
          RETURN_NOT_OK(builder->Append(x));
        } else {
          RETURN_NOT_OK(builder->AppendNull());
        }
      }
    } else {
      auto batch = static_cast<liborc::Decimal64VectorBatch*>(cbatch);
      for (int i = offset; i < length + offset; i++) {
        if (!batch->hasNulls || batch->notNull[i]) {
          const Decimal128& x = Decimal128(batch->values[i]);
          RETURN_NOT_OK(builder->Append(x));
        } else {
          RETURN_NOT_OK(builder->AppendNull());
        }
      }
    }
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
  std::unique_ptr<liborc::Reader> reader_;
  std::vector<StripeInformation> stripes_;
};

ORCFileReader::ORCFileReader(std::unique_ptr<ORCFileReader::Impl> impl)
    : impl_(std::move(impl)) {}

ORCFileReader::~ORCFileReader() {}

Status ORCFileReader::Open(const std::shared_ptr<io::ReadableFileInterface>& file,
                           MemoryPool* pool, std::unique_ptr<ORCFileReader>* reader) {
  std::unique_ptr<ORCFileReader::Impl> impl;
  Status status = ORCFileReader::Impl::Open(file, pool, &impl);
  RETURN_NOT_OK(status);
  reader->reset(new ORCFileReader(std::move(impl)));
  return status;
}

Status ORCFileReader::ReadSchema(std::shared_ptr<Schema>* out) {
  return impl_->ReadSchema(out);
}

Status ORCFileReader::Read(std::shared_ptr<RecordBatch>* out) { return impl_->Read(out); }

Status ORCFileReader::Read(const std::list<uint64_t>& include_indices,
                           std::shared_ptr<RecordBatch>* out) {
  return impl_->Read(include_indices, out);
}

Status ORCFileReader::ReadStripe(int64_t stripe, std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadStripe(stripe, out);
}

Status ORCFileReader::ReadStripe(int64_t stripe,
                                 const std::list<uint64_t>& include_indices,
                                 std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadStripe(stripe, include_indices, out);
}

int64_t ORCFileReader::NumberOfStripes() { return impl_->NumberOfStripes(); }

int64_t ORCFileReader::NumberOfRows() { return impl_->NumberOfRows(); }

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
