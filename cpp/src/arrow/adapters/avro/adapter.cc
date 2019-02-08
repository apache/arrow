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
#include <memory>
#include <string>
#include <sstream>
#include <arrow/util/logging.h>

#include "arrow/adapters/avro/adapter.h"

#include "arrow/builder.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/api.h"
#include "arrow/io/interfaces.h"

#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#include "avro/Schema.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"
#include "avro/Stream.hh"
#include "avro/Types.hh"


namespace arrow {
namespace adapters {

#define RETURN_AVRO_ERROR(res)           \
  do {                                   \
    int _res = (res);                    \
    if (res != 0) {                      \
        return Status::IOError(avro_strerror()); \
    } \
  } while (false) \


class AvroArrowReader::AvroArrowReaderImpl {
public:
  Status AvroFileWriterSchemaToArrowSchema(avro::DataFileReader<avro::GenericDatum> *file_reader,
                                           std::shared_ptr<Schema> *out) {

    auto s = file_reader->dataSchema();
    // make array builder
    auto root = s.root();
    *out = avro_to_arrow(root);
    return Status::OK();
  }

  std::shared_ptr<Schema> avro_to_arrow(avro::NodePtr &schema) {
    // TODO: Make this Node thing work
    auto size = schema->leaves();
    auto fields = std::vector<std::shared_ptr<Field>>();
    for (auto idx = 0; idx < size; idx++) {
      auto child = schema->leafAt(idx);
      auto elem_type = get_arrow_type(child);
      auto name = child->name().fullname();
      auto f = field(name, elem_type);
      fields.insert(fields.end(), f);
    }
    return std::make_shared<Schema>(fields, nullptr);;
  }

  std::shared_ptr<DataType> get_arrow_type(avro::NodePtr &schema) {
    auto avro_type = schema->type();
    switch (avro_type) {
      case avro::AVRO_BOOL:
        return boolean();
      case avro::AVRO_INT:
        return int32();
      case avro::AVRO_LONG:
        return int64();
      case avro::AVRO_FLOAT:
        return float32();
      case avro::AVRO_DOUBLE:
        return float64();
      case avro::AVRO_STRING:
        return utf8();
      case avro::AVRO_BYTES:
        return binary();
      case avro::AVRO_FIXED: {
        auto length = schema->fixedSize();
        return fixed_size_binary(length);
      }
      case avro::AVRO_ARRAY: {
        auto e = schema->leafAt(0);
        auto elem_type = get_arrow_type(e);
        return list(elem_type);
      }

      case avro::AVRO_MAP: {
        auto vt = schema->leafAt(0);
        auto value_type = get_arrow_type(vt);
        auto fields = {field("key", utf8()), field("value", value_type)};
        return list(struct_(fields));
      }

      case avro::AVRO_UNION: {
        auto size = schema->leaves();
        auto fields = std::vector<std::shared_ptr<Field>>();
        std::vector<uint8_t> type_codes;

        for (auto idx = 0; idx < size; idx++) {
          auto child = schema->leafAt(idx);
          auto f = field("_union_" + std::to_string(idx),
                         get_arrow_type(child));
          fields.push_back(f);
          type_codes.push_back((uint8_t) idx);
        };
        return union_(fields, type_codes);
      }

      case avro::AVRO_RECORD: {
        auto size = schema->leaves();
        auto fields = std::vector<std::shared_ptr<Field>>();
        for (auto idx = 0; idx < size; idx++) {
          auto child = schema->leafAt(idx);
          auto elem_type = get_arrow_type(child);
          auto name = child->name().fullname();
          auto f = field(name, elem_type);
          fields.insert(fields.end(), f);
        }
        return struct_(fields);
      }
      case avro::AVRO_NULL:return null();
      case avro::AVRO_ENUM: break;
      case avro::AVRO_SYMBOLIC: break;
      case avro::AVRO_UNKNOWN: break;
    }
    // TODO: unhandled case ??
    return null();
  }

  Status generic_read(avro::GenericDatum &val, ArrayBuilder *builder) {
    // Generic avro type read dispatcher. Dispatches to the various specializations by AVRO_TYPE.
    // This is used by the various readers for complex types"""

    auto avro_type =  val.type();

    switch (avro_type) {
      case avro::AVRO_BOOL:
        return this->read_bool(val, builder);
      case avro::AVRO_INT:
        return this->read_int32(val, builder);
      case avro::AVRO_LONG:
        return this->read_int64(val, builder);
      case avro::AVRO_FLOAT:
        return this->read_float32(val, builder);
      case avro::AVRO_DOUBLE:
        return this->read_float64(val, builder);
      case avro::AVRO_STRING:
        return this->read_string(val, builder);
      case avro::AVRO_BYTES:
        return this->read_binary(val, builder);
      case avro::AVRO_FIXED:
        return this->read_fixed(val, builder);
      case avro::AVRO_MAP:
        return this->read_map(val, builder);
      case avro::AVRO_RECORD:
        return this->read_record(val, builder);
      case avro::AVRO_ARRAY:
        return this->read_array(val, builder);
      default:
        std::stringstream ss;
        ss << "Unhandled avro type: " << avro_type;
        return Status::NotImplemented(ss.str());
    }
  }

  Status read_string(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto tval = val.value<std::string>();
    return static_cast<StringBuilder *>(builder)->Append(tval);
  }

  Status read_binary(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto value = val.value<std::vector<uint8_t>>();
    auto start_ptr = &value[0];
    return static_cast<BinaryBuilder *>(builder)->Append(start_ptr, value.size());
  };

  Status read_fixed(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto value = val.value<avro::GenericFixed>();
    auto start_ptr = &value.value()[0];
    return static_cast<FixedSizeBinaryBuilder *>(builder)->Append(start_ptr);
  }

  Status read_int32(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto out = val.value<int32_t>();
    return static_cast<Int32Builder *>(builder)->Append(out);
  }

  Status read_int64(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto out = val.value<int64_t>();
    return static_cast<Int64Builder *>(builder)->Append(out);
  }

  Status read_float64(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto out = val.value<double>();
    return static_cast<DoubleBuilder *>(builder)->Append(out);
  }

  Status read_float32(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto out = val.value<float>();
    return static_cast<FloatBuilder *>(builder)->Append(out);
  }

  Status read_bool(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto out = val.value<bool>();
    return static_cast<BooleanBuilder *>(builder)->Append(out);
  }

  Status read_array(avro::GenericDatum& val, ArrayBuilder *builder) {
    size_t i;
    auto v = val.value<avro::GenericArray>();
    std::vector<avro::GenericDatum> arr = v.value();
    // ArrayBuilder *child_builder;//

    RETURN_NOT_OK(static_cast<ListBuilder *>(builder)->Append(true));
    auto child_builder = static_cast<ListBuilder *>(builder)->value_builder();
    for (i = 0; i < arr.size(); i++) {
      RETURN_NOT_OK(generic_read(arr[i], child_builder));
    }
    return Status::OK();
  };

  Status read_map(avro::GenericDatum& val, ArrayBuilder *builder) {
    size_t num_values, i;
    auto listB = static_cast<ListBuilder *>(builder);
    auto structB = static_cast<StructBuilder *>(listB->child(0));
    auto keyBuilder = static_cast<StringBuilder *>(structB->child(0));
    auto valueBuilder = structB->child(1);

    auto v = val.value<avro::GenericMap>();
    std::vector<std::pair<std::string, avro::GenericDatum> > arr = v.value();

    num_values = arr.size();

    RETURN_NOT_OK(listB->Append(true));
    for (i = 0; i < num_values; i++) {
      RETURN_NOT_OK(structB->Append(true));
      RETURN_NOT_OK(keyBuilder->Append(arr[i].first));
      RETURN_NOT_OK(generic_read(arr[i].second, valueBuilder));
    }
    return Status::OK();
  }

  Status read_record(avro::GenericDatum& val, ArrayBuilder *builder) {
    auto value = val.value<avro::GenericRecord>();

    //avro_value_t child;
    ArrayBuilder *child_builder;
    StructBuilder *typed_builder;
    Status result;

    int container_length = builder->num_children();
    typed_builder = static_cast<StructBuilder *>(builder);
    RETURN_NOT_OK(typed_builder->Append());

    for (auto i = 0; i < container_length; i++) {
      auto child = value.fieldAt(i);
      child_builder = typed_builder->child(i);
      RETURN_NOT_OK(generic_read(child, child_builder));
    }
    return Status::OK();
  }

  Status read_record(avro::GenericDatum& val, RecordBatchBuilder *builder) {
    auto value = val.value<avro::GenericRecord>();

    int container_length = builder->num_fields();
    for (auto i = 0; i < container_length; i++) {
      auto child = value.fieldAt(i);
      RETURN_NOT_OK(generic_read(child, builder->GetField(i)));
    }
    return Status::OK();
  }

  Status ReadFromAvroFile(
          MemoryPool* pool,
          avro::DataFileReader<avro::GenericDatum> *file_reader,
                                           std::shared_ptr<RecordBatch>* out) {

    Status status;
    int rval = 0;
    std::shared_ptr<Schema> datatype;
    RETURN_NOT_OK(AvroFileWriterSchemaToArrowSchema(file_reader, &datatype));
    std::unique_ptr<RecordBatchBuilder> builder;
    RecordBatchBuilder::Make(datatype, pool, &builder);

    avro::GenericDatum datum = avro::GenericDatum(file_reader->dataSchema());
    while (file_reader->read(datum)) {
      RETURN_NOT_OK(read_record(datum, std::move(builder).get()));
    }

    return builder->Flush(out);
  }
};

// Public API

AvroArrowReader::AvroArrowReader() {
  impl_.reset(new AvroArrowReaderImpl());
  pool_ = default_memory_pool();
}

Status AvroArrowReader::ReadFromFileName(std::string filename, std::shared_ptr<RecordBatch>* out) {
  const char* filename_c = filename.c_str();
  auto reader = new avro::DataFileReader<avro::GenericDatum>(filename_c);
  return this->impl_->ReadFromAvroFile(pool_, reader, out);
}

/**
 * Adadpted from avrocpp
 */
class AvroArrowInInputStream : public avro::InputStream {
    const size_t bufferSize_;
    uint8_t* const buffer_;
    const std::unique_ptr<arrow::io::Readable> in_;
    size_t byteCount_;
    uint8_t* next_;
    int64_t available_;

    bool next(const uint8_t** data, size_t *size) {
      if (available_ == 0 && ! fill()) {
        return false;
      }
      *data = next_;
      *size = available_;
      next_ += available_;
      byteCount_ += available_;
      available_ = 0;
      return true;
    }

    void backup(size_t len) {
      next_ -= len;
      available_ += len;
      byteCount_ -= len;
    }

    void skip(size_t len) {
      while (len > 0) {
        if (available_ == 0) {
          in_->Read(len, nullptr);
          byteCount_ += len;
          return;
        }
        size_t n = std::min(available_, static_cast<int64_t>(len));
        available_ -= n;
        next_ += n;
        len -= n;
        byteCount_ += n;
      }
    }

    size_t byteCount() const { return byteCount_; }

    bool fill() {
      int64_t n = 0;
      auto status = in_->Read(bufferSize_, &n, buffer_);
      if (status.ok()) {
        next_ = buffer_;
        available_ = n;
        return true;
      }
      return false;
    }


public:
    AvroArrowInInputStream(std::unique_ptr<arrow::io::Readable>& in, size_t bufferSize) :
            bufferSize_(bufferSize),
            buffer_(new uint8_t[bufferSize]),
            in_(std::move(in)),
            byteCount_(0),
            next_(buffer_),
            available_(0) { }

    ~AvroArrowInInputStream() {
      delete[] buffer_;
    }
};

Status AvroArrowReader::ReadFromIO(arrow::io::Readable, std::shared_ptr<RecordBatch>* out) {
  return Status::NotImplemented("Pending...");
};

//  const char* filename_c = filename.c_str();
//  std::auto_ptr<avro::DataFileReaderBase> reader_base;
//  auto reader = avro::DataFileReader<avro::GenericDatum>(reader_base);
//
////
////  avro_file_reader_t reader = NULL;
////  avro_file_reader(filename.c_str(), &reader);
//  return this->impl_->ReadFromAvroFile(&reader, out);
//
//
//
//  auto reader = avro_reader_memory(buffer, num_bytes);
//  // TODO set the correct schema here.
//  avro_schema_t reader_schema = nullptr;
//  avro_schema_t projection_schema = reader_schema;
//  auto value_reader_iface = avro_generic_class_from_schema(reader_schema);
//
//  avro_value_t avro_val;
//  avro_value_t resolved_avro_val;
//  avro_generic_value_new(value_reader_iface, &avro_val);
//
//  auto resolved_reader_iface = avro_resolved_reader_new(reader_schema, projection_schema);
//  avro_resolved_reader_new_value(resolved_reader_iface, &resolved_avro_val);
//  avro_resolved_reader_set_source(&resolved_avro_val, &avro_val);
//
//  auto datatype = impl_->avro_to_arrow(reader_schema);
//  std::unique_ptr<RecordBatchBuilder> builder;
//  RecordBatchBuilder::Make(datatype, this->pool_, &builder);
//
//  int rval;
//  while ((rval = avro_value_read(reader, &resolved_avro_val)) == 0) {
//    RETURN_AVRO_ERROR(rval);
//    RETURN_NOT_OK(impl_->read_record(resolved_avro_val, std::move(builder).get()));
//    avro_value_reset(&resolved_avro_val);
//  }
//
//  avro_value_decref(&avro_val);
//  avro_value_iface_decref(resolved_reader_iface);
//  avro_value_iface_decref(value_reader_iface);
//  avro_reader_free(reader);
//
//  return builder->Flush(out);

//Status AvroArrowReader::GenerateAvroSchema(avro_file_reader_t *file_reader,
//                                           std::shared_ptr<Schema> *out) {
//  return impl_->AvroFileWriterSchemaToArrowSchema(file_reader, out);
//}




}
}
