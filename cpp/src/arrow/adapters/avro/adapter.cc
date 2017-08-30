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
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#include "avro.h"

namespace arrow {
namespace adapters {
namespace avro {

class AvroArrowReader::AvroArrowReaderImpl {
public:
  Status GenerateAvroSchema(avro_file_reader_t *file_reader,
                            std::shared_ptr<DataType> *out) {
    auto wschema = avro_file_reader_get_writer_schema(*file_reader);
    std::shared_ptr<DataType> arrow_schema = get_arrow_type(wschema);
    // make array builder
    out = &arrow_schema;
    return Status::OK();
  }

  std::shared_ptr<DataType> get_arrow_type(avro_schema_t schema) {
    avro_schema_t child;
    size_t size, idx;
    int64_t length;
    const char *name;

    auto avro_type = schema->type;
    switch (avro_type) {
      case AVRO_BOOLEAN:
        return boolean();
      case AVRO_INT32:
        return int32();
      case AVRO_INT64:
        return int64();
      case AVRO_FLOAT:
        return float32();
      case AVRO_DOUBLE:
        return float64();
      case AVRO_STRING:
        return utf8();
      case AVRO_BYTES:
        return binary();
      case AVRO_FIXED: {
        length = avro_schema_fixed_size(schema);
        return fixed_size_binary(length);
      }
      case AVRO_ARRAY: {
        auto elem_type = get_arrow_type(avro_schema_array_items(schema));
        return list(elem_type);
      }

      case AVRO_MAP: {
        auto value_type = get_arrow_type(avro_schema_map_values(schema));
        auto fields = {field("key", utf8()), field("value", value_type)};
        return list(struct_(fields));
      }

      case AVRO_UNION: {
        size = avro_schema_union_size(schema);
        auto fields = std::vector<std::shared_ptr<Field>>();
        std::vector<uint8_t> type_codes;

        for (idx = 0; idx < size; idx++) {
          child = avro_schema_union_branch(schema, idx);
          auto f = field("_union_" + std::to_string(idx),
                         get_arrow_type(child));
          fields.push_back(f);
          type_codes.push_back((uint8_t) idx);
        };
        return union_(fields, type_codes);
      }

      case AVRO_RECORD: {
        size = avro_schema_record_size(schema);
        auto fields = std::vector<std::shared_ptr<Field>>();
        for (idx = 0; idx < size; idx++) {
          child = avro_schema_record_field_get_by_index(schema, idx);
          auto elem_type = get_arrow_type(child);
          name = avro_schema_record_field_name(schema, idx);
          auto f = field(name, elem_type);
          fields.insert(fields.end(), f);
        }
        return struct_(fields);
      }
    }
  }

  Status generic_read(const avro_value_t val, ArrayBuilder *builder) {
    // Generic avro type read dispatcher. Dispatches to the various specializations by AVRO_TYPE.
    // This is used by the various readers for complex types"""
    avro_type_t avro_type;
    avro_type = avro_value_get_type(&val);

    switch (avro_type) {
      case AVRO_BOOLEAN:
        return this->read_bool(val, builder);
      case AVRO_INT32:
        return this->read_int32(val, builder);
      case AVRO_INT64:
        return this->read_int64(val, builder);
      case AVRO_FLOAT:
        return this->read_float32(val, builder);
      case AVRO_DOUBLE:
        return this->read_float64(val, builder);
      case AVRO_STRING:
        return this->read_string(val, builder);
      case AVRO_BYTES:
        return this->read_binary(val, builder);
      case AVRO_FIXED:
        return this->read_fixed(val, builder);
      case AVRO_MAP:
        return this->read_map(val, builder);
      case AVRO_RECORD:
        return this->read_record(val, builder);
      case AVRO_ARRAY:
        return this->read_array(val, builder);
      default:
        std::stringstream ss;
        ss << "Unhandled avro type: " << avro_type;
        return Status::NotImplemented(ss.str());
    }
  }

  Status read_string(const avro_value_t val, ArrayBuilder *builder) {
    size_t strlen;
    const char *c_str = NULL;
    avro_value_get_string(&val, &c_str, &strlen);
    return static_cast<StringBuilder *>(builder)->Append(c_str);
  }

  Status read_binary(const avro_value_t val, ArrayBuilder *builder) {
    size_t strlen;
    const char *c_str = NULL;
    avro_value_get_bytes(&val, (const void **) &c_str, &strlen);
    return static_cast<BinaryBuilder *>(builder)->Append(c_str);
  };

  Status read_fixed(const avro_value_t val, ArrayBuilder *builder) {
    size_t strlen;
    const char *c_str = NULL;
    std::string buffer;
    avro_value_get_fixed(&val, (const void **) &c_str, &strlen);
    buffer = c_str;
    return static_cast<FixedSizeBinaryBuilder *>(builder)->Append(buffer);
  }

  Status read_int32(const avro_value_t val, ArrayBuilder *builder) {
    int32_t out;
    avro_value_get_int(&val, &out);
    return static_cast<Int32Builder *>(builder)->Append(out);
  }

  Status read_int64(const avro_value_t val, ArrayBuilder *builder) {
    int64_t out;
    avro_value_get_long(&val, &out);
    return static_cast<Int64Builder *>(builder)->Append(out);
  }

  Status read_float64(const avro_value_t val, ArrayBuilder *builder) {
    double out;
    avro_value_get_double(&val, &out);
    return static_cast<DoubleBuilder *>(builder)->Append(out);
  }

  Status read_float32(avro_value_t val, ArrayBuilder *builder) {
    float out
        avro_value_get_float(&val, &out);
    return static_cast<FloatBuilder *>(builder)->Append(out);
  }

  Status read_bool(const avro_value_t val, ArrayBuilder *builder) {
    int temp;
    avro_value_get_boolean(&val, &temp);
    bool out = temp;
    return static_cast<BooleanBuilder *>(builder)->Append(out);
  }

  Status read_array(const avro_value_t val, ArrayBuilder *builder) {
    size_t actual_size;
    size_t i;
    const char *map_key = NULL;
    avro_value_t child;
    ArrayBuilder *child_builder;
    Status result;

    avro_value_get_size(&val, &actual_size);
    static_cast<ListBuilder *>(builder)->Append(true);
    child_builder = static_cast<ListBuilder *>(builder)->value_builder();
    for (i = 0; i < actual_size; i++) {
      avro_value_get_by_index(&val, i, &child, &map_key);
      result = generic_read(child, child_builder);
    }
    return result;
  };

  Status read_map(const avro_value_t val, ArrayBuilder *builder) {
    size_t num_values, i;
    auto listB = static_cast<ListBuilder *>(builder);
    auto structB = static_cast<StructBuilder *>(listB->child(0));
    auto keyBuilder = static_cast<StringBuilder *>(structB->child(0));
    auto valueBuilder = structB->child(1);
    avro_value_t child;
    const char *map_key;

    avro_value_get_size(&val, &num_values);

    listB->Append(true);
    for (i = 0; i < num_values; i++) {
      structB->Append(true);
      avro_value_get_by_index(&val, i, &child, &map_key);
      keyBuilder->Append(map_key);
      generic_read(child, valueBuilder);
    }
    return Status::OK();
  }

  Status read_record(const avro_value_t val, ArrayBuilder *builder) {
    avro_value_t child;
    ArrayBuilder *child_builder;
    StructBuilder *typed_builder;
    Status result;

    int container_length = builder->num_children();
    typed_builder = static_cast<StructBuilder *>(builder);
    typed_builder->Append();

    for (auto i = 0; i < container_length; i++) {
      avro_value_get_by_index(&val, i, &child, NULL);
      child_builder = typed_builder->child(i);
      result = generic_read(child, child_builder);
    }
    return result;
  }

};

// Public API

AvroArrowReader::AvroArrowReader() {
  impl_.reset(new AvroArrowReaderImpl());
  pool_ = default_memory_pool();
}

Status AvroArrowReader::ReadFromFileName(std::string filename, std::shared_ptr<Array> *out) {
  avro_file_reader_t reader = NULL;
  avro_file_reader(filename.c_str(), &reader);
  return this->ReadFromAvroFile(&reader, out);
}

Status AvroArrowReader::ReadFromAvroFile(avro_file_reader_t *file_reader,
                                         std::shared_ptr<Array> *out) {

  std::unique_ptr<ArrayBuilder> builder;
  std::shared_ptr<Array> res;
  std::shared_ptr<DataType> datatype;
  Status status;
  int rval;
  avro_value_iface_t *iface;
  avro_value_t record;
  ARROW_CHECK_OK(impl_->GenerateAvroSchema(file_reader, &datatype));
  ARROW_CHECK_OK(MakeBuilder(this->pool_, datatype, &builder));

  auto wschema = avro_file_reader_get_writer_schema(*file_reader);
  iface = avro_generic_class_from_schema(wschema);
  avro_generic_value_new(iface, &record);

  while (true) {
    rval = avro_file_reader_read_value(*file_reader, &record);
    if (rval != 0)
      break;
    // decompose record into Python types
    status = impl_->generic_read(record, std::move(builder).get());
  }

  // cast into our final types.
  ArrayBuilder* sBuilder = static_cast<ArrayBuilder*>(builder.get());
  StructBuilder* typedBuilder = static_cast<StructBuilder*>(sBuilder);
  typedBuilder->Finish(&res);

  out = &res;
  return Status::OK();
}

Status AvroArrowReader::GenerateAvroSchema(avro_file_reader_t *file_reader,
                                           std::shared_ptr<DataType> *out) {
  return impl_->GenerateAvroSchema(file_reader, out);
}

}
}
}

