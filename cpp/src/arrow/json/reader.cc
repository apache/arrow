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

#include "arrow/json/reader.h"

#include <unordered_map>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/io/readahead.h"
#include "arrow/json/chunker.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"
#include "arrow/util/task-group.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace json {

using internal::GetCpuThreadPool;
using internal::ThreadPool;
using io::internal::ReadaheadBuffer;
using io::internal::ReadaheadSpooler;

using internal::StringConverter;

class SerialTableReader : public TableReader {
 public:
  // Since we're converting serially, no need to readahead more than one block
  static constexpr int32_t block_queue_size = 1;

  SerialTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                    const ReadOptions& read_options, const ParseOptions& parse_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        readahead_(pool_, input, read_options_.block_size, block_queue_size),
        chunker_(Chunker::Make(parse_options_)),
        task_group_(internal::TaskGroup::MakeSerial()),
        column_count_(parse_options_.explicit_schema->num_fields()) {}

  Status Read(std::shared_ptr<Table>* out) override {
    ReadaheadBuffer rh;
    RETURN_NOT_OK(readahead_.Read(&rh));
    if (rh.buffer == nullptr) {
      return Status::Invalid("Empty JSON file");
    }

    for (auto partial = std::make_shared<Buffer>(""); rh.buffer != nullptr;) {
      // get the completion of a partial row from the previous block
      // FIXME(bkietz) this will just error out if a row spans more than a pair of blocks
      std::shared_ptr<Buffer> raw = rh.buffer, completion;
      RETURN_NOT_OK(chunker_->Process(partial, raw, &completion, &raw));

      // get all whole objects entirely inside the current buffer
      std::shared_ptr<Buffer> whole, next_partial;
      RETURN_NOT_OK(chunker_->Process(raw, &whole, &next_partial));

      // launch parse task
      int column_count;
      {
        // lock the schema mutex, store the current column count
        column_count = column_count_;
      }
      task_group_->Append([this, column_count, partial, completion, whole] {
        BlockParser parser(pool_, parse_options_, whole);
        if (completion->size() != 0) {
          // FIXME(bkietz) ensure that the parser has sufficient scalar storage for
          // all scalars in straddling + whole
          std::shared_ptr<Buffer> straddling;
          RETURN_NOT_OK(ConcatenateBuffers({partial, completion}, pool_, &straddling));
          RETURN_NOT_OK(parser.Parse(straddling));
        }
        RETURN_NOT_OK(parser.Parse(whole));
        std::shared_ptr<Array> parsed;
        RETURN_NOT_OK(parser.Finish(&parsed));
        return Convert(column_count, std::move(parsed));
      });

      RETURN_NOT_OK(readahead_.Read(&rh));
      partial = next_partial;
    }
    return Status::OK();
  }

  Status Convert(int column_count, std::shared_ptr<Array> parsed) {
    // if we are not inferring, flatten parsed into columns for conversion, hand off to
    // column builders, and return; nothing complicated.

    // lock the schema mutex (column builders are a vector and we may need to realloc it)

    // compare the (flattened) number of parsed columns to the number of columns expected
    // at launch; if they are equal hand off as above and return.

    // for each new column in parsed, identify an existing column builder or allocate a
    // new one. Hand off as above, and return.
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ReadaheadSpooler readahead_;
  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<internal::TaskGroup> task_group_;
  int column_count_;
};

Kind::type KindFromTag(const std::shared_ptr<const KeyValueMetadata>& tag) {
  std::string kind_name = tag->value(0);
  switch (kind_name[0]) {
    case 'n':
      if (kind_name[2] == 'l') {
        return Kind::kNull;
      } else {
        return Kind::kNumber;
      }
    case 'b':
      return Kind::kBoolean;
    case 's':
      return Kind::kString;
    case 'o':
      return Kind::kObject;
    case 'a':
      return Kind::kArray;
    default:
      ARROW_LOG(FATAL);
      return Kind::kNull;
  }
}

static Status Convert(const std::shared_ptr<DataType>& out_type,
                      std::shared_ptr<Array> in, std::shared_ptr<Array>* out);

// handle conversion to types with StringConverter
template <typename T>
Status ConvertEachWith(StringConverter<T>& convert_one,
                       const std::shared_ptr<DataType>& out_type, const Array* in,
                       std::shared_ptr<Array>* out) {
  auto dict_array = static_cast<const DictionaryArray*>(in);
  const StringArray& dict = static_cast<const StringArray&>(*dict_array->dictionary());
  const Int32Array& indices = static_cast<const Int32Array&>(*dict_array->indices());
  using Builder = typename TypeTraits<T>::BuilderType;
  Builder builder(out_type, default_memory_pool());
  RETURN_NOT_OK(builder.Resize(indices.length()));
  for (int64_t i = 0; i != indices.length(); ++i) {
    if (indices.IsNull(i)) {
      builder.UnsafeAppendNull();
      continue;
    }
    auto repr = dict.GetView(indices.GetView(i));
    typename StringConverter<T>::value_type value;
    if (!convert_one(repr.data(), repr.size(), &value)) {
      return Status::Invalid("Failed of conversion of JSON to ", *out_type, ":", repr);
    }
    builder.UnsafeAppend(value);
  }
  return builder.Finish(out);
}

struct ConvertImpl {
  Status VisitAs(const std::shared_ptr<DataType>& repr_type) {
    std::shared_ptr<Array> repr_array;
    RETURN_NOT_OK(Convert(repr_type, in, &repr_array));
    auto data = repr_array->data();
    data->type = out_type;
    *out = MakeArray(data);
    return Status::OK();
  }

  Status Visit(const NullType&) {
    *out = in;
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    *out = in;
    return Status::OK();
  }

  template <typename T>
  Status Visit(const T&, decltype(StringConverter<T>())* = nullptr) {
    StringConverter<T> convert_one;
    return ConvertEachWith(convert_one, out_type, in.get(), out);
  }

  // handle conversion to Timestamp
  Status Visit(const TimestampType&) {
    StringConverter<TimestampType> convert_one(out_type);
    return ConvertEachWith(convert_one, out_type, in.get(), out);
  }

  // handle types represented as integers
  template <typename T>
  Status Visit(
      const T&,
      typename std::enable_if<std::is_base_of<TimeType, T>::value ||
                              std::is_base_of<DateType, T>::value>::type* = nullptr) {
    return VisitAs(std::is_same<typename T::c_type, int64_t>::value ? int64() : int32());
  }

  // handle binary and string
  template <typename T>
  Status Visit(
      const T& t,
      typename std::enable_if<std::is_base_of<BinaryType, T>::value>::type* = nullptr) {
    auto dict_array = static_cast<const DictionaryArray*>(in.get());
    const StringArray& dict = static_cast<const StringArray&>(*dict_array->dictionary());
    const Int32Array& indices = static_cast<const Int32Array&>(*dict_array->indices());
    using Builder = typename TypeTraits<T>::BuilderType;
    Builder builder(out_type, default_memory_pool());
    RETURN_NOT_OK(builder.Resize(indices.length()));
    int64_t values_length = 0;
    for (int64_t i = 0; i != indices.length(); ++i) {
      if (indices.IsNull(i)) {
        continue;
      }
      values_length += dict.GetView(indices.GetView(i)).size();
    }
    RETURN_NOT_OK(builder.ReserveData(values_length));
    for (int64_t i = 0; i != indices.length(); ++i) {
      if (indices.IsNull(i)) {
        builder.UnsafeAppendNull();
        continue;
      }
      auto value = dict.GetView(indices.GetView(i));
      builder.UnsafeAppend(value);
    }
    return builder.Finish(out);
  }

  Status Visit(const ListType& t) {
    auto list_array = static_cast<const ListArray*>(in.get());
    std::shared_ptr<Array> values;
    auto value_type = t.value_type();
    RETURN_NOT_OK(Convert(value_type, list_array->values(), &values));
    auto data = ArrayData::Make(out_type, in->length(),
                                {in->null_bitmap(), list_array->value_offsets()},
                                {values->data()}, in->null_count());
    *out = MakeArray(data);
    return Status::OK();
  }

  Status Visit(const StructType& t) {
    auto struct_array = static_cast<const StructArray*>(in.get());
    std::vector<std::shared_ptr<ArrayData>> child_data(t.num_children());
    for (int i = 0; i != t.num_children(); ++i) {
      std::shared_ptr<Array> child;
      RETURN_NOT_OK(Convert(t.child(i)->type(), struct_array->field(i), &child));
      child_data[i] = child->data();
    }
    auto data = ArrayData::Make(out_type, in->length(), {in->null_bitmap()},
                                std::move(child_data), in->null_count());
    *out = MakeArray(data);
    return Status::OK();
  }

  Status Visit(const DataType& not_impl) {
    return Status::NotImplemented("JSON parsing of ", not_impl);
  }

  std::shared_ptr<DataType> out_type;
  std::shared_ptr<Array> in;
  std::shared_ptr<Array>* out;
};

static Status Convert(const std::shared_ptr<DataType>& out_type,
                      std::shared_ptr<Array> in, std::shared_ptr<Array>* out) {
  ConvertImpl visitor = {out_type, in, out};
  return VisitTypeInline(*out_type, &visitor);
}

static Status InferAndConvert(std::shared_ptr<DataType> expected,
                              const std::shared_ptr<const KeyValueMetadata>& tag,
                              const std::shared_ptr<Array>& in,
                              std::shared_ptr<Array>* out) {
  Kind::type kind = KindFromTag(tag);
  switch (kind) {
    case Kind::kObject: {
      // FIXME(bkietz) in general expected fields may not be an exact prefix of parsed's
      auto in_type = static_cast<StructType*>(in->type().get());
      if (expected == nullptr) {
        expected = struct_({});
      }
      auto expected_type = static_cast<StructType*>(expected.get());
      if (in_type->num_children() == expected_type->num_children()) {
        return Convert(expected, in, out);
      }

      auto fields = expected_type->children();
      fields.resize(in_type->num_children());
      std::vector<std::shared_ptr<ArrayData>> child_data(in_type->num_children());

      for (int i = 0; i != in_type->num_children(); ++i) {
        std::shared_ptr<DataType> expected_field_type;
        if (i < expected_type->num_children()) {
          expected_field_type = expected_type->child(i)->type();
        }
        auto in_field = in_type->child(i);
        auto in_column = static_cast<StructArray*>(in.get())->field(i);
        std::shared_ptr<Array> column;
        RETURN_NOT_OK(InferAndConvert(expected_field_type, in_field->metadata(),
                                      in_column, &column));
        fields[i] = field(in_field->name(), column->type());
        child_data[i] = column->data();
      }
      auto data =
          ArrayData::Make(struct_(std::move(fields)), in->length(), {in->null_bitmap()},
                          std::move(child_data), in->null_count());
      *out = MakeArray(data);
      return Status::OK();
    }
    case Kind::kArray: {
      auto list_array = static_cast<const ListArray*>(in.get());
      auto value_tag = list_array->list_type()->value_field()->metadata();
      std::shared_ptr<Array> values;
      if (expected != nullptr) {
        RETURN_NOT_OK(InferAndConvert(expected->child(0)->type(), value_tag,
                                      list_array->values(), &values));
      } else {
        RETURN_NOT_OK(InferAndConvert(nullptr, value_tag, list_array->values(), &values));
      }
      auto data = ArrayData::Make(list(values->type()), in->length(),
                                  {in->null_bitmap(), list_array->value_offsets()},
                                  {values->data()}, in->null_count());
      *out = MakeArray(data);
      return Status::OK();
    }
    default:
      // an expected type overrides inferrence for scalars
      // (but not nested types, which may have unexpected fields)
      if (expected != nullptr) {
        return Convert(expected, in, out);
      }
  }
  switch (kind) {
    case Kind::kNull:
      return Convert(null(), in, out);
    case Kind::kBoolean:
      return Convert(boolean(), in, out);
    case Kind::kNumber:
      // attempt conversion to Int64 first
      if (Convert(int64(), in, out).ok()) {
        return Status::OK();
      }
      return Convert(float64(), in, out);
    case Kind::kString:  // attempt conversion to Timestamp first
      if (Convert(timestamp(TimeUnit::SECOND), in, out).ok()) {
        return Status::OK();
      }
      return Convert(utf8(), in, out);
    default:
      return Status::Invalid("invalid JSON kind");
  }
}

Status ParseOne(ParseOptions options, std::shared_ptr<Buffer> json,
                std::shared_ptr<RecordBatch>* out) {
  BlockParser parser(default_memory_pool(), options, json);
  RETURN_NOT_OK(parser.Parse(json));
  std::shared_ptr<Array> parsed;
  RETURN_NOT_OK(parser.Finish(&parsed));
  std::shared_ptr<Array> converted;
  auto schm = options.explicit_schema;
  if (options.unexpected_field_behavior == UnexpectedFieldBehavior::InferType) {
    if (schm) {
      RETURN_NOT_OK(InferAndConvert(struct_(schm->fields()), Tag(Kind::kObject), parsed,
                                    &converted));
    } else {
      RETURN_NOT_OK(InferAndConvert(nullptr, Tag(Kind::kObject), parsed, &converted));
    }
    schm = schema(converted->type()->children());
  } else {
    RETURN_NOT_OK(Convert(struct_(schm->fields()), parsed, &converted));
  }
  std::vector<std::shared_ptr<Array>> columns(parsed->num_fields());
  for (int i = 0; i != parsed->num_fields(); ++i) {
    columns[i] = static_cast<StructArray*>(converted.get())->field(i);
  }
  *out = RecordBatch::Make(schm, parsed->length(), std::move(columns));
  return Status::OK();
}

}  // namespace json
}  // namespace arrow
