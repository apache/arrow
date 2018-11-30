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

#include "arrow/json/parser.h"

#include <algorithm>
#include <cstdio>
#include <sstream>
#include <utility>

#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"

namespace arrow {
namespace json {

using internal::checked_cast;
using internal::StringConverter;

struct ParseError {
  ParseError() { ss_ << "JSON parse error: "; }

  template <typename T>
  ParseError& operator<<(T&& t) {
    ss_ << t;
    return *this;
  }

  operator Status() { return Status::Invalid(ss_.str()); }

  std::stringstream ss_;
};

struct ConversionError {
  ConversionError() { ss_ << "Conversion error: "; }

  template <typename T>
  ConversionError& operator<<(T&& t) {
    ss_ << t;
    return *this;
  }

  operator Status() { return Status::Invalid(ss_.str()); }

  std::stringstream ss_;
};

template <typename Visitor>
Status VisitBuilder(ArrayBuilder* builder, Visitor&& visitor) {
  // if (builder == nullptr) {
  //   return visitor.Null();
  // }
  switch (builder->type()->id()) {
    case Type::NA:
      return visitor.Visit(static_cast<NullBuilder*>(builder));
    case Type::BOOL:
      return visitor.Visit(static_cast<BooleanBuilder*>(builder));
    case Type::UINT8:
      return visitor.Visit(static_cast<UInt8Builder*>(builder));
    case Type::INT8:
      return visitor.Visit(static_cast<Int8Builder*>(builder));
    case Type::UINT16:
      return visitor.Visit(static_cast<UInt16Builder*>(builder));
    case Type::INT16:
      return visitor.Visit(static_cast<Int16Builder*>(builder));
    case Type::UINT32:
      return visitor.Visit(static_cast<UInt32Builder*>(builder));
    case Type::INT32:
      return visitor.Visit(static_cast<Int32Builder*>(builder));
    case Type::UINT64:
      return visitor.Visit(static_cast<UInt64Builder*>(builder));
    case Type::INT64:
      return visitor.Visit(static_cast<Int64Builder*>(builder));
    case Type::HALF_FLOAT:
      return visitor.Visit(static_cast<HalfFloatBuilder*>(builder));
    case Type::FLOAT:
      return visitor.Visit(static_cast<FloatBuilder*>(builder));
    case Type::DOUBLE:
      return visitor.Visit(static_cast<DoubleBuilder*>(builder));
    case Type::STRING:
      return visitor.Visit(static_cast<StringBuilder*>(builder));
    case Type::BINARY:
      return visitor.Visit(static_cast<BinaryBuilder*>(builder));
    case Type::FIXED_SIZE_BINARY:
      return visitor.Visit(static_cast<FixedSizeBinaryBuilder*>(builder));
    case Type::DATE32:
      return visitor.Visit(static_cast<Date32Builder*>(builder));
    case Type::DATE64:
      return visitor.Visit(static_cast<Date64Builder*>(builder));
    case Type::TIMESTAMP:
      return visitor.Visit(static_cast<TimestampBuilder*>(builder));
    case Type::TIME32:
      return visitor.Visit(static_cast<Time32Builder*>(builder));
    case Type::TIME64:
      return visitor.Visit(static_cast<Time64Builder*>(builder));
    case Type::INTERVAL:
      return Status::NotImplemented("No IntervalBuilder");
    case Type::DECIMAL:
      return visitor.Visit(static_cast<Decimal128Builder*>(builder));
    case Type::LIST:
      return visitor.Visit(static_cast<ListBuilder*>(builder));
    case Type::STRUCT:
      return visitor.Visit(static_cast<StructBuilder*>(builder));
    case Type::UNION:
      return Status::NotImplemented("No UnionBuilder");
    case Type::DICTIONARY:
      return Status::NotImplemented("No DictionaryBuilder");
    case Type::MAP:
      return Status::NotImplemented("No MapBuilder");
  }
}

class TypedHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, TypedHandler> {
 public:
  TypedHandler(StructBuilder* root_builder) : builder_(root_builder) {}

  bool Null() {
    if (Skipping()) return true;
    status_ = VisitBuilder(builder_, AppendNullVisitor{});
    return status_.ok();
  }

  bool Bool(bool value) {
    if (Skipping()) return true;
    if (builder_->type()->id() == Type::BOOL) {
      status_ = static_cast<BooleanBuilder*>(builder_)->Append(value);
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool RawNumber(const char* data, rapidjson::SizeType size, bool) {
    if (Skipping()) return true;
    status_ = VisitBuilder(builder_, AppendNumberVisitor{data, size});
    return status_.ok();
  }

  bool String(const char* data, rapidjson::SizeType size, bool) {
    if (Skipping()) return true;
    status_ = VisitBuilder(builder_, AppendStringVisitor{data, size});
    return status_.ok();
  }

  bool StartObject() {
    ++depth_;
    if (Skipping()) return true;
    if (builder_->type()->id() == Type::STRUCT) {
      auto struct_builder = static_cast<StructBuilder*>(builder_);
      auto num_fields = struct_builder->num_fields();
      status_ = struct_builder->Append();
      builder_stack_.push_back(builder_);
      null_fields_.push_back(std::vector<bool>(num_fields, true));
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool Key(const char* key, rapidjson::SizeType len, bool) {
    MaybeStopSkipping();  // new key at the depth where we started skipping -> terminate
                          // skipping
    if (Skipping()) return true;
    DCHECK_EQ(builder_stack_.back()->type()->id(), Type::STRUCT)
        << "how did we encounter a key here?";
    auto parent = static_cast<StructBuilder*>(builder_stack_.back());
    auto parent_type = std::static_pointer_cast<StructType>(parent->type());
    auto field_index = parent_type->GetChildIndex(std::string(key, len));
    if (field_index == -1) {
      skip_depth_ = depth_;
      return true;
    }
    null_fields_.back()[field_index] = false;
    builder_ = parent->field_builder(field_index);
    return true;
  }

  bool EndObject(rapidjson::SizeType) {
    MaybeStopSkipping();  // end of object containing depth where we started skipping ->
                          // terminate skipping
    --depth_;
    if (Skipping()) return true;
    int field_index = 0;
    auto parent = static_cast<StructBuilder*>(builder_stack_.back());
    for (bool null : null_fields_.back()) {
      // TODO since this is expected to be sparse, it would probably be more efficient to
      // use CountLeadingZeros() to find the indices of the few null fields
      if (null) {
        status_ = VisitBuilder(parent->field_builder(field_index), AppendNullVisitor{});
        if (!status_.ok()) return false;
      }
      ++field_index;
    }
    null_fields_.pop_back();
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
    return true;
  }

  bool StartArray() {
    if (Skipping()) return true;
    if (builder_->type()->id() == Type::LIST) {
      auto list_builder = static_cast<ListBuilder*>(builder_);
      status_ = list_builder->Append();
      builder_stack_.push_back(builder_);
      builder_ = list_builder->value_builder();
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool EndArray(rapidjson::SizeType) {
    if (Skipping()) return true;
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
    return true;
  }

  Status Error() { return status_; }

 private:
  bool Skipping() { return depth_ >= skip_depth_; }

  void MaybeStopSkipping() {
    if (skip_depth_ == depth_) {
      skip_depth_ = std::numeric_limits<int>::max();
    }
  }

  struct AppendNullVisitor {
    Status Visit(NullBuilder* b) { return b->AppendNull(); }
    Status Visit(BooleanBuilder* b) { return b->AppendNull(); }
    template <typename T>
    Status Visit(NumericBuilder<T>* b) {
      return b->AppendNull();
    }
    Status Visit(BinaryBuilder* b) { return b->AppendNull(); }
    Status Visit(FixedSizeBinaryBuilder* b) { return b->AppendNull(); }
    Status Visit(ListBuilder* b) { return b->AppendNull(); }
    Status Visit(StructBuilder* b) {
      ARROW_RETURN_NOT_OK(b->AppendNull());
      for (int i = 0; i != b->num_fields(); ++i) {
        auto field_builder = b->field_builder(i);
        ARROW_RETURN_NOT_OK(VisitBuilder(field_builder, AppendNullVisitor()));
      }
      return Status::OK();
    }
  };

  struct AppendNumberVisitor {
    template <typename T>
    Status Visit(NumericBuilder<T>* b) {
      return Append<T>(b);
    }
    Status Visit(HalfFloatBuilder* b) {
      // FIXME this should parse a float and convert it down to half, we need the
      // converter though
      return Append<UInt16Type>(b);
    }
    Status Visit(TimestampBuilder* b) { return Append<Int64Type>(b); }
    Status Visit(Time32Builder* b) { return Append<Int32Type>(b); }
    Status Visit(Time64Builder* b) { return Append<Int64Type>(b); }
    Status Visit(Date32Builder* b) { return Append<Int32Type>(b); }
    Status Visit(Date64Builder* b) { return Append<Int64Type>(b); }
    Status Visit(Decimal128Builder* b) {
      int64_t value;
      ARROW_RETURN_NOT_OK(ConvertTo<Int64Type>(&value));
      return b->Append(Decimal128(value));
    }
    Status Visit(ArrayBuilder*) { return ConversionError(); }
    template <typename Repr>
    Status ConvertTo(typename StringConverter<Repr>::value_type* value) {
      StringConverter<Repr> converter;
      if (!converter(data_, size_, value)) {
        return ConversionError();
      }
      return Status::OK();
    }
    template <typename Repr, typename Logical>
    Status Append(NumericBuilder<Logical>* b) {
      typename StringConverter<Repr>::value_type value;
      ARROW_RETURN_NOT_OK(ConvertTo<Repr>(&value));
      return b->Append(value);
    }
    const char* data_;
    rapidjson::SizeType size_;
  };

  struct AppendStringVisitor {
    Status Visit(BinaryBuilder* b) { return b->Append(data_, size_); }
    Status Visit(FixedSizeBinaryBuilder* b) {
      if (static_cast<rapidjson::SizeType>(b->byte_width()) != size_) {
        return ConversionError();
      }
      return b->Append(data_);
    }
    Status Visit(TimestampBuilder* b) {
      StringConverter<TimestampType> converter(b->type());
      typename StringConverter<TimestampType>::value_type value;
      if (!converter(data_, size_, &value)) {
        return ConversionError();
      }
      return b->Append(value);
    }
    Status Visit(ArrayBuilder*) { return ConversionError(); }
    const char* data_;
    rapidjson::SizeType size_;
  };

  ArrayBuilder* builder_;
  Status status_;
  std::vector<ArrayBuilder*> builder_stack_;
  std::vector<std::vector<bool>> null_fields_;
  int depth_ = 0;
  int skip_depth_ = std::numeric_limits<int>::max();
};

Status BlockParser::Parse(const char* data, uint32_t size, uint32_t* out_size) {
  if (!options_.explicit_schema) {
    return Status::NotImplemented("No type inferrence yet");
  }

  // rapidjson::InsituStringStream ss(const_cast<char*>(data));
  rapidjson::StringStream ss(data);
  rapidjson::Reader reader;

  constexpr unsigned parse_flags =  // rapidjson::kParseInsituFlag |
      rapidjson::kParseIterativeFlag | rapidjson::kParseStopWhenDoneFlag |
      rapidjson::kParseNumbersAsStringsFlag;

  std::unique_ptr<ArrayBuilder> root_builder;
  auto struct_type = struct_(options_.explicit_schema->fields());
  ARROW_RETURN_NOT_OK(MakeBuilder(pool_, struct_type, &root_builder));

  TypedHandler handler(static_cast<StructBuilder*>(root_builder.get()));
  for (int i = 0; i != max_num_rows_; ++i) {
    // parse a single line of JSON
    auto ok = reader.Parse<parse_flags>(ss, handler);
    switch (ok.Code()) {
      case rapidjson::kParseErrorNone:
        // parse the next object
        continue;
      case rapidjson::kParseErrorDocumentEmpty: {
        // parsed all objects, finish
        std::shared_ptr<Array> array;
        ARROW_RETURN_NOT_OK(root_builder->Finish(&array));
        auto struct_array = std::static_pointer_cast<StructArray>(array);
        std::vector<std::shared_ptr<Array>> columns;
        for (int i = 0; i != options_.explicit_schema->num_fields(); ++i) {
          columns.push_back(struct_array->field(i));
        }
        parsed_ =
            RecordBatch::Make(options_.explicit_schema, num_rows_, std::move(columns));
        return Status::OK();
      }
      case rapidjson::kParseErrorTermination:
        // handler emitted an error
        return handler.Error();
      default:
        // rapidjson emitted an error
        // FIXME(bkietz) this will probably need to be localized better
        return ParseError() << rapidjson::GetParseError_En(ok.Code());
    }
  }
  return Status::Invalid("Exceeded maximum rows");
}

BlockParser::BlockParser(MemoryPool* pool, ParseOptions options, int32_t num_cols,
                         int32_t max_num_rows)
    : pool_(pool), options_(options), num_cols_(num_cols), max_num_rows_(max_num_rows) {}

BlockParser::BlockParser(ParseOptions options, int32_t num_cols, int32_t max_num_rows)
    : BlockParser(default_memory_pool(), options, num_cols, max_num_rows) {}

}  // namespace json
}  // namespace arrow
