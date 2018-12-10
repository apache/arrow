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
#include "arrow/builder.h"
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
    default:
      return Status::NotImplemented("Unknown Builder");
  }
}

class TypedHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, TypedHandler> {
 public:
  TypedHandler(StructBuilder* root_builder) : builder_(root_builder) {}

  bool Null() {
    if (Skipping()) return true;
    // TODO check whether we're currently nullable
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
      absent_fields_.push_back(std::vector<bool>(num_fields, true));
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool Key(const char* key, rapidjson::SizeType len, bool) {
    MaybeStopSkipping();  // new key at the depth where we started skipping ->
                          // terminate skipping
    if (Skipping()) return true;
    auto parent = static_cast<StructBuilder*>(builder_stack_.back());
    auto parent_type = std::static_pointer_cast<StructType>(parent->type());
    auto field_index = parent_type->GetChildIndex(std::string(key, len));
    if (field_index == -1) {
      skip_depth_ = depth_;
      return true;
    }
    absent_fields_.back()[field_index] = false;
    builder_ = parent->field_builder(field_index);
    return true;
  }

  bool EndObject(rapidjson::SizeType) {
    MaybeStopSkipping();  // end of object containing depth where we started
                          // skipping -> terminate skipping
    --depth_;
    if (Skipping()) return true;
    int field_index = 0;
    auto parent = static_cast<StructBuilder*>(builder_stack_.back());
    for (bool null : absent_fields_.back()) {
      // TODO since this is expected to be sparse, it would probably be more
      // efficient to use CountLeadingZeros() to find the indices of the few
      // null fields
      if (null) {
        status_ = VisitBuilder(parent->field_builder(field_index),
                               AppendNullVisitor{});
        if (!status_.ok()) return false;
      }
      ++field_index;
    }
    absent_fields_.pop_back();
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
      // FIXME this should parse a float and convert it down to half, we need
      // the converter though
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
  std::vector<std::vector<bool>> absent_fields_;
  int depth_ = 0;
  int skip_depth_ = std::numeric_limits<int>::max();
};

class AdaptiveNullBuilder;
class AdaptiveBooleanBuilder;
class Int64OrDoubleBuilder;
class TimestampOrStringBuilder;
class AdaptiveStructBuilder;
class AdaptiveListBuilder;

template <typename Visitor>
Status VisitAdaptiveBuilder(ArrayBuilder* builder, Visitor&& visitor) {
  switch (builder->type()->id()) {
    case Type::NA:
      return visitor.Visit(static_cast<AdaptiveNullBuilder*>(builder));
    case Type::BOOL:
      return visitor.Visit(static_cast<AdaptiveBooleanBuilder*>(builder));
    case Type::INT64:
    case Type::DOUBLE:
      return visitor.Visit(static_cast<Int64OrDoubleBuilder*>(builder));
    case Type::STRING:
    case Type::TIMESTAMP:
      return visitor.Visit(static_cast<TimestampOrStringBuilder*>(builder));
    case Type::LIST:
      return visitor.Visit(static_cast<AdaptiveListBuilder*>(builder));
    case Type::STRUCT:
      return visitor.Visit(static_cast<AdaptiveStructBuilder*>(builder));
    default:
      return Status::NotImplemented("Not a custom builder");
  }
}

struct UpdateTypeVisitor {
  Status Visit(AdaptiveStructBuilder*);
  Status Visit(AdaptiveListBuilder*);
  Status Visit(ArrayBuilder*) {
    return Status::OK();
  }
};

class AdaptiveNullBuilder : public NullBuilder {
 public:
  using NullBuilder::NullBuilder;

  Status SetLeadingNulls(int64_t leading_nulls) {
    ARROW_CHECK(length_ == 0);
    null_count_ = leading_nulls;
    length_ = leading_nulls;
    return Status::OK();
  }
};

class AdaptiveBooleanBuilder : public BooleanBuilder {
 public:
  using BooleanBuilder::BooleanBuilder;

  Status SetLeadingNulls(int64_t leading_nulls) {
    ARROW_CHECK(length_ == 0);
    null_count_ = leading_nulls;
    length_ = leading_nulls;
    return Resize(leading_nulls);
  }
};

class Int64OrDoubleBuilder : public FixedSizeBinaryBuilder {
 public:
  explicit Int64OrDoubleBuilder(MemoryPool* pool)
      : FixedSizeBinaryBuilder(fixed_size_binary(sizeof(int64_t)), pool) {
    type_ = int64();
  }

  Status SetLeadingNulls(int64_t leading_nulls) {
    ARROW_CHECK(length_ == 0);
    null_count_ = leading_nulls;
    length_ = leading_nulls;
    ARROW_RETURN_NOT_OK(byte_builder_.Advance(sizeof(int64_t) * leading_nulls));
    return Resize(leading_nulls);
  }

  using FixedSizeBinaryBuilder::AppendNull;

  Status Append(util::string_view repr) {
    if (failed_conversion_to_int_ < 0) {
      StringConverter<Int64Type> converter;
      int64_t value;
      if (converter(repr.data(), repr.size(), &value)) {
        return FixedSizeBinaryBuilder::Append(
            reinterpret_cast<const char*>(&value));
      }
      failed_conversion_to_int_ = length_;
      type_ = float64();
    }
    StringConverter<DoubleType> converter;
    double value;
    if (converter(repr.data(), repr.size(), &value)) {
      return FixedSizeBinaryBuilder::Append(
          reinterpret_cast<const char*>(&value));
    }
    return ConversionError();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    std::shared_ptr<Buffer> data;
    ARROW_RETURN_NOT_OK(byte_builder_.Finish(&data));

    if (failed_conversion_to_int_ > 0) {
      // cast preceding ints to double
      auto doubles = reinterpret_cast<double*>(data->mutable_data());
      auto ints = reinterpret_cast<const int64_t*>(data->data());
      for (int64_t i = 0; i != failed_conversion_to_int_; ++i) {
        doubles[i] = static_cast<double>(ints[i]);
      }
    }

    *out = ArrayData::Make(type_, length_, {null_bitmap_, data}, null_count_);
    return Status::OK();
  }

 private:
  int64_t failed_conversion_to_int_ = -1;
};

class TimestampOrStringBuilder : public BinaryBuilder {
 public:
  TimestampOrStringBuilder(MemoryPool* pool)
      : BinaryBuilder(timestamp(TimeUnit::SECOND), pool),
      timestamp_builder_(pool) {}

  Status SetLeadingNulls(int64_t leading_nulls) {
    ARROW_CHECK(length_ == 0);
    null_count_ = leading_nulls;
    length_ = leading_nulls;
    ARROW_RETURN_NOT_OK(timestamp_builder_.Resize(sizeof(int64_t) * leading_nulls));
    return Resize(leading_nulls);
  }

  Status AppendNull() {
    if (type_->id() == Type::TIMESTAMP) {
      ARROW_RETURN_NOT_OK(timestamp_builder_.Append(0));
    }
    return BinaryBuilder::AppendNull();
  }

  Status Append(util::string_view repr) {
    if (type_->id() == Type::TIMESTAMP) {
      StringConverter<TimestampType> converter(type_);
      int64_t value;
      if (converter(repr.data(), repr.size(), &value)) {
        ARROW_RETURN_NOT_OK(timestamp_builder_.Append(value));
      } else {
        type_ = utf8();
        timestamp_builder_.Reset(); // we don't need to store these timestamps now
      }
    }
    return BinaryBuilder::Append(repr);
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    if (type_->id() == Type::TIMESTAMP) {
      std::shared_ptr<Buffer> timestamps;
      ARROW_RETURN_NOT_OK(timestamp_builder_.Finish(&timestamps));
      *out = ArrayData::Make(type_, length_, {null_bitmap_, timestamps}, null_count_);
      return Status::OK();
    }
    return BinaryBuilder::FinishInternal(out);
  }

 private:
  TypedBufferBuilder<int64_t> timestamp_builder_;
};

class AdaptiveStructBuilder : public StructBuilder {
 public:
  explicit AdaptiveStructBuilder(MemoryPool* pool)
      : StructBuilder(struct_({}), pool, {}) {}

  Status SetLeadingNulls(int64_t leading_nulls) {
    ARROW_CHECK(length_ == 0);
    ARROW_CHECK(num_fields() == 0);
    null_count_ = leading_nulls;
    length_ = leading_nulls;
    return Resize(leading_nulls);
  }

  Status AddField(const std::string& name) {
    name_to_index_[name] = static_cast<int>(field_builders_.size());
    auto null_builder = std::make_shared<AdaptiveNullBuilder>(pool_);
    field_builders_.push_back(null_builder);
    return null_builder->SetLeadingNulls(length_ - 1);
  }

  // assumes that new builder is initialized with the correct element count
  void SetFieldBuilder(int index, std::shared_ptr<ArrayBuilder> builder) {
    field_builders_[index] = builder;
  }

  int GetFieldIndex(const std::string& name) {
    auto it = name_to_index_.find(name);
    if (it == name_to_index_.end())
      return -1;
    return it->second;
  }

  Status UpdateType() {
    std::vector<std::shared_ptr<Field>> fields(field_builders_.size());
    for (auto &&name_index : name_to_index_) {
      auto builder = field_builders_[name_index.second].get();
      ARROW_RETURN_NOT_OK(VisitAdaptiveBuilder(builder, UpdateTypeVisitor{}));
      fields[name_index.second] = field(name_index.first, builder->type());
    }
    type_ = struct_(std::move(fields));
    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    ARROW_RETURN_NOT_OK(UpdateType());
    return StructBuilder::FinishInternal(out);
  }

 private:
  std::unordered_map<std::string, int> name_to_index_;
};

class AdaptiveListBuilder : public ListBuilder {
 public:
  explicit AdaptiveListBuilder(MemoryPool* pool)
      : ListBuilder(pool, std::make_shared<NullBuilder>(pool)) {}

  Status SetLeadingNulls(int64_t leading_nulls) {
    ARROW_CHECK(length_ == 0);
    null_count_ = leading_nulls;
    length_ = leading_nulls;
    ARROW_RETURN_NOT_OK(offsets_builder_.Resize(sizeof(int32_t) * leading_nulls));
    return Resize(leading_nulls);
  }

  Status UpdateType() {
    ARROW_RETURN_NOT_OK(VisitAdaptiveBuilder(value_builder_.get(), UpdateTypeVisitor{}));
    type_ = list(value_builder_->type());
    return Status::OK();
  }

  // assumes that existing offsets are correct within the new builder
  void SetValueBuilder(std::shared_ptr<ArrayBuilder> builder) {
    value_builder_ = builder;
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    ARROW_RETURN_NOT_OK(UpdateType());
    return ListBuilder::FinishInternal(out);
  }
};

Status UpdateTypeVisitor::Visit(AdaptiveStructBuilder* b) { return b->UpdateType(); }
Status UpdateTypeVisitor::Visit(AdaptiveListBuilder* b) { return b->UpdateType(); }

class InferringHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, InferringHandler> {
 public:
  InferringHandler(MemoryPool* pool, AdaptiveStructBuilder* root_builder) : pool_(pool), builder_(root_builder) {}

  bool Null() {
    status_ = VisitAdaptiveBuilder(builder_, AppendNullVisitor{});
    return status_.ok();
  }

  bool Bool(bool value) {
    if (!UpgradeIfNull<AdaptiveBooleanBuilder>()) return false;
    if (builder_->type()->id() == Type::BOOL) {
      status_ = static_cast<BooleanBuilder*>(builder_)->Append(value);
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool RawNumber(const char* data, rapidjson::SizeType size, bool) {
    if (!UpgradeIfNull<Int64OrDoubleBuilder>()) return false;
    status_ = VisitAdaptiveBuilder(builder_, AppendNumberVisitor{data, size});
    return status_.ok();
  }

  bool String(const char* data, rapidjson::SizeType size, bool) {
    if (!UpgradeIfNull<TimestampOrStringBuilder>()) return false;
    status_ = VisitAdaptiveBuilder(builder_, AppendStringVisitor{data, size});
    return status_.ok();
  }

  bool StartObject() {
    if (!UpgradeIfNull<AdaptiveStructBuilder>()) return false;
    if (builder_->type()->id() == Type::STRUCT) {
      auto struct_builder = static_cast<StructBuilder*>(builder_);
      auto num_fields = struct_builder->num_fields();
      builder_stack_.push_back(builder_);
      absent_fields_.push_back(std::vector<bool>(num_fields, true));
      field_index_stack_.push_back(-1);
      status_ = struct_builder->Append();
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool Key(const char* key, rapidjson::SizeType len, bool) {
    auto parent = static_cast<AdaptiveStructBuilder*>(builder_stack_.back());
    std::string name(key, len);
    int field_index = parent->GetFieldIndex(name);
    if (field_index == -1) {
      // add this field to the parent
      field_index = parent->num_fields();
      absent_fields_.back().push_back(false);
      status_ = parent->AddField(name);
      if (!status_.ok()) return false;
    }
    field_index_stack_.back() = field_index;
    absent_fields_.back()[field_index] = false;
    builder_ = parent->field_builder(field_index);
    return true;
  }

  bool EndObject(rapidjson::SizeType) {
    int field_index = 0;
    auto parent = static_cast<StructBuilder*>(builder_stack_.back());
    for (bool null : absent_fields_.back()) {
      // TODO since this is expected to be sparse, it would probably be more
      // efficient to use CountLeadingZeros() to find the indices of the few
      // null fields
      if (null) {
        status_ = VisitAdaptiveBuilder(parent->field_builder(field_index),
                               AppendNullVisitor{});
        if (!status_.ok()) return false;
      }
      ++field_index;
    }
    field_index_stack_.pop_back();
    absent_fields_.pop_back();
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
    return true;
  }

  bool StartArray() {
    if (!UpgradeIfNull<AdaptiveListBuilder>()) return false;
    if (builder_->type()->id() == Type::LIST) {
      auto list_builder = static_cast<ListBuilder*>(builder_);
      builder_stack_.push_back(builder_);
      builder_ = list_builder->value_builder();
      field_index_stack_.push_back(-1);
      status_ = list_builder->Append();
    } else {
      status_ = ConversionError();
    }
    return status_.ok();
  }

  bool EndArray(rapidjson::SizeType) {
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
    field_index_stack_.pop_back();
    return true;
  }

  Status Error() { return status_; }

 private:
  template <typename Builder>
  bool UpgradeIfNull() {
    if (ARROW_PREDICT_TRUE(builder_->type()->id() != Type::NA))
      return true;
    auto null_count = builder_->null_count();
    int field_index = field_index_stack_.back();
    std::shared_ptr<Builder> new_builder;
    new_builder.reset(new Builder(pool_));
    builder_ = new_builder.get();
    if (field_index == -1) {
      auto parent = static_cast<AdaptiveListBuilder*>(builder_stack_.back());
      parent->SetValueBuilder(new_builder);
    } else {
      auto parent = static_cast<AdaptiveStructBuilder*>(builder_stack_.back());
      parent->SetFieldBuilder(field_index, new_builder);
    }
    status_ = new_builder->SetLeadingNulls(null_count);
    return status_.ok();
  }

  struct AppendNullVisitor {
    Status Visit(NullBuilder* b) { return b->AppendNull(); }
    Status Visit(BooleanBuilder* b) { return b->AppendNull(); }
    Status Visit(Int64OrDoubleBuilder* b) { return b->AppendNull(); }
    Status Visit(TimestampOrStringBuilder* b) { return b->AppendNull(); }
    Status Visit(ListBuilder* b) { return b->AppendNull(); }
    Status Visit(StructBuilder* b) {
      ARROW_RETURN_NOT_OK(b->AppendNull());
      for (int i = 0; i != b->num_fields(); ++i) {
        auto field_builder = b->field_builder(i);
        ARROW_RETURN_NOT_OK(VisitAdaptiveBuilder(field_builder, AppendNullVisitor()));
      }
      return Status::OK();
    }
  };

  struct AppendNumberVisitor {
    Status Visit(ArrayBuilder* b) { return ConversionError(); }
    Status Visit(Int64OrDoubleBuilder* b) { return b->Append(util::string_view(data_, size_)); }
    const char* data_;
    rapidjson::SizeType size_;
  };

  struct AppendStringVisitor {
    Status Visit(ArrayBuilder* b) { return ConversionError(); }
    Status Visit(TimestampOrStringBuilder* b) { return b->Append(util::string_view(data_, size_)); }
    const char* data_;
    rapidjson::SizeType size_;
  };

  MemoryPool* pool_;
  ArrayBuilder* builder_;
  Status status_;
  std::vector<ArrayBuilder*> builder_stack_;
  std::vector<std::vector<bool>> absent_fields_;
  std::vector<int> field_index_stack_;
};

template <unsigned Flags, typename Handler>
Status BlockParser::DoParse(Handler& handler, const char* data, uint32_t size, uint32_t* out_size) {
  rapidjson::GenericInsituStringStream<rapidjson::UTF8<>> ss(
      const_cast<char*>(data));
  rapidjson::Reader reader;

  for (num_rows_ = 0; num_rows_ != max_num_rows_; ++num_rows_) {
    // parse a single line of JSON
    auto ok = reader.Parse<Flags>(ss, handler);
    switch (ok.Code()) {
      case rapidjson::kParseErrorNone:
        // parse the next object
        continue;
      case rapidjson::kParseErrorDocumentEmpty: {
        // parsed all objects, finish
        *out_size = static_cast<uint32_t>(ss.Tell());
        return Status::OK();
      }
      case rapidjson::kParseErrorTermination:
        // handler emitted an error
        return handler.Error();
      default:
        // rapidjson emitted an error
        return ParseError() << rapidjson::GetParseError_En(ok.Code());
    }
  }
  return Status::Invalid("Exceeded maximum rows");
}

Status BlockParser::Parse(const char* data, uint32_t size, uint32_t* out_size) {
  constexpr unsigned parse_flags =
      rapidjson::kParseInsituFlag | rapidjson::kParseIterativeFlag |
      rapidjson::kParseStopWhenDoneFlag | rapidjson::kParseNumbersAsStringsFlag;

  std::unique_ptr<ArrayBuilder> root_builder;
  if (options_.explicit_schema) {
    auto struct_type = struct_(options_.explicit_schema->fields());
    ARROW_RETURN_NOT_OK(MakeBuilder(pool_, struct_type, &root_builder));
    TypedHandler handler(static_cast<StructBuilder*>(root_builder.get()));
    ARROW_RETURN_NOT_OK(DoParse<parse_flags>(handler, data, size, out_size));
  } else {
    root_builder.reset(new AdaptiveStructBuilder(pool_));
    InferringHandler handler(pool_, static_cast<AdaptiveStructBuilder*>(root_builder.get()));
    ARROW_RETURN_NOT_OK(DoParse<parse_flags>(handler, data, size, out_size));
  }

  std::shared_ptr<Array> array;
  ARROW_RETURN_NOT_OK(root_builder->Finish(&array));
  auto struct_array = std::static_pointer_cast<StructArray>(array);
  
  std::shared_ptr<Schema> root_schema;
  if (options_.explicit_schema) {
    root_schema = options_.explicit_schema;
  } else {
    root_schema = schema(root_builder->type()->children());
  }

  std::vector<std::shared_ptr<Array>> columns;
  for (int i = 0; i != root_schema->num_fields(); ++i) {
    columns.push_back(struct_array->field(i));
  }
  parsed_ = RecordBatch::Make(root_schema, num_rows_, std::move(columns));
  return Status::OK();
}

BlockParser::BlockParser(MemoryPool* pool, ParseOptions options,
                         int32_t num_cols, int32_t max_num_rows)
    : pool_(pool),
      options_(options),
      num_cols_(num_cols),
      max_num_rows_(max_num_rows) {}

BlockParser::BlockParser(ParseOptions options, int32_t num_cols,
                         int32_t max_num_rows)
    : BlockParser(default_memory_pool(), options, num_cols, max_num_rows) {}

}  // namespace json
}  // namespace arrow
