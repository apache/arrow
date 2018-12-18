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
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace json {

using internal::checked_cast;
using internal::StringConverter;
using util::string_view;

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
        status_ = VisitBuilder(parent->field_builder(field_index), AppendNullVisitor{});
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
      RETURN_NOT_OK(b->AppendNull());
      for (int i = 0; i != b->num_fields(); ++i) {
        auto field_builder = b->field_builder(i);
        RETURN_NOT_OK(VisitBuilder(field_builder, AppendNullVisitor()));
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
      RETURN_NOT_OK(ConvertTo<Int64Type>(&value));
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
      RETURN_NOT_OK(ConvertTo<Repr>(&value));
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

/// template <typename B>
/// concept JsonAdaptiveArrayBuilder = DerivedFrom<B, AdaptiveArrayBuilder> && requires
/// {
///   // each builder defines a value_type
///   typename B::value_type;
///
///   // builders begin at a known type when constructed
///   { B::initial_type() }
///   -> std::shared_ptr<DataType>
///
///   // each builder has a factory taking a memory pool and a leading null count
///   requires(MemoryPool * pool, int64_t leading_nulls) {
///     { B::Make(pool, leading_nulls) }
///     ->Status;
///   }
///
///   // builders have uniformly named append methods for null and otherwise
///   requires(B b, typename B::value_type value) {
///     { b.AppendNull() }
///     ->Status;
///     { b.Append(value) }
///     ->Status;
///   }
/// };

class AdaptiveNullBuilder;
class AdaptiveBooleanBuilder;
class Int64OrDoubleBuilder;
class TimestampOrStringBuilder;
class AdaptiveStructBuilder;
class AdaptiveListBuilder;

template <typename T, typename... A>
std::unique_ptr<T> make_unique(A&&... args) {
  return std::unique_ptr<T>(new T(std::forward<A>(args)...));
}

class AdaptiveNullBuilder : public AdaptiveArrayBuilder {
 public:
  struct value_type {};

  AdaptiveNullBuilder(MemoryPool* pool)
      : AdaptiveArrayBuilder(initial_type()), pool_(pool) {}

  Status Append(value_type) { return AppendNull(); }

  Status AppendNull() {
    ++length_;
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    *out = std::make_shared<NullArray>(length_);
    return Status::OK();
  }

  Status MaybePromoteTo(std::shared_ptr<DataType> type,
                        std::unique_ptr<AdaptiveArrayBuilder>* out) override;

  static std::shared_ptr<DataType> initial_type() { return null(); }

  static Status Make(MemoryPool* pool, int64_t leading_nulls,
                     std::unique_ptr<AdaptiveArrayBuilder>* out) {
    *out = make_unique<AdaptiveNullBuilder>(pool);
    auto builder = static_cast<AdaptiveNullBuilder*>(out->get());
    builder->length_ = leading_nulls;
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
};

class AdaptiveBooleanBuilder : public AdaptiveArrayBuilder {
 public:
  using value_type = bool;

  AdaptiveBooleanBuilder(MemoryPool* pool)
      : AdaptiveArrayBuilder(initial_type()), boolean_builder_(pool) {}

  Status Append(bool value) {
    ++length_;
    return boolean_builder_.Append(value);
  }

  Status AppendNull() {
    ++length_;
    return boolean_builder_.AppendNull();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    return boolean_builder_.Finish(out);
  }

  Status MaybePromoteTo(std::shared_ptr<DataType> type,
                        std::unique_ptr<AdaptiveArrayBuilder>*) override {
    if (type->Equals(type_)) return Status::OK();
    return ConversionError();
  }

  static std::shared_ptr<DataType> initial_type() { return boolean(); }

  static Status Make(MemoryPool* pool, int64_t leading_nulls,
                     std::unique_ptr<AdaptiveArrayBuilder>* out) {
    *out = make_unique<AdaptiveBooleanBuilder>(pool);
    auto builder = static_cast<AdaptiveBooleanBuilder*>(out->get());
    builder->length_ = leading_nulls;
    return builder->boolean_builder_.SetLeadingNulls(leading_nulls);
  }

 private:
  struct WrappedBuilder : BooleanBuilder {
    using BooleanBuilder::BooleanBuilder;

    Status SetLeadingNulls(int64_t leading_nulls) {
      ARROW_CHECK(length_ == 0);
      null_count_ = leading_nulls;
      length_ = leading_nulls;
      return Resize(leading_nulls);
    }
  } boolean_builder_;
};

class Int64OrDoubleBuilder : public AdaptiveArrayBuilder {
 public:
  using value_type = string_view;

  Int64OrDoubleBuilder(MemoryPool* pool)
      : AdaptiveArrayBuilder(initial_type()), bytes_builder_(pool) {}

  Status Append(string_view repr) {
    ++length_;
    if (failed_conversion_to_int_ < 0) {
      StringConverter<Int64Type> converter;
      int64_t value;
      if (converter(repr.data(), repr.size(), &value)) {
        return bytes_builder_.Append(reinterpret_cast<const char*>(&value));
      }
      PromoteToDouble();
    }
    StringConverter<DoubleType> converter;
    double value;
    if (converter(repr.data(), repr.size(), &value)) {
      return bytes_builder_.Append(reinterpret_cast<const char*>(&value));
    }
    return ConversionError();
  }

  Status AppendNull() {
    ++length_;
    return bytes_builder_.AppendNull();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> bytes_array;
    RETURN_NOT_OK(bytes_builder_.Finish(&bytes_array));
    auto data = bytes_array->data()->Copy();
    if (failed_conversion_to_int_ > leading_nulls_) {
      // convert any ints to double
      auto doubles = data->GetMutableValues<double>(1);
      auto ints = data->GetValues<int64_t>(1);
      for (int64_t i = leading_nulls_; i != failed_conversion_to_int_; ++i) {
        doubles[i] = static_cast<double>(ints[i]);
      }
    }
    data->type = type_;
    *out = MakeArray(data);
    return Status::OK();
  }

  Status MaybePromoteTo(std::shared_ptr<DataType> type,
                        std::unique_ptr<AdaptiveArrayBuilder>*) override {
    if (type->Equals(type_)) return Status::OK();
    if (type->id() == Type::DOUBLE) {
      PromoteToDouble();
      return Status::OK();
    }
    return ConversionError();
  }

  void PromoteToDouble() {
    if (failed_conversion_to_int_ < 0) {
      failed_conversion_to_int_ = bytes_builder_.length();
      type_ = float64();
    }
  }

  static std::shared_ptr<DataType> initial_type() { return int64(); }

  static Status Make(MemoryPool* pool, int64_t leading_nulls,
                     std::unique_ptr<AdaptiveArrayBuilder>* out) {
    *out = make_unique<Int64OrDoubleBuilder>(pool);
    auto builder = static_cast<Int64OrDoubleBuilder*>(out->get());
    builder->leading_nulls_ = leading_nulls;
    builder->length_ = leading_nulls;
    return builder->bytes_builder_.SetLeadingNulls(leading_nulls);
  }

 private:
  struct WrappedBuilder : public FixedSizeBinaryBuilder {
    WrappedBuilder(MemoryPool* pool)
        : FixedSizeBinaryBuilder(fixed_size_binary(sizeof(int64_t)), pool) {}

    Status SetLeadingNulls(int64_t leading_nulls) {
      ARROW_CHECK(length_ == 0);
      null_count_ = leading_nulls;
      length_ = leading_nulls;
      RETURN_NOT_OK(byte_builder_.Advance(sizeof(int64_t) * leading_nulls));
      return Resize(leading_nulls);
    }

  } bytes_builder_;
  int64_t failed_conversion_to_int_ = -1;
  int64_t leading_nulls_ = 0;
};

class TimestampOrStringBuilder : public AdaptiveArrayBuilder {
 public:
  using value_type = string_view;

  TimestampOrStringBuilder(MemoryPool* pool)
      : AdaptiveArrayBuilder(initial_type()),
        string_builder_(pool),
        timestamp_builder_(pool) {}

  Status Append(string_view str) {
    ++length_;
    if (all_timestamps_) {
      StringConverter<TimestampType> converter(type_);
      int64_t value;
      if (converter(str.data(), str.size(), &value)) {
        RETURN_NOT_OK(timestamp_builder_.Append(value));
      } else
        PromoteToString();
    }
    return string_builder_.Append(str);
  }

  Status AppendNull() {
    ++length_;
    if (all_timestamps_) {
      RETURN_NOT_OK(timestamp_builder_.Append(0));
    }
    return string_builder_.AppendNull();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> strings_array;
    RETURN_NOT_OK(string_builder_.Finish(&strings_array));
    if (!all_timestamps_) {
      *out = strings_array;
      return Status::OK();
    }
    std::shared_ptr<Buffer> timestamps;
    RETURN_NOT_OK(timestamp_builder_.Finish(&timestamps));
    auto data = strings_array->data()->Copy();
    data->type = type_;
    data->buffers = {data->buffers[0], timestamps};
    *out = MakeArray(data);
    return Status::OK();
  }

  Status MaybePromoteTo(std::shared_ptr<DataType> type,
                        std::unique_ptr<AdaptiveArrayBuilder>*) override {
    if (type->Equals(type_)) return Status::OK();
    if (all_timestamps_ && type->id() == Type::STRING) {
      PromoteToString();
      return Status::OK();
    }
    return ConversionError();
  }

  void PromoteToString() {
    all_timestamps_ = false;
    type_ = utf8();
    timestamp_builder_.Reset();  // we don't need to store timestamps now
  }

  static std::shared_ptr<DataType> initial_type() { return timestamp(TimeUnit::SECOND); }

  static Status Make(MemoryPool* pool, int64_t leading_nulls,
                     std::unique_ptr<AdaptiveArrayBuilder>* out) {
    *out = make_unique<TimestampOrStringBuilder>(pool);
    auto builder = static_cast<TimestampOrStringBuilder*>(out->get());
    builder->length_ = leading_nulls;
    RETURN_NOT_OK(builder->string_builder_.SetLeadingNulls(leading_nulls));
    RETURN_NOT_OK(builder->timestamp_builder_.Resize(sizeof(int64_t) * leading_nulls));
    return Status::OK();
  }

 private:
  struct WrappedBuilder : public StringBuilder {
    WrappedBuilder(MemoryPool* pool) : StringBuilder(pool) {}

    Status SetLeadingNulls(int64_t leading_nulls) {
      ARROW_CHECK(length_ == 0);
      null_count_ = leading_nulls;
      length_ = leading_nulls;
      return Resize(leading_nulls);
    }

  } string_builder_;
  TypedBufferBuilder<int64_t> timestamp_builder_;
  bool all_timestamps_ = true;
};

class AdaptiveStructBuilder : public AdaptiveArrayBuilder {
 public:
  struct value_type {};

  AdaptiveStructBuilder(MemoryPool* pool)
      : AdaptiveArrayBuilder(initial_type()), bitmap_builder_(pool), pool_(pool) {}

  Status Append(value_type) {
    ++length_;
    return bitmap_builder_.AppendToBitmap(true);
  }

  Status AppendNull() {
    ++length_;
    return bitmap_builder_.AppendToBitmap(false);
  }

  Status UpdateType() override {
    RETURN_NOT_OK(SortFieldsByName());
    return SetTypeFromFieldBuilders();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    RETURN_NOT_OK(UpdateType());
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(bitmap_builder_.FinishInternal(&data));
    data->type = type_;
    for (auto&& builder : field_builders_) {
      std::shared_ptr<Array> field_array;
      RETURN_NOT_OK(builder->Finish(&field_array));
      data->child_data.push_back(field_array->data());
    }
    *out = MakeArray(data);
    return Status::OK();
  }

  Status MaybePromoteTo(std::shared_ptr<DataType> type,
                        std::unique_ptr<AdaptiveArrayBuilder>*) override {
    RETURN_NOT_OK(UpdateType());
    if (type->Equals(type_)) return Status::OK();
    if (type->id() != Type::STRUCT) return ConversionError();
    using field_ref = const std::shared_ptr<Field>&;
    ARROW_CHECK(
        std::is_sorted(type->children().begin(), type->children().end(),
                       [](field_ref l, field_ref r) { return l->name() < r->name(); }));
    for (const auto& other_field : type->children()) {
      auto index = GetFieldIndex(other_field->name(), false);
      std::unique_ptr<AdaptiveArrayBuilder> replacement;
      RETURN_NOT_OK(
          field_builders_[index]->MaybePromoteTo(other_field->type(), &replacement));
      if (replacement) {
        SetFieldBuilder(index, std::move(replacement));
      }
    }
    return UpdateType();
  }

  // If a field with this name does not yet exist insert an AdaptiveNullBuilder,
  // constructed with length equal to this struct builder's or one less if the
  // child builder will be appended to next
  int GetFieldIndex(const std::string& name, bool will_append) {
    auto it = name_to_index_.find(name);
    if (it != name_to_index_.end()) {
      return it->second;
    }
    auto index = static_cast<int>(field_builders_.size());
    name_to_index_[name] = index;
    std::unique_ptr<AdaptiveArrayBuilder> null_builder;
    auto leading_nulls = will_append ? length_ - 1 : length_;
    ARROW_IGNORE_EXPR(AdaptiveNullBuilder::Make(pool_, leading_nulls, &null_builder));
    field_builders_.push_back(std::move(null_builder));
    return index;
  }

  // Assumes that new builder is initialized with the correct element count
  void SetFieldBuilder(int index, std::unique_ptr<AdaptiveArrayBuilder> builder) {
    field_builders_[index] = std::move(builder);
  }

  AdaptiveArrayBuilder* field_builder(int index) { return field_builders_[index].get(); }

  int num_fields() { return static_cast<int>(field_builders_.size()); }

  static std::shared_ptr<DataType> initial_type() { return struct_({}); }

  static Status Make(MemoryPool* pool, int64_t leading_nulls,
                     std::unique_ptr<AdaptiveArrayBuilder>* out) {
    *out = make_unique<AdaptiveStructBuilder>(pool);
    auto builder = static_cast<AdaptiveStructBuilder*>(out->get());
    builder->length_ = leading_nulls;
    return builder->bitmap_builder_.SetLeadingNulls(leading_nulls);
  }

 private:
  Status SortFieldsByName() {
    struct zip_t {
      std::unique_ptr<AdaptiveArrayBuilder> builder;
      std::string name;
    };
    std::vector<zip_t> fields_and_builders;
    fields_and_builders.reserve(num_fields());
    for (auto&& name_index : name_to_index_) {
      auto builder = std::move(field_builders_[name_index.second]);
      fields_and_builders.push_back({std::move(builder), name_index.first});
    }
    std::sort(fields_and_builders.begin(), fields_and_builders.end(),
              [](const zip_t& l, const zip_t& r) { return l.name < r.name; });
    int index = 0;
    for (auto&& zip : fields_and_builders) {
      field_builders_[index] = std::move(zip.builder);
      name_to_index_[zip.name] = index;
      ++index;
    }
    return Status::OK();
  }

  Status SetTypeFromFieldBuilders() {
    std::vector<std::shared_ptr<Field>> fields(field_builders_.size());
    for (auto&& name_index : name_to_index_) {
      auto builder = field_builder(name_index.second);
      RETURN_NOT_OK(builder->UpdateType());
      fields[name_index.second] = field(name_index.first, builder->type());
    }
    type_ = struct_(std::move(fields));
    return Status::OK();
  }

  // FIXME replace this with TypedBufferBuilder<bool>
  struct WrappedBuilder : public ArrayBuilder {
    WrappedBuilder(MemoryPool* pool) : ArrayBuilder(null(), pool) {}

    Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
      *out = ArrayData::Make(type_, length_, {null_bitmap_}, null_count_);
      null_bitmap_ = nullptr;
      capacity_ = length_ = null_count_ = 0;
      return Status::OK();
    }

    using ArrayBuilder::AppendToBitmap;

    Status SetLeadingNulls(int64_t leading_nulls) {
      ARROW_CHECK(length_ == 0);
      null_count_ = leading_nulls;
      length_ = leading_nulls;
      return Resize(leading_nulls);
    }

  } bitmap_builder_;
  MemoryPool* pool_;
  std::vector<std::unique_ptr<AdaptiveArrayBuilder>> field_builders_;
  std::unordered_map<std::string, int> name_to_index_;
};

class AdaptiveListBuilder : public AdaptiveArrayBuilder {
 public:
  struct value_type {};

  AdaptiveListBuilder(MemoryPool* pool)
      : AdaptiveArrayBuilder(initial_type()),
        bitmap_builder_(pool),
        offsets_builder_(pool) {}

  Status Append(value_type) {
    RETURN_NOT_OK(bitmap_builder_.AppendToBitmap(true));
    return AppendNextOffset();
  }

  Status AppendNull() {
    RETURN_NOT_OK(bitmap_builder_.AppendToBitmap(false));
    return AppendNextOffset();
  }

  Status UpdateType() override {
    RETURN_NOT_OK(value_builder_->UpdateType());
    type_ = list(value_builder_->type());
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    RETURN_NOT_OK(AppendNextOffset());
    RETURN_NOT_OK(UpdateType());
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(bitmap_builder_.FinishInternal(&data));
    data->type = type_;
    std::shared_ptr<Buffer> offsets;
    RETURN_NOT_OK(offsets_builder_.Finish(&offsets));
    data->buffers.push_back(offsets);
    std::shared_ptr<Array> values_array;
    RETURN_NOT_OK(value_builder_->Finish(&values_array));
    data->child_data = {values_array->data()};
    *out = MakeArray(data);
    return Status::OK();
  }

  Status MaybePromoteTo(std::shared_ptr<DataType> type,
                        std::unique_ptr<AdaptiveArrayBuilder>*) override {
    RETURN_NOT_OK(UpdateType());
    if (type->Equals(type_)) return Status::OK();
    if (type->id() != Type::LIST) return ConversionError();
    auto list_type = std::static_pointer_cast<ListType>(type);
    std::unique_ptr<AdaptiveArrayBuilder> replacement;
    RETURN_NOT_OK(value_builder_->MaybePromoteTo(list_type->value_type(), &replacement));
    if (replacement) {
      SetValueBuilder(std::move(replacement));
    }
    return UpdateType();
  }

  AdaptiveArrayBuilder* value_builder() { return value_builder_.get(); }

  // Assumes that new builder is initialized with the correct element count
  void SetValueBuilder(std::unique_ptr<AdaptiveArrayBuilder> builder) {
    value_builder_ = std::move(builder);
  }

  static std::shared_ptr<DataType> initial_type() { return list(null()); }

  static Status Make(MemoryPool* pool, int64_t leading_nulls,
                     std::unique_ptr<AdaptiveArrayBuilder>* out) {
    *out = make_unique<AdaptiveListBuilder>(pool);
    auto builder = static_cast<AdaptiveListBuilder*>(out->get());
    builder->length_ = leading_nulls;
    RETURN_NOT_OK(AdaptiveNullBuilder::Make(pool, 0, &builder->value_builder_));
    RETURN_NOT_OK(builder->offsets_builder_.Resize(leading_nulls));
    return builder->bitmap_builder_.SetLeadingNulls(leading_nulls);
  }

 private:
  Status AppendNextOffset() {
    ++length_;
    int64_t num_values = value_builder_->length();
    if (ARROW_PREDICT_FALSE(num_values > kListMaximumElements)) {
      std::stringstream ss;
      ss << "ListArray cannot contain more than INT32_MAX - 1 child elements,"
         << " but " << num_values << " were inserted";
      return Status::CapacityError(ss.str());
    }
    return offsets_builder_.Append(static_cast<int32_t>(num_values));
  }

  // FIXME replace this with TypedBufferBuilder<bool>
  struct WrappedBuilder : public ArrayBuilder {
    WrappedBuilder(MemoryPool* pool) : ArrayBuilder(null(), pool) {}

    Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
      *out = ArrayData::Make(type_, length_, {null_bitmap_}, null_count_);
      null_bitmap_ = nullptr;
      capacity_ = length_ = null_count_ = 0;
      return Status::OK();
    }

    using ArrayBuilder::AppendToBitmap;

    Status SetLeadingNulls(int64_t leading_nulls) {
      ARROW_CHECK(length_ == 0);
      null_count_ = leading_nulls;
      length_ = leading_nulls;
      return Resize(leading_nulls);
    }

  } bitmap_builder_;

  TypedBufferBuilder<int32_t> offsets_builder_;
  std::unique_ptr<AdaptiveArrayBuilder> value_builder_;
  std::unordered_map<std::string, int> name_to_index_;
};

Status AdaptiveNullBuilder::MaybePromoteTo(std::shared_ptr<DataType> type,
                                           std::unique_ptr<AdaptiveArrayBuilder>* out) {
  struct {
    Status Visit(const NullType&) { return Status::OK(); }
    Status Visit(const BooleanType&) {
      return AdaptiveBooleanBuilder::Make(pool_, length_, out);
    }
    Status Visit(const Int64Type&) {
      return Int64OrDoubleBuilder::Make(pool_, length_, out);
    }
    Status Visit(const DoubleType&) {
      RETURN_NOT_OK(Int64OrDoubleBuilder::Make(pool_, length_, out));
      static_cast<Int64OrDoubleBuilder*>(out->get())->PromoteToDouble();
      return Status::OK();
    }
    Status Visit(const TimestampType&) {
      return TimestampOrStringBuilder::Make(pool_, length_, out);
    }
    Status Visit(const StringType&) {
      RETURN_NOT_OK(TimestampOrStringBuilder::Make(pool_, length_, out));
      static_cast<TimestampOrStringBuilder*>(out->get())->PromoteToString();
      return Status::OK();
    }
    Status Visit(const StructType&) {
      return AdaptiveStructBuilder::Make(pool_, length_, out);
    }
    Status Visit(const ListType&) {
      return AdaptiveListBuilder::Make(pool_, length_, out);
    }
    Status Visit(const DataType&) {
      ARROW_LOG(FATAL) << "How did we get here";
      return Status::Invalid("How did we get here");
    }

    MemoryPool* pool_;
    int64_t length_;
    std::unique_ptr<AdaptiveArrayBuilder>* out;
  } visitor{pool_, length_, out};

  return VisitTypeInline(*type, &visitor);
}

class InferringHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, InferringHandler> {
 public:
  InferringHandler(AdaptiveStructBuilder* root_builder) : builder_(root_builder) {}

  bool Null() {
    status_ = VisitBuilder(builder_, AppendNullVisitor{});
    return status_.ok();
  }

  bool Bool(bool value) {
    status_ = VisitBuilder(builder_, AppendVisitor<AdaptiveBooleanBuilder>{this, value});
    return status_.ok();
  }

  bool RawNumber(const char* data, rapidjson::SizeType size, bool) {
    string_view value(data, size);
    status_ = VisitBuilder(builder_, AppendVisitor<Int64OrDoubleBuilder>{this, value});
    return status_.ok();
  }

  bool String(const char* data, rapidjson::SizeType size, bool) {
    string_view value(data, size);
    status_ =
        VisitBuilder(builder_, AppendVisitor<TimestampOrStringBuilder>{this, value});
    return status_.ok();
  }

  bool StartObject() {
    status_ = VisitBuilder(builder_, AppendVisitor<AdaptiveStructBuilder>{this, {}});
    if (!status_.ok()) return false;
    auto parent = static_cast<AdaptiveStructBuilder*>(builder_);
    builder_stack_.push_back(builder_);
    absent_fields_.push_back(std::vector<bool>(parent->num_fields(), true));
    field_index_stack_.push_back(-1);  // overwritten in Key()
    return true;
  }

  bool Key(const char* key, rapidjson::SizeType len, bool) {
    auto parent = static_cast<AdaptiveStructBuilder*>(builder_stack_.back());
    std::string name(key, len);
    int field_index = parent->GetFieldIndex(name, true);
    field_index_stack_.back() = field_index;
    if (field_index < absent_fields_.back().size()) {
      absent_fields_.back()[field_index] = false;
    }
    builder_ = parent->field_builder(field_index);
    return true;
  }

  bool EndObject(...) {
    int field_index = 0;
    auto parent = static_cast<AdaptiveStructBuilder*>(builder_stack_.back());
    for (bool null : absent_fields_.back()) {
      // TODO since this is expected to be sparse, it would probably be more
      // efficient to use CountLeadingZeros() to find the indices of the few
      // null fields
      if (null) {
        status_ = VisitBuilder(parent->field_builder(field_index), AppendNullVisitor{});
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
    status_ = VisitBuilder(builder_, AppendVisitor<AdaptiveListBuilder>{this, {}});
    if (!status_.ok()) return false;
    auto parent = static_cast<AdaptiveListBuilder*>(builder_);
    builder_stack_.push_back(builder_);
    builder_ = parent->value_builder();
    field_index_stack_.push_back(-1);
    return true;
  }

  bool EndArray(...) {
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
    field_index_stack_.pop_back();
    return true;
  }

  Status Error() { return status_; }

 private:
  struct AppendNullVisitor {
    template <typename Builder>
    Status Visit(Builder* b, ...) {
      return b->AppendNull();
    }
  };

  template <typename Builder>
  struct AppendVisitor {
    using value_type = typename Builder::value_type;

    // visiting the specified builder class; we know how to append value_
    Status Visit(Builder* b, std::unique_ptr<AdaptiveArrayBuilder>*) {
      return b->Append(value_);
    }

    // anything else -> try promotion
    // it's alright that MaybePromoteTo is virtual here; we'll hit this seldom
    Status Visit(AdaptiveArrayBuilder* b, std::unique_ptr<AdaptiveArrayBuilder>* new_b) {
      RETURN_NOT_OK(b->MaybePromoteTo(Builder::initial_type(), new_b));
      auto target = *new_b ? new_b->get()  // promotion has moved builder from b to new_b
                           : b;  // promotion handled internally; b is still valid
      return this_->VisitBuilder(target, *this);
    }

    InferringHandler* this_;
    value_type value_;
  };

  template <typename Visitor>
  Status VisitBuilder(AdaptiveArrayBuilder* builder, Visitor&& visitor) {
    std::unique_ptr<AdaptiveArrayBuilder> replacement;
    switch (builder->type()->id()) {
      case Type::NA:
        RETURN_NOT_OK(
            visitor.Visit(static_cast<AdaptiveNullBuilder*>(builder), &replacement));
        break;
      case Type::BOOL:
        RETURN_NOT_OK(
            visitor.Visit(static_cast<AdaptiveBooleanBuilder*>(builder), &replacement));
        break;
      case Type::INT64:
      case Type::DOUBLE:
        RETURN_NOT_OK(
            visitor.Visit(static_cast<Int64OrDoubleBuilder*>(builder), &replacement));
        break;
      case Type::TIMESTAMP:
      case Type::STRING:
        RETURN_NOT_OK(
            visitor.Visit(static_cast<TimestampOrStringBuilder*>(builder), &replacement));
        break;
      case Type::STRUCT:
        RETURN_NOT_OK(
            visitor.Visit(static_cast<AdaptiveStructBuilder*>(builder), &replacement));
        break;
      case Type::LIST:
        RETURN_NOT_OK(
            visitor.Visit(static_cast<AdaptiveListBuilder*>(builder), &replacement));
        break;
      default:
        ARROW_LOG(FATAL) << "How did we get here";
        return Status::Invalid("How did we get here");
    };

    if (replacement) {
      builder_ = replacement.get();
      int field_index = field_index_stack_.back();
      if (field_index == -1) {
        auto parent = static_cast<AdaptiveListBuilder*>(builder_stack_.back());
        parent->SetValueBuilder(std::move(replacement));
      } else {
        auto parent = static_cast<AdaptiveStructBuilder*>(builder_stack_.back());
        parent->SetFieldBuilder(field_index, std::move(replacement));
      }
    }

    return Status::OK();
  }

  AdaptiveArrayBuilder* builder_;
  Status status_;
  std::vector<AdaptiveArrayBuilder*> builder_stack_;
  std::vector<std::vector<bool>> absent_fields_;
  std::vector<int> field_index_stack_;
};

template <unsigned Flags, typename Handler>
Status BlockParser::DoParse(Handler& handler, string_view json) {
  rapidjson::GenericInsituStringStream<rapidjson::UTF8<>> ss(
      const_cast<char*>(json.data()));
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

Status BlockParser::Parse(string_view json) {
  constexpr unsigned parse_flags =
      rapidjson::kParseInsituFlag | rapidjson::kParseIterativeFlag |
      rapidjson::kParseStopWhenDoneFlag | rapidjson::kParseNumbersAsStringsFlag;

  std::shared_ptr<Array> array;
  if (options_.explicit_schema) {
    auto struct_type = struct_(options_.explicit_schema->fields());
    std::unique_ptr<ArrayBuilder> root_builder;
    RETURN_NOT_OK(MakeBuilder(pool_, struct_type, &root_builder));
    TypedHandler handler(static_cast<StructBuilder*>(root_builder.get()));
    RETURN_NOT_OK(DoParse<parse_flags>(handler, json));
    RETURN_NOT_OK(root_builder->Finish(&array));
  } else {
    auto root_builder = make_unique<AdaptiveStructBuilder>(pool_);
    InferringHandler handler(root_builder.get());
    RETURN_NOT_OK(DoParse<parse_flags>(handler, json));
    RETURN_NOT_OK(root_builder->Finish(&array));
  }

  auto struct_array = std::static_pointer_cast<StructArray>(array);

  std::shared_ptr<Schema> root_schema;
  if (options_.explicit_schema) {
    root_schema = options_.explicit_schema;
  } else {
    root_schema = schema(array->type()->children());
  }

  std::vector<std::shared_ptr<Array>> columns;
  for (int i = 0; i != root_schema->num_fields(); ++i) {
    columns.push_back(struct_array->field(i));
  }
  parsed_ = RecordBatch::Make(root_schema, num_rows_, std::move(columns));
  return Status::OK();
}

BlockParser::BlockParser(MemoryPool* pool, ParseOptions options, int32_t num_cols,
                         int32_t max_num_rows)
    : pool_(pool), options_(options), num_cols_(num_cols), max_num_rows_(max_num_rows) {}

BlockParser::BlockParser(ParseOptions options, int32_t num_cols, int32_t max_num_rows)
    : BlockParser(default_memory_pool(), options, num_cols, max_num_rows) {}

}  // namespace json
}  // namespace arrow
