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

#include <limits>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/json/rapidjson-defs.h"
#include "rapidjson/error/en.h"
#include "rapidjson/reader.h"

#include "arrow/array.h"
#include "arrow/buffer-builder.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/util/string_view.h"
#include "arrow/util/trie.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace json {

namespace rj = arrow::rapidjson;

using internal::BitsetStack;
using internal::checked_cast;
using internal::make_unique;
using util::string_view;

template <typename... T>
static Status ParseError(T&&... t) {
  return Status::Invalid("JSON parse error: ", std::forward<T>(t)...);
}

static Status KindChangeError(Kind::type from, Kind::type to) {
  return ParseError("A column changed from ", Kind::Name(from), " to ", Kind::Name(to));
}

const std::string& Kind::Name(Kind::type kind) {
  static const std::string names[] = {"null",   "boolean", "number",
                                      "string", "array",   "object"};

  return names[kind];
}

const std::shared_ptr<const KeyValueMetadata>& Kind::Tag(Kind::type kind) {
  static const std::shared_ptr<const KeyValueMetadata> tags[] = {
      key_value_metadata({{"json_kind", Kind::Name(Kind::kNull)}}),
      key_value_metadata({{"json_kind", Kind::Name(Kind::kBoolean)}}),
      key_value_metadata({{"json_kind", Kind::Name(Kind::kNumber)}}),
      key_value_metadata({{"json_kind", Kind::Name(Kind::kString)}}),
      key_value_metadata({{"json_kind", Kind::Name(Kind::kArray)}}),
      key_value_metadata({{"json_kind", Kind::Name(Kind::kObject)}}),
  };
  return tags[kind];
}

static internal::Trie MakeFromTagTrie() {
  internal::TrieBuilder builder;
  for (auto kind : {Kind::kNull, Kind::kBoolean, Kind::kNumber, Kind::kString,
                    Kind::kArray, Kind::kObject}) {
    DCHECK_OK(builder.Append(Kind::Name(kind)));
  }
  auto name_to_kind = builder.Finish();
  DCHECK_OK(name_to_kind.Validate());
  return name_to_kind;
}

Kind::type Kind::FromTag(const std::shared_ptr<const KeyValueMetadata>& tag) {
  static internal::Trie name_to_kind = MakeFromTagTrie();
  DCHECK_NE(tag->FindKey("json_kind"), -1);
  util::string_view name = tag->value(tag->FindKey("json_kind"));
  DCHECK_NE(name_to_kind.Find(name), -1);
  return static_cast<Kind::type>(name_to_kind.Find(name));
}

Status Kind::ForType(const DataType& type, Kind::type* kind) {
  struct {
    Status Visit(const NullType&) { return SetKind(Kind::kNull); }
    Status Visit(const BooleanType&) { return SetKind(Kind::kBoolean); }
    Status Visit(const Number&) { return SetKind(Kind::kNumber); }
    Status Visit(const TimeType&) { return SetKind(Kind::kNumber); }
    Status Visit(const DateType&) { return SetKind(Kind::kNumber); }
    Status Visit(const BinaryType&) { return SetKind(Kind::kString); }
    Status Visit(const FixedSizeBinaryType&) { return SetKind(Kind::kString); }
    Status Visit(const DictionaryType& dict_type) {
      return Kind::ForType(*dict_type.dictionary()->type(), kind_);
    }
    Status Visit(const ListType&) { return SetKind(Kind::kArray); }
    Status Visit(const StructType&) { return SetKind(Kind::kObject); }
    Status Visit(const DataType& not_impl) {
      return Status::NotImplemented("JSON parsing of ", not_impl);
    }
    Status SetKind(Kind::type kind) {
      *kind_ = kind;
      return Status::OK();
    }
    Kind::type* kind_;
  } visitor = {kind};
  return VisitTypeInline(type, &visitor);
}

/// \brief ArrayBuilder for parsed but unconverted arrays
template <Kind::type>
class RawArrayBuilder;

/// \brief packed pointer to a RawArrayBuilder
///
/// RawArrayBuilders are stored in HandlerBase,
/// which allows storage of their indices (uint32_t) instead of a full pointer.
/// BuilderPtr is also tagged with the json kind and nullable properties
/// so those can be accessed before dereferencing the builder.
struct BuilderPtr {
  BuilderPtr() : BuilderPtr(BuilderPtr::null) {}
  BuilderPtr(Kind::type k, uint32_t i, bool n) : index(i), kind(k), nullable(n) {}

  BuilderPtr(const BuilderPtr&) = default;
  BuilderPtr& operator=(const BuilderPtr&) = default;
  BuilderPtr(BuilderPtr&&) = default;
  BuilderPtr& operator=(BuilderPtr&&) = default;

  // index of builder in its arena
  // OR the length of that builder if kind == Kind::kNull
  // (we don't allocate an arena for nulls since they're trivial)
  // FIXME(bkietz) GCC is emitting conversion errors for the bitfields
  uint32_t index;  // : 28;
  Kind::type kind;
  bool nullable;

  bool operator==(BuilderPtr other) const {
    return kind == other.kind && index == other.index;
  }

  bool operator!=(BuilderPtr other) const { return !(other == *this); }

  operator bool() const { return *this != null; }

  bool operator!() const { return *this == null; }

  static const BuilderPtr null;
};

const BuilderPtr BuilderPtr::null(Kind::kNull, 0, false);

template <>
class RawArrayBuilder<Kind::kBoolean> {
 public:
  explicit RawArrayBuilder(MemoryPool* pool)
      : data_builder_(pool), null_bitmap_builder_(pool) {}

  Status Append(bool value) {
    RETURN_NOT_OK(data_builder_.Append(value));
    return null_bitmap_builder_.Append(true);
  }

  Status AppendNull() {
    RETURN_NOT_OK(data_builder_.Append(false));
    return null_bitmap_builder_.Append(false);
  }

  Status AppendNull(int64_t count) {
    RETURN_NOT_OK(data_builder_.Append(count, false));
    return null_bitmap_builder_.Append(count, false);
  }

  Status Finish(std::shared_ptr<Array>* out) {
    auto size = length();
    auto null_count = null_bitmap_builder_.false_count();
    std::shared_ptr<Buffer> data, null_bitmap;
    RETURN_NOT_OK(data_builder_.Finish(&data));
    RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
    *out = MakeArray(ArrayData::Make(boolean(), size, {null_bitmap, data}, null_count));
    return Status::OK();
  }

  int64_t length() { return null_bitmap_builder_.length(); }

 private:
  TypedBufferBuilder<bool> data_builder_;
  TypedBufferBuilder<bool> null_bitmap_builder_;
};

/// \brief builder for strings or unconverted numbers
///
/// Both of these are represented in the builder as an index only;
/// the actual characters are stored in a single StringArray (into which
/// an index refers). This means building is faster since we don't do
/// allocation for string/number characters but accessing is strided.
///
/// On completion the indices and the character storage are combined into
/// a DictionaryArray, which is a convenient container for indices referring
/// into another array.
class ScalarBuilder {
 public:
  explicit ScalarBuilder(MemoryPool* pool)
      : data_builder_(pool), null_bitmap_builder_(pool) {}

  Status Append(int32_t index, int32_t value_length) {
    RETURN_NOT_OK(data_builder_.Append(index));
    values_length_ += value_length;
    return null_bitmap_builder_.Append(true);
  }

  Status AppendNull() {
    RETURN_NOT_OK(data_builder_.Append(0));
    return null_bitmap_builder_.Append(false);
  }

  Status AppendNull(int64_t count) {
    RETURN_NOT_OK(data_builder_.Append(count, 0));
    return null_bitmap_builder_.Append(count, false);
  }

  Status Finish(std::shared_ptr<Array>* out) {
    auto size = length();
    auto null_count = null_bitmap_builder_.false_count();
    std::shared_ptr<Buffer> data, null_bitmap;
    RETURN_NOT_OK(data_builder_.Finish(&data));
    RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
    *out = MakeArray(ArrayData::Make(int32(), size, {null_bitmap, data}, null_count));
    return Status::OK();
  }

  int64_t length() { return null_bitmap_builder_.length(); }

  int32_t values_length() { return values_length_; }

 private:
  int32_t values_length_;
  TypedBufferBuilder<int32_t> data_builder_;
  TypedBufferBuilder<bool> null_bitmap_builder_;
};

template <>
class RawArrayBuilder<Kind::kNumber> : public ScalarBuilder {
 public:
  using ScalarBuilder::ScalarBuilder;
};

template <>
class RawArrayBuilder<Kind::kString> : public ScalarBuilder {
 public:
  using ScalarBuilder::ScalarBuilder;
};

template <>
class RawArrayBuilder<Kind::kArray> {
 public:
  explicit RawArrayBuilder(MemoryPool* pool)
      : offset_builder_(pool), null_bitmap_builder_(pool) {}

  Status Append(int32_t child_length) {
    RETURN_NOT_OK(offset_builder_.Append(offset_));
    offset_ += child_length;
    return null_bitmap_builder_.Append(true);
  }

  Status AppendNull() {
    RETURN_NOT_OK(offset_builder_.Append(offset_));
    return null_bitmap_builder_.Append(false);
  }

  Status AppendNull(int64_t count) {
    RETURN_NOT_OK(offset_builder_.Append(count, offset_));
    return null_bitmap_builder_.Append(count, false);
  }

  template <typename HandlerBase>
  Status Finish(HandlerBase& handler, std::shared_ptr<Array>* out) {
    RETURN_NOT_OK(offset_builder_.Append(offset_));
    auto size = length();
    auto null_count = null_bitmap_builder_.false_count();
    std::shared_ptr<Buffer> offsets, null_bitmap;
    RETURN_NOT_OK(offset_builder_.Finish(&offsets));
    RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
    std::shared_ptr<Array> values;
    RETURN_NOT_OK(handler.Finish(value_builder_, &values));
    auto type = list(field("item", values->type(), value_builder_.nullable,
                           Kind::Tag(value_builder_.kind)));
    *out = MakeArray(ArrayData::Make(type, size, {null_bitmap, offsets}, {values->data()},
                                     null_count));
    return Status::OK();
  }

  BuilderPtr value_builder() const { return value_builder_; }

  void value_builder(BuilderPtr builder) { value_builder_ = builder; }

  int64_t length() { return null_bitmap_builder_.length(); }

 private:
  BuilderPtr value_builder_ = BuilderPtr::null;
  int32_t offset_ = 0;
  TypedBufferBuilder<int32_t> offset_builder_;
  TypedBufferBuilder<bool> null_bitmap_builder_;
};

template <>
class RawArrayBuilder<Kind::kObject> {
 public:
  explicit RawArrayBuilder(MemoryPool* pool) : null_bitmap_builder_(pool) {}

  Status Append() { return null_bitmap_builder_.Append(true); }

  Status AppendNull() { return null_bitmap_builder_.Append(false); }

  Status AppendNull(int64_t count) { return null_bitmap_builder_.Append(count, false); }

  int GetFieldIndex(const std::string& name) const {
    auto it = name_to_index_.find(name);
    if (it == name_to_index_.end()) {
      return -1;
    }
    return it->second;
  }

  int AddField(std::string name, BuilderPtr builder) {
    auto index = num_fields();
    field_builders_.push_back(builder);
    name_to_index_.emplace(std::move(name), index);
    return index;
  }

  int num_fields() const { return static_cast<int>(field_builders_.size()); }

  BuilderPtr field_builder(int index) const { return field_builders_[index]; }

  void field_builder(int index, BuilderPtr builder) { field_builders_[index] = builder; }

  template <typename HandlerBase>
  Status Finish(HandlerBase& handler, std::shared_ptr<Array>* out) {
    auto size = length();
    auto null_count = null_bitmap_builder_.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

    std::vector<string_view> field_names(num_fields());
    for (const auto& name_index : name_to_index_) {
      field_names[name_index.second] = name_index.first;
    }

    std::vector<std::shared_ptr<Field>> fields(num_fields());
    std::vector<std::shared_ptr<ArrayData>> child_data(num_fields());
    for (int i = 0; i < num_fields(); ++i) {
      std::shared_ptr<Array> values;
      RETURN_NOT_OK(handler.Finish(field_builders_[i], &values));
      child_data[i] = values->data();
      fields[i] = field(field_names[i].to_string(), values->type(),
                        field_builders_[i].nullable, Kind::Tag(field_builders_[i].kind));
    }

    *out = MakeArray(ArrayData::Make(struct_(std::move(fields)), size, {null_bitmap},
                                     std::move(child_data), null_count));
    return Status::OK();
  }

  int64_t length() { return null_bitmap_builder_.length(); }

 private:
  std::vector<BuilderPtr> field_builders_;
  std::unordered_map<std::string, int> name_to_index_;
  TypedBufferBuilder<bool> null_bitmap_builder_;
};

/// Three implementations are provided for BlockParser, one for each
/// UnexpectedFieldBehavior. However most of the logic is identical in each
/// case, so the majority of the implementation is in this base class
class HandlerBase : public BlockParser,
                    public rj::BaseReaderHandler<rj::UTF8<>, HandlerBase> {
 public:
  /// Retrieve a pointer to a builder from a BuilderPtr
  template <Kind::type kind>
  typename std::enable_if<kind != Kind::kNull, RawArrayBuilder<kind>*>::type Cast(
      BuilderPtr builder) {
    DCHECK_EQ(builder.kind, kind);
    return arena<kind>().data() + builder.index;
  }

  /// Accessor for a stored error Status
  Status Error() { return status_; }

  /// \defgroup rapidjson-handler-interface functions expected by rj::Reader
  ///
  /// bool Key(const char* data, rj::SizeType size, ...) is omitted since
  /// the behavior varies greatly between UnexpectedFieldBehaviors
  ///
  /// @{
  bool Null() {
    status_ = AppendNull();
    return status_.ok();
  }

  bool Bool(bool value) {
    status_ = AppendBool(value);
    return status_.ok();
  }

  bool RawNumber(const char* data, rj::SizeType size, ...) {
    status_ = AppendScalar<Kind::kNumber>(string_view(data, size));
    return status_.ok();
  }

  bool String(const char* data, rj::SizeType size, ...) {
    status_ = AppendScalar<Kind::kString>(string_view(data, size));
    return status_.ok();
  }

  bool StartObject() {
    status_ = StartObjectImpl();
    return status_.ok();
  }

  bool EndObject(...) {
    status_ = EndObjectImpl();
    return status_.ok();
  }

  bool StartArray() {
    status_ = StartArrayImpl();
    return status_.ok();
  }

  bool EndArray(rj::SizeType size) {
    status_ = EndArrayImpl(size);
    return status_.ok();
  }
  /// @}

  /// \brief Set up builders using an expected Schema
  Status Initialize(const std::shared_ptr<Schema>& s) {
    auto type = struct_({});
    if (s) {
      type = struct_(s->fields());
    }
    return MakeBuilder(*type, 0, &builder_);
  }

  Status Finish(BuilderPtr builder, std::shared_ptr<Array>* out) {
    switch (builder.kind) {
      case Kind::kNull: {
        auto length = static_cast<int64_t>(builder.index);
        *out = std::make_shared<NullArray>(length);
        return Status::OK();
      }
      case Kind::kBoolean:
        return Cast<Kind::kBoolean>(builder)->Finish(out);
      case Kind::kNumber:
        return FinishScalar(Cast<Kind::kNumber>(builder), out);
      case Kind::kString:
        return FinishScalar(Cast<Kind::kString>(builder), out);
      case Kind::kArray:
        return Cast<Kind::kArray>(builder)->Finish(*this, out);
      case Kind::kObject:
        return Cast<Kind::kObject>(builder)->Finish(*this, out);
      default:
        return Status::NotImplemented("invalid builder kind");
    }
  }

  Status Finish(std::shared_ptr<Array>* parsed) override {
    RETURN_NOT_OK(scalar_values_builder_.Finish(&scalar_values_));
    return Finish(builder_, parsed);
  }

  explicit HandlerBase(MemoryPool* pool)
      : BlockParser(pool), scalar_values_builder_(pool) {}

 protected:
  /// finish a column of scalar values (string or number)
  Status FinishScalar(ScalarBuilder* builder, std::shared_ptr<Array>* out) {
    std::shared_ptr<Array> indices;
    // TODO(bkietz) embed builder->values_length() in this output somehow
    RETURN_NOT_OK(builder->Finish(&indices));
    return DictionaryArray::FromArrays(dictionary(int32(), scalar_values_), indices, out);
  }

  template <typename Handler, typename Stream>
  Status DoParse(Handler& handler, Stream&& json) {
    constexpr auto parse_flags = rj::kParseIterativeFlag | rj::kParseNanAndInfFlag |
                                 rj::kParseStopWhenDoneFlag |
                                 rj::kParseNumbersAsStringsFlag;

    rj::Reader reader;

    for (; num_rows_ < kMaxParserNumRows; ++num_rows_) {
      auto ok = reader.Parse<parse_flags>(json, handler);
      switch (ok.Code()) {
        case rj::kParseErrorNone:
          // parse the next object
          continue;
        case rj::kParseErrorDocumentEmpty:
          // parsed all objects, finish
          return Status::OK();
        case rj::kParseErrorTermination:
          // handler emitted an error
          return handler.Error();
        default:
          // rj emitted an error
          // FIXME(bkietz) report more error data (at least the byte range of the current
          // block, and maybe the path to the most recently parsed value?)
          return ParseError(rj::GetParseError_En(ok.Code()));
      }
    }
    return Status::Invalid("Exceeded maximum rows");
  }

  template <typename Handler>
  Status DoParse(Handler& handler, const std::shared_ptr<Buffer>& json) {
    auto remaining_capacity = scalar_values_builder_.value_data_capacity() -
                              scalar_values_builder_.value_data_length();
    if (json->size() > remaining_capacity) {
      auto additional_storage = json->size() - remaining_capacity;
      RETURN_NOT_OK(scalar_values_builder_.ReserveData(additional_storage));
    }

    rj::MemoryStream ms(reinterpret_cast<const char*>(json->data()), json->size());
    using InputStream = rj::EncodedInputStream<rj::UTF8<>, rj::MemoryStream>;
    return DoParse(handler, InputStream(ms));
  }

  /// construct a builder of statically defined kind in arenas_
  template <Kind::type kind>
  Status MakeBuilder(int64_t leading_nulls, BuilderPtr* builder) {
    builder->index = static_cast<uint32_t>(arena<kind>().size());
    builder->kind = kind;
    builder->nullable = true;
    arena<kind>().emplace_back(pool_);
    return Cast<kind>(*builder)->AppendNull(leading_nulls);
  }

  /// construct a builder of whatever kind corresponds to a DataType
  Status MakeBuilder(const DataType& t, int64_t leading_nulls, BuilderPtr* builder) {
    Kind::type kind;
    RETURN_NOT_OK(Kind::ForType(t, &kind));
    switch (kind) {
      case Kind::kNull:
        *builder = BuilderPtr(Kind::kNull, static_cast<uint32_t>(leading_nulls), true);
        return Status::OK();
      case Kind::kBoolean:
        return MakeBuilder<Kind::kBoolean>(leading_nulls, builder);
      case Kind::kNumber:
        return MakeBuilder<Kind::kNumber>(leading_nulls, builder);
      case Kind::kString:
        return MakeBuilder<Kind::kString>(leading_nulls, builder);
      case Kind::kArray: {
        RETURN_NOT_OK(MakeBuilder<Kind::kArray>(leading_nulls, builder));
        const auto& list_type = static_cast<const ListType&>(t);
        BuilderPtr value_builder;
        RETURN_NOT_OK(MakeBuilder(*list_type.value_type(), 0, &value_builder));
        value_builder.nullable = list_type.value_field()->nullable();
        Cast<Kind::kArray>(*builder)->value_builder(value_builder);
        return Status::OK();
      }
      case Kind::kObject: {
        RETURN_NOT_OK(MakeBuilder<Kind::kObject>(leading_nulls, builder));
        const auto& struct_type = static_cast<const StructType&>(t);
        for (const auto& f : struct_type.children()) {
          BuilderPtr field_builder;
          RETURN_NOT_OK(MakeBuilder(*f->type(), leading_nulls, &field_builder));
          field_builder.nullable = f->nullable();
          Cast<Kind::kObject>(*builder)->AddField(f->name(), field_builder);
        }
        return Status::OK();
      }
      default:
        return Status::NotImplemented("invalid builder type");
    }
  }

  /// \defgroup handlerbase-append-methods append non-nested values
  ///
  /// These methods act on builder_
  /// @{

  Status AppendNull() {
    if (ARROW_PREDICT_FALSE(!builder_.nullable)) {
      return ParseError("a required field was null");
    }
    switch (builder_.kind) {
      case Kind::kNull: {
        // increment null count stored inline
        // update the parent, since changing builder_ doesn't affect parent
        auto parent = builder_stack_.back();
        if (parent.kind == Kind::kArray) {
          auto list_builder = Cast<Kind::kArray>(parent);
          DCHECK_EQ(list_builder->value_builder(), builder_);
          builder_.index += 1;
          list_builder->value_builder(builder_);
        } else {
          auto struct_builder = Cast<Kind::kObject>(parent);
          DCHECK_EQ(struct_builder->field_builder(field_index_), builder_);
          builder_.index += 1;
          struct_builder->field_builder(field_index_, builder_);
        }
        return Status::OK();
      }
      case Kind::kBoolean:
        return Cast<Kind::kBoolean>(builder_)->AppendNull();
      case Kind::kNumber:
        return Cast<Kind::kNumber>(builder_)->AppendNull();
      case Kind::kString:
        return Cast<Kind::kString>(builder_)->AppendNull();
      case Kind::kArray:
        return Cast<Kind::kArray>(builder_)->AppendNull();
      case Kind::kObject: {
        auto root = builder_;
        auto struct_builder = Cast<Kind::kObject>(builder_);
        RETURN_NOT_OK(struct_builder->AppendNull());
        for (int i = 0; i < struct_builder->num_fields(); ++i) {
          builder_ = struct_builder->field_builder(i);
          RETURN_NOT_OK(AppendNull());
        }
        builder_ = root;
        return Status::OK();
      }
      default:
        return Status::NotImplemented("invalid builder Kind");
    }
  }

  Status AppendBool(bool value) {
    constexpr auto kind = Kind::kBoolean;
    if (ARROW_PREDICT_FALSE(builder_.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    return Cast<kind>(builder_)->Append(value);
  }

  template <Kind::type kind>
  Status AppendScalar(string_view scalar) {
    if (ARROW_PREDICT_FALSE(builder_.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    auto index = static_cast<int32_t>(scalar_values_builder_.length());
    auto value_length = static_cast<int32_t>(scalar.size());
    RETURN_NOT_OK(Cast<kind>(builder_)->Append(index, value_length));
    RETURN_NOT_OK(scalar_values_builder_.Reserve(1));
    scalar_values_builder_.UnsafeAppend(scalar);
    return Status::OK();
  }

  /// @}

  Status StartObjectImpl() {
    constexpr auto kind = Kind::kObject;
    if (ARROW_PREDICT_FALSE(builder_.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    auto struct_builder = Cast<kind>(builder_);
    absent_fields_stack_.Push(struct_builder->num_fields(), true);
    PushStacks();
    return struct_builder->Append();
  }

  /// \brief helper for Key() functions
  ///
  /// sets the field builder with name key, or returns false if
  /// there is no field with that name
  bool SetFieldBuilder(string_view key) {
    auto parent = Cast<Kind::kObject>(builder_stack_.back());
    field_index_ = parent->GetFieldIndex(std::string(key));
    if (ARROW_PREDICT_FALSE(field_index_ == -1)) {
      return false;
    }
    builder_ = parent->field_builder(field_index_);
    absent_fields_stack_[field_index_] = false;
    return true;
  }

  Status EndObjectImpl() {
    auto parent = Cast<Kind::kObject>(builder_stack_.back());

    auto expected_count = absent_fields_stack_.TopSize();
    for (field_index_ = 0; field_index_ < expected_count; ++field_index_) {
      if (!absent_fields_stack_[field_index_]) {
        continue;
      }
      builder_ = parent->field_builder(field_index_);
      if (ARROW_PREDICT_FALSE(!builder_.nullable)) {
        return ParseError("a required field was absent");
      }
      RETURN_NOT_OK(AppendNull());
    }
    absent_fields_stack_.Pop();
    PopStacks();
    return Status::OK();
  }

  Status StartArrayImpl() {
    constexpr auto kind = Kind::kArray;
    if (ARROW_PREDICT_FALSE(builder_.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    PushStacks();
    // append to the list builder in EndArrayImpl
    builder_ = Cast<kind>(builder_)->value_builder();
    return Status::OK();
  }

  Status EndArrayImpl(rj::SizeType size) {
    PopStacks();
    // append to list_builder here
    auto list_builder = Cast<Kind::kArray>(builder_);
    return list_builder->Append(size);
  }

  /// helper method for StartArray and StartObject
  /// adds the current builder to a stack so its
  /// children can be visited and parsed.
  void PushStacks() {
    field_index_stack_.push_back(field_index_);
    field_index_ = -1;
    builder_stack_.push_back(builder_);
  }

  /// helper method for EndArray and EndObject
  /// replaces the current builder with its parent
  /// so parsing of the parent can continue
  void PopStacks() {
    field_index_ = field_index_stack_.back();
    field_index_stack_.pop_back();
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
  }

  Status IllegallyChangedTo(Kind::type illegally_changed_to) {
    return KindChangeError(builder_.kind, illegally_changed_to);
  }

  template <Kind::type kind>
  std::vector<RawArrayBuilder<kind>>& arena() {
    return std::get<static_cast<std::size_t>(kind)>(arenas_);
  }

  Status status_;
  std::tuple<std::tuple<>, std::vector<RawArrayBuilder<Kind::kBoolean>>,
             std::vector<RawArrayBuilder<Kind::kNumber>>,
             std::vector<RawArrayBuilder<Kind::kString>>,
             std::vector<RawArrayBuilder<Kind::kArray>>,
             std::vector<RawArrayBuilder<Kind::kObject>>>
      arenas_;
  BuilderPtr builder_;
  // top of this stack is the parent of builder_
  std::vector<BuilderPtr> builder_stack_;
  // top of this stack refers to the fields of the highest *StructBuilder*
  // in builder_stack_ (list builders don't have absent fields)
  BitsetStack absent_fields_stack_;
  // index of builder_ within its parent
  int field_index_;
  // top of this stack == field_index_
  std::vector<int> field_index_stack_;
  StringBuilder scalar_values_builder_;
  std::shared_ptr<Array> scalar_values_;
};

template <UnexpectedFieldBehavior>
class Handler;

template <>
class Handler<UnexpectedFieldBehavior::Error> : public HandlerBase {
 public:
  using HandlerBase::HandlerBase;

  Status Parse(const std::shared_ptr<Buffer>& json) override {
    return DoParse(*this, json);
  }

  /// \ingroup rapidjson-handler-interface
  ///
  /// if an unexpected field is encountered, emit a parse error and bail
  bool Key(const char* key, rj::SizeType len, ...) {
    if (ARROW_PREDICT_TRUE(SetFieldBuilder(string_view(key, len)))) {
      return true;
    }
    status_ = ParseError("unexpected field");
    return false;
  }
};

template <>
class Handler<UnexpectedFieldBehavior::Ignore> : public HandlerBase {
 public:
  using HandlerBase::HandlerBase;

  Status Parse(const std::shared_ptr<Buffer>& json) override {
    return DoParse(*this, json);
  }

  bool Null() {
    if (Skipping()) {
      return true;
    }
    return HandlerBase::Null();
  }

  bool Bool(bool value) {
    if (Skipping()) {
      return true;
    }
    return HandlerBase::Bool(value);
  }

  bool RawNumber(const char* data, rj::SizeType size, ...) {
    if (Skipping()) {
      return true;
    }
    return HandlerBase::RawNumber(data, size);
  }

  bool String(const char* data, rj::SizeType size, ...) {
    if (Skipping()) {
      return true;
    }
    return HandlerBase::String(data, size);
  }

  bool StartObject() {
    ++depth_;
    if (Skipping()) {
      return true;
    }
    return HandlerBase::StartObject();
  }

  /// \ingroup rapidjson-handler-interface
  ///
  /// if an unexpected field is encountered, skip until its value has been consumed
  bool Key(const char* key, rj::SizeType len, ...) {
    MaybeStopSkipping();
    if (Skipping()) {
      return true;
    }
    if (ARROW_PREDICT_TRUE(SetFieldBuilder(string_view(key, len)))) {
      return true;
    }
    skip_depth_ = depth_;
    return true;
  }

  bool EndObject(...) {
    MaybeStopSkipping();
    --depth_;
    if (Skipping()) {
      return true;
    }
    return HandlerBase::EndObject();
  }

  bool StartArray() {
    if (Skipping()) {
      return true;
    }
    return HandlerBase::StartArray();
  }

  bool EndArray(rj::SizeType size) {
    if (Skipping()) {
      return true;
    }
    return HandlerBase::EndArray(size);
  }

 private:
  bool Skipping() { return depth_ >= skip_depth_; }

  void MaybeStopSkipping() {
    if (skip_depth_ == depth_) {
      skip_depth_ = std::numeric_limits<int>::max();
    }
  }

  int depth_ = 0;
  int skip_depth_ = std::numeric_limits<int>::max();
};

template <>
class Handler<UnexpectedFieldBehavior::InferType> : public HandlerBase {
 public:
  using HandlerBase::HandlerBase;

  Status Parse(const std::shared_ptr<Buffer>& json) override {
    return DoParse(*this, json);
  }

  bool Bool(bool value) {
    if (ARROW_PREDICT_FALSE(MaybePromoteFromNull<Kind::kBoolean>())) {
      return false;
    }
    return HandlerBase::Bool(value);
  }

  bool RawNumber(const char* data, rj::SizeType size, ...) {
    if (ARROW_PREDICT_FALSE(MaybePromoteFromNull<Kind::kNumber>())) {
      return false;
    }
    return HandlerBase::RawNumber(data, size);
  }

  bool String(const char* data, rj::SizeType size, ...) {
    if (ARROW_PREDICT_FALSE(MaybePromoteFromNull<Kind::kString>())) {
      return false;
    }
    return HandlerBase::String(data, size);
  }

  bool StartObject() {
    if (ARROW_PREDICT_FALSE(MaybePromoteFromNull<Kind::kObject>())) {
      return false;
    }
    return HandlerBase::StartObject();
  }

  /// \ingroup rapidjson-handler-interface
  ///
  /// If an unexpected field is encountered, add a new builder to
  /// the current parent builder. It is added as a NullBuilder with
  /// (parent.length - 1) leading nulls. The next value parsed
  /// will probably trigger promotion of this field from null
  bool Key(const char* key, rj::SizeType len, ...) {
    if (ARROW_PREDICT_TRUE(SetFieldBuilder(string_view(key, len)))) {
      return true;
    }
    auto struct_builder = Cast<Kind::kObject>(builder_stack_.back());
    auto leading_nulls = static_cast<uint32_t>(struct_builder->length() - 1);
    builder_ = BuilderPtr(Kind::kNull, leading_nulls, true);
    field_index_ = struct_builder->AddField(std::string(key, len), builder_);
    return true;
  }

  bool StartArray() {
    if (ARROW_PREDICT_FALSE(MaybePromoteFromNull<Kind::kArray>())) {
      return false;
    }
    return HandlerBase::StartArray();
  }

 private:
  // return true if a terminal error was encountered
  template <Kind::type kind>
  bool MaybePromoteFromNull() {
    if (ARROW_PREDICT_TRUE(builder_.kind != Kind::kNull)) {
      return false;
    }
    auto parent = builder_stack_.back();
    if (parent.kind == Kind::kArray) {
      auto list_builder = Cast<Kind::kArray>(parent);
      DCHECK_EQ(list_builder->value_builder(), builder_);
      status_ = MakeBuilder<kind>(builder_.index, &builder_);
      if (ARROW_PREDICT_FALSE(!status_.ok())) {
        return true;
      }
      list_builder = Cast<Kind::kArray>(parent);
      list_builder->value_builder(builder_);
    } else {
      auto struct_builder = Cast<Kind::kObject>(parent);
      DCHECK_EQ(struct_builder->field_builder(field_index_), builder_);
      status_ = MakeBuilder<kind>(builder_.index, &builder_);
      if (ARROW_PREDICT_FALSE(!status_.ok())) {
        return true;
      }
      struct_builder = Cast<Kind::kObject>(parent);
      struct_builder->field_builder(field_index_, builder_);
    }
    return false;
  }
};

Status BlockParser::Make(MemoryPool* pool, const ParseOptions& options,
                         std::unique_ptr<BlockParser>* out) {
  DCHECK(options.unexpected_field_behavior == UnexpectedFieldBehavior::InferType ||
         options.explicit_schema != nullptr);

  switch (options.unexpected_field_behavior) {
    case UnexpectedFieldBehavior::Ignore: {
      *out = make_unique<Handler<UnexpectedFieldBehavior::Ignore>>(pool);
      break;
    }
    case UnexpectedFieldBehavior::Error: {
      *out = make_unique<Handler<UnexpectedFieldBehavior::Error>>(pool);
      break;
    }
    case UnexpectedFieldBehavior::InferType:
      *out = make_unique<Handler<UnexpectedFieldBehavior::InferType>>(pool);
      break;
  }
  return static_cast<HandlerBase&>(**out).Initialize(options.explicit_schema);
}

Status BlockParser::Make(const ParseOptions& options, std::unique_ptr<BlockParser>* out) {
  return BlockParser::Make(default_memory_pool(), options, out);
}

}  // namespace json
}  // namespace arrow
