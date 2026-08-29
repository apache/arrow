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

#include <functional>
#include <limits>
#include <memory>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/buffer_builder.h"
#include "arrow/type.h"
#include "arrow/util/bitset_stack_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/simdjson_internal.h"
#include "arrow/util/trie_internal.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::BitsetStack;
using internal::checked_cast;

namespace json {

namespace sj = simdjson::ondemand;

template <typename... T>
static Status ParseError(T&&... t) {
  return Status::Invalid("JSON parse error: ", std::forward<T>(t)...);
}

static std::string_view TrimTrailingWhitespace(std::string_view value) {
  while (!value.empty() &&
         (value.back() == ' ' || value.back() == '\t' ||
          value.back() == '\n' || value.back() == '\r')) {
    value.remove_suffix(1);
  }
  return value;
}

const std::string& Kind::Name(Kind::type kind) {
  static const std::string names[] = {
      "null", "boolean", "number", "string", "array", "object", "number_or_string",
  };

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
      key_value_metadata({{"json_kind", Kind::Name(Kind::kNumberOrString)}}),
  };
  return tags[kind];
}

static arrow::internal::Trie MakeFromTagTrie() {
  arrow::internal::TrieBuilder builder;
  for (auto kind : {Kind::kNull, Kind::kBoolean, Kind::kNumber, Kind::kString,
                    Kind::kArray, Kind::kObject, Kind::kNumberOrString}) {
    DCHECK_OK(builder.Append(Kind::Name(kind)));
  }
  auto name_to_kind = builder.Finish();
  DCHECK_OK(name_to_kind.Validate());
  return name_to_kind;
}

Kind::type Kind::FromTag(const std::shared_ptr<const KeyValueMetadata>& tag) {
  static arrow::internal::Trie name_to_kind = MakeFromTagTrie();
  DCHECK_NE(tag->FindKey("json_kind"), -1);
  std::string_view name = tag->value(tag->FindKey("json_kind"));
  DCHECK_NE(name_to_kind.Find(name), -1);
  return static_cast<Kind::type>(name_to_kind.Find(name));
}

Status Kind::ForType(const DataType& type, Kind::type* kind) {
  struct {
    Status Visit(const NullType&) { return SetKind(Kind::kNull); }
    Status Visit(const BooleanType&) { return SetKind(Kind::kBoolean); }
    Status Visit(const NumberType&) { return SetKind(Kind::kNumber); }
    Status Visit(const TimeType&) { return SetKind(Kind::kNumber); }
    Status Visit(const DateType&) { return SetKind(Kind::kNumber); }
    Status Visit(const BinaryType&) { return SetKind(Kind::kString); }
    Status Visit(const LargeBinaryType&) { return SetKind(Kind::kString); }
    Status Visit(const BinaryViewType&) { return SetKind(Kind::kString); }
    Status Visit(const TimestampType&) { return SetKind(Kind::kString); }
    Status Visit(const DecimalType&) { return SetKind(Kind::kNumberOrString); }
    Status Visit(const DictionaryType& dict_type) {
      return Kind::ForType(*dict_type.value_type(), kind_);
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
  uint32_t index;
  Kind::type kind;
  bool nullable;

  bool operator==(BuilderPtr other) const {
    return kind == other.kind && index == other.index;
  }

  bool operator!=(BuilderPtr other) const { return !(other == *this); }

  operator bool() const { return *this != null; }

  bool operator!() const { return *this == null; }

  // The static BuilderPtr for null type data
  static const BuilderPtr null;
};

const BuilderPtr BuilderPtr::null(Kind::kNull, 0, true);

/// \brief Shared context for all value builders in a `RawBuilderSet`
class BuildContext {
 public:
  explicit BuildContext(MemoryPool* pool) : pool_(pool) {}

  MemoryPool* pool() const { return pool_; }

  // Finds or allocates a unique string and returns a persistent `std::string_view`
  std::string_view InternString(std::string_view str) {
    return *string_cache_.emplace(str).first;
  }

 private:
  MemoryPool* pool_;
  std::unordered_set<std::string> string_cache_;
};

template <>
class RawArrayBuilder<Kind::kBoolean> {
 public:
  explicit RawArrayBuilder(BuildContext* context)
      : data_builder_(context->pool()), null_bitmap_builder_(context->pool()) {}

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
/// On completion the indices and the character storage are combined
/// into a dictionary-encoded array, which is a convenient container
/// for indices referring into another array.
class ScalarBuilder {
 public:
  explicit ScalarBuilder(BuildContext* context)
      : values_length_(0),
        data_builder_(context->pool()),
        null_bitmap_builder_(context->pool()) {}

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
  explicit RawArrayBuilder(BuildContext* context)
      : offset_builder_(context->pool()), null_bitmap_builder_(context->pool()) {}

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

  Status Finish(std::function<Status(BuilderPtr, std::shared_ptr<Array>*)> finish_child,
                std::shared_ptr<Array>* out) {
    RETURN_NOT_OK(offset_builder_.Append(offset_));
    auto size = length();
    auto null_count = null_bitmap_builder_.false_count();
    std::shared_ptr<Buffer> offsets, null_bitmap;
    RETURN_NOT_OK(offset_builder_.Finish(&offsets));
    RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
    std::shared_ptr<Array> values;
    RETURN_NOT_OK(finish_child(value_builder_, &values));
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
  explicit RawArrayBuilder(BuildContext* context)
      : context_(context), null_bitmap_builder_(context->pool()) {}

  Status Append() { return null_bitmap_builder_.Append(true); }

  Status AppendNull() { return null_bitmap_builder_.Append(false); }

  Status AppendNull(int64_t count) { return null_bitmap_builder_.Append(count, false); }

  int FindFieldIndex(std::string_view name) const {
    auto it = name_to_index_.find(name);
    return it != name_to_index_.end() ? it->second : -1;
  }

  int GetFieldIndex(std::string_view name) {
    if (ARROW_PREDICT_FALSE(num_fields() == 0)) {
      return -1;
    }

    if (next_index_ == -1) {
      return FindFieldIndex(name);
    }

    if (next_index_ == num_fields()) {
      next_index_ = 0;
    }
    // Field ordering has been predictable thus far, so check the expected index first
    if (ARROW_PREDICT_TRUE(name == field_infos_[next_index_].name)) {
      return next_index_++;
    }

    // Prediction failed - fall back to the map
    auto index = FindFieldIndex(name);
    if (ARROW_PREDICT_FALSE(index != -1)) {
      // We already have this key, so the incoming fields are sparse and/or inconsistently
      // ordered. At the risk of introducing crippling overhead for worst-case input, we
      // bail on the optimization.
      next_index_ = -1;
    }

    return index;
  }

  int AddField(std::string_view name, BuilderPtr builder) {
    auto index = FindFieldIndex(name);

    if (ARROW_PREDICT_TRUE(index == -1)) {
      name = context_->InternString(name);
      index = num_fields();
      field_infos_.push_back(FieldInfo{name, builder});
      name_to_index_.emplace(name, index);
    }

    return index;
  }

  int num_fields() const { return static_cast<int>(field_infos_.size()); }

  std::string_view field_name(int index) const { return field_infos_[index].name; }

  BuilderPtr field_builder(int index) const { return field_infos_[index].builder; }

  void field_builder(int index, BuilderPtr builder) {
    field_infos_[index].builder = builder;
  }

  Status Finish(std::function<Status(BuilderPtr, std::shared_ptr<Array>*)> finish_child,
                std::shared_ptr<Array>* out) {
    auto size = length();
    auto null_count = null_bitmap_builder_.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

    std::vector<std::shared_ptr<Field>> fields(num_fields());
    std::vector<std::shared_ptr<ArrayData>> child_data(num_fields());
    for (int i = 0; i < num_fields(); ++i) {
      const auto& info = field_infos_[i];
      std::shared_ptr<Array> field_values;
      RETURN_NOT_OK(finish_child(info.builder, &field_values));
      child_data[i] = field_values->data();
      fields[i] = field(std::string(info.name), field_values->type(),
                        info.builder.nullable, Kind::Tag(info.builder.kind));
    }

    *out = MakeArray(ArrayData::Make(struct_(std::move(fields)), size, {null_bitmap},
                                     std::move(child_data), null_count));
    return Status::OK();
  }

  int64_t length() { return null_bitmap_builder_.length(); }

 private:
  struct FieldInfo {
    std::string_view name;
    BuilderPtr builder;
  };

  BuildContext* context_;

  std::vector<FieldInfo> field_infos_;
  std::unordered_map<std::string_view, int> name_to_index_;

  TypedBufferBuilder<bool> null_bitmap_builder_;

  // Predictive index for optimizing name -> index lookups in cases where fields are
  // consistently ordered.
  int next_index_ = 0;
};

template <>
class RawArrayBuilder<Kind::kNumberOrString> : public ScalarBuilder {
 public:
  using ScalarBuilder::ScalarBuilder;
};

class RawBuilderSet {
 public:
  explicit RawBuilderSet(MemoryPool* pool) : context_(pool) {}

  /// Retrieve a pointer to a builder from a BuilderPtr
  template <Kind::type kind>
  enable_if_t<kind != Kind::kNull, RawArrayBuilder<kind>*> Cast(BuilderPtr builder) {
    DCHECK_EQ(builder.kind, kind);
    return arena<kind>().data() + builder.index;
  }

  /// construct a builder of statically defined kind
  template <Kind::type kind>
  Status MakeBuilder(int64_t leading_nulls, BuilderPtr* builder) {
    builder->index = static_cast<uint32_t>(arena<kind>().size());
    builder->kind = kind;
    builder->nullable = true;
    arena<kind>().emplace_back(RawArrayBuilder<kind>(&context_));
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

      case Kind::kNumberOrString:
        return MakeBuilder<Kind::kNumberOrString>(leading_nulls, builder);

      case Kind::kArray: {
        RETURN_NOT_OK(MakeBuilder<Kind::kArray>(leading_nulls, builder));
        const auto& list_type = checked_cast<const ListType&>(t);

        BuilderPtr value_builder;
        RETURN_NOT_OK(MakeBuilder(*list_type.value_type(), 0, &value_builder));
        value_builder.nullable = list_type.value_field()->nullable();

        Cast<Kind::kArray>(*builder)->value_builder(value_builder);
        return Status::OK();
      }
      case Kind::kObject: {
        RETURN_NOT_OK(MakeBuilder<Kind::kObject>(leading_nulls, builder));
        const auto& struct_type = checked_cast<const StructType&>(t);

        for (const auto& f : struct_type.fields()) {
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

  /// Appending null is slightly tricky since null count is stored inline
  /// for builders of Kind::kNull. Append nulls using this helper
  Status AppendNull(BuilderPtr parent, int field_index, BuilderPtr builder) {
    if (ARROW_PREDICT_FALSE(!builder.nullable)) {
      return ParseError("a required field was null");
    }
    switch (builder.kind) {
      case Kind::kNull: {
        DCHECK_EQ(builder, parent.kind == Kind::kArray
                               ? Cast<Kind::kArray>(parent)->value_builder()
                               : Cast<Kind::kObject>(parent)->field_builder(field_index));

        // increment null count stored inline
        builder.index += 1;

        // update the parent, since changing builder doesn't affect parent
        if (parent.kind == Kind::kArray) {
          Cast<Kind::kArray>(parent)->value_builder(builder);
        } else {
          Cast<Kind::kObject>(parent)->field_builder(field_index, builder);
        }
        return Status::OK();
      }
      case Kind::kBoolean:
        return Cast<Kind::kBoolean>(builder)->AppendNull();

      case Kind::kNumber:
        return Cast<Kind::kNumber>(builder)->AppendNull();

      case Kind::kString:
        return Cast<Kind::kString>(builder)->AppendNull();

      case Kind::kNumberOrString: {
        return Cast<Kind::kNumberOrString>(builder)->AppendNull();
      }

      case Kind::kArray:
        return Cast<Kind::kArray>(builder)->AppendNull();

      case Kind::kObject: {
        auto struct_builder = Cast<Kind::kObject>(builder);
        RETURN_NOT_OK(struct_builder->AppendNull());

        for (int i = 0; i < struct_builder->num_fields(); ++i) {
          auto field_builder = struct_builder->field_builder(i);
          RETURN_NOT_OK(AppendNull(builder, i, field_builder));
        }
        return Status::OK();
      }

      default:
        return Status::NotImplemented("invalid builder Kind");
    }
  }

  Status Finish(const std::shared_ptr<Array>& scalar_values, BuilderPtr builder,
                std::shared_ptr<Array>* out) {
    auto finish_children = [this, &scalar_values](BuilderPtr child,
                                                  std::shared_ptr<Array>* out) {
      return Finish(scalar_values, child, out);
    };
    switch (builder.kind) {
      case Kind::kNull: {
        auto length = static_cast<int64_t>(builder.index);
        *out = std::make_shared<NullArray>(length);
        return Status::OK();
      }
      case Kind::kBoolean:
        return Cast<Kind::kBoolean>(builder)->Finish(out);

      case Kind::kNumber:
        return FinishScalar(scalar_values, Cast<Kind::kNumber>(builder), out);

      case Kind::kString:
        return FinishScalar(scalar_values, Cast<Kind::kString>(builder), out);

      case Kind::kNumberOrString:
        return FinishScalar(scalar_values, Cast<Kind::kNumberOrString>(builder), out);

      case Kind::kArray:
        return Cast<Kind::kArray>(builder)->Finish(std::move(finish_children), out);

      case Kind::kObject:
        return Cast<Kind::kObject>(builder)->Finish(std::move(finish_children), out);

      default:
        return Status::NotImplemented("invalid builder kind");
    }
  }

 private:
  /// finish a column of scalar values (string or number)
  Status FinishScalar(const std::shared_ptr<Array>& scalar_values, ScalarBuilder* builder,
                      std::shared_ptr<Array>* out) {
    std::shared_ptr<Array> indices;
    // TODO(bkietz) embed builder->values_length() in this output somehow
    RETURN_NOT_OK(builder->Finish(&indices));
    auto ty = dictionary(int32(), scalar_values->type());
    *out = std::make_shared<DictionaryArray>(ty, indices, scalar_values);
    return Status::OK();
  }

  template <Kind::type kind>
  std::vector<RawArrayBuilder<kind>>& arena() {
    return std::get<static_cast<std::size_t>(kind)>(arenas_);
  }

  BuildContext context_;
  std::tuple<std::tuple<>, std::vector<RawArrayBuilder<Kind::kBoolean>>,
             std::vector<RawArrayBuilder<Kind::kNumber>>,
             std::vector<RawArrayBuilder<Kind::kString>>,
             std::vector<RawArrayBuilder<Kind::kArray>>,
             std::vector<RawArrayBuilder<Kind::kObject>>,
             std::vector<RawArrayBuilder<Kind::kNumberOrString>>>
      arenas_;
};

/// Three implementations are provided for BlockParser, one for each
/// UnexpectedFieldBehavior. However most of the logic is identical in each
/// case, so the majority of the implementation is in this base class
class HandlerBase : public BlockParser {
 public:
  explicit HandlerBase(MemoryPool* pool)
      : BlockParser(pool),
        builder_set_(pool),
        field_index_(-1),
        scalar_values_builder_(pool) {}

  /// Retrieve a pointer to a builder from a BuilderPtr
  template <Kind::type kind>
  enable_if_t<kind != Kind::kNull, RawArrayBuilder<kind>*> Cast(BuilderPtr builder) {
    return builder_set_.Cast<kind>(builder);
  }

  /// Accessor for a stored error Status
  Status Error() { return status_; }

  Status Null() {
    return builder_set_.AppendNull(builder_stack_.back(), field_index_, builder_);
  }

  Status Bool(bool value) {
    constexpr auto kind = Kind::kBoolean;
    if (ARROW_PREDICT_FALSE(builder_.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    return Cast<kind>(builder_)->Append(value);
  }

  Status RawNumber(std::string_view value) {
    if (builder_.kind == Kind::kNumberOrString) {
      return AppendScalar<Kind::kNumberOrString>(builder_, value);
    }
    return AppendScalar<Kind::kNumber>(builder_, value);
  }

  Status String(std::string_view value) {
    if (builder_.kind == Kind::kNumberOrString) {
      return AppendScalar<Kind::kNumberOrString>(builder_, value);
    }
    return AppendScalar<Kind::kString>(builder_, value);
  }

  /// \brief Set up builders using an expected Schema
  Status Initialize(const std::shared_ptr<Schema>& s) {
    auto type = struct_({});
    if (s) {
      type = struct_(s->fields());
    }
    return builder_set_.MakeBuilder(*type, 0, &builder_);
  }

  Status Finish(std::shared_ptr<Array>* parsed) override {
    std::shared_ptr<Array> scalar_values;
    RETURN_NOT_OK(scalar_values_builder_.Finish(&scalar_values));
    return builder_set_.Finish(scalar_values, builder_, parsed);
  }

  /// \brief Emit path of current field for debugging purposes
  std::string Path() {
    std::string path;
    for (size_t i = 0; i < builder_stack_.size(); ++i) {
      auto builder = builder_stack_[i];
      if (builder.kind == Kind::kArray) {
        path += "/[]";
      } else {
        auto struct_builder = Cast<Kind::kObject>(builder);
        auto field_index = field_index_;
        if (i + 1 < field_index_stack_.size()) {
          field_index = field_index_stack_[i + 1];
        }
        path += "/" + std::string(struct_builder->field_name(field_index));
      }
    }
    return path;
  }

 protected:
  template <typename Handler>
  Status DoParse(Handler& handler, const std::shared_ptr<Buffer>& json) {
    RETURN_NOT_OK(ReserveScalarStorage(json->size()));

    simdjson::padded_string padded_json(
        reinterpret_cast<const char*>(json->data()), json->size());

    sj::parser parser;

    ARROW_ASSIGN_OR_RAISE(
        auto stream,
        internal::ResolveSimdjsonResult(
            parser.iterate_many(padded_json),
            "Failed to create JSON document stream"));

    for (auto document_result : stream) {
      ARROW_ASSIGN_OR_RAISE(
          auto document,
          internal::ResolveSimdjsonResult(
              document_result, "Failed to iterate JSON document stream"));

      if (num_rows_ == std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Row count overflowed int32_t");
      }

      ARROW_ASSIGN_OR_RAISE(
          auto value,
          internal::ResolveSimdjsonResult(
              document.get_value(), "JSON parse error: Failed to get JSON value"));

      RETURN_NOT_OK(ParseValue(handler, value));

      ++num_rows_;
    }

    if (stream.truncated_bytes() != 0) {
      return ParseError("The document is empty");
    }

    return Status::OK();
  }

  template <Kind::type kind>
  Status MaybePromoteFromNull() {
    if (builder_.kind != Kind::kNull) {
      return Status::OK();
    }

    auto parent = builder_stack_.back();

    if (parent.kind == Kind::kArray) {
      auto list_builder = Cast<Kind::kArray>(parent);
      DCHECK_EQ(list_builder->value_builder(), builder_);

      RETURN_NOT_OK(builder_set_.MakeBuilder<kind>(builder_.index, &builder_));

      list_builder = Cast<Kind::kArray>(parent);
      list_builder->value_builder(builder_);
    } else {
      auto struct_builder = Cast<Kind::kObject>(parent);
      DCHECK_EQ(struct_builder->field_builder(field_index_), builder_);

      RETURN_NOT_OK(builder_set_.MakeBuilder<kind>(builder_.index, &builder_));

      struct_builder = Cast<Kind::kObject>(parent);
      struct_builder->field_builder(field_index_, builder_);
    }

    return Status::OK();
  }

  template <typename Handler>
  Status ParseValue(Handler& handler, sj::value value) {
    ARROW_ASSIGN_OR_RAISE(
        auto type,
        internal::ResolveSimdjsonResult(
            value.type(), "Failed to determine JSON type"));

    switch (type) {
      case sj::json_type::null:
        return Null();

      case sj::json_type::boolean: {
        RETURN_NOT_OK(handler.template MaybePromoteFromNull<Kind::kBoolean>());

        ARROW_ASSIGN_OR_RAISE(
            auto boolean,
            internal::ResolveSimdjsonResult(
                value.get_bool(), "Failed to get JSON boolean"));
        return Bool(boolean);
      }

      case sj::json_type::string: {
        RETURN_NOT_OK(handler.template MaybePromoteFromNull<Kind::kString>());

        ARROW_ASSIGN_OR_RAISE(
            auto string,
            internal::ResolveSimdjsonResult(
                value.get_string(), "Failed to get JSON string"));
        return String(string);
      }

      case sj::json_type::number: {
        RETURN_NOT_OK(handler.template MaybePromoteFromNull<Kind::kNumber>());
        return RawNumber(TrimTrailingWhitespace(value.raw_json_token()));
      }

      case sj::json_type::array:
        RETURN_NOT_OK(handler.template MaybePromoteFromNull<Kind::kArray>());
        return ParseArray(handler, value);

      case sj::json_type::object:
        RETURN_NOT_OK(handler.template MaybePromoteFromNull<Kind::kObject>());
        return ParseObject(handler, value);

      default:
        return ParseError("Invalid value");
    }
  }

  template <typename Handler>
  Status ParseArray(Handler& handler, sj::value value) {
    RETURN_NOT_OK(StartArrayImpl());

    ARROW_ASSIGN_OR_RAISE(
        auto array,
        internal::ResolveSimdjsonResult(
            value.get_array(), "Failed to get JSON array"));

    size_t size = 0;

    for (auto element_result : array) {
      ARROW_ASSIGN_OR_RAISE(
          auto element,
          internal::ResolveSimdjsonResult(
              element_result, "Failed to iterate JSON array"));

      RETURN_NOT_OK(ParseValue(handler, element));
      ++size;
    }

    return EndArrayImpl(size);
  }

  template <typename Handler>
  Status ParseObject(Handler& handler, sj::value value) {
    RETURN_NOT_OK(StartObjectImpl());

    ARROW_ASSIGN_OR_RAISE(
        auto object,
        internal::ResolveSimdjsonResult(
            value.get_object(), "Failed to get JSON object"));

    for (auto field_result : object) {
      ARROW_ASSIGN_OR_RAISE(
          auto field,
          internal::ResolveSimdjsonResult(
              field_result, "JSON parse error: Failed to iterate JSON object"));

      ARROW_ASSIGN_OR_RAISE(
          auto key,
          internal::ResolveSimdjsonResult(
              field.unescaped_key(), "Failed to get JSON object key"));

      auto field_value = field.value();

      RETURN_NOT_OK(ParseObjectField(handler, key, field_value));
    }

    return EndObjectImpl();
  }

  template <typename Handler>
  Status ParseObjectField(Handler& handler, std::string_view key, sj::value value) {
    bool duplicate_keys = false;

    if (SetFieldBuilder(key, &duplicate_keys)) {
      return ParseValue(handler, value);
    }

    if (duplicate_keys) {
      return status_;
    }

    return handler.HandleUnexpectedField(key, value);
  }

  /// \defgroup handlerbase-append-methods append non-nested values
  ///
  /// @{

  template <Kind::type kind>
  Status AppendScalar(BuilderPtr builder, std::string_view scalar) {
    if (ARROW_PREDICT_FALSE(builder.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    auto index = static_cast<int32_t>(scalar_values_builder_.length());
    auto value_length = static_cast<int32_t>(scalar.size());
    RETURN_NOT_OK(Cast<kind>(builder)->Append(index, value_length));
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
    StartNested();
    return struct_builder->Append();
  }

  /// \brief helper for Key() functions
  ///
  /// sets the field builder with name key, or returns false if
  /// there is no field with that name
  bool SetFieldBuilder(std::string_view key, bool* duplicate_keys) {
    auto parent = Cast<Kind::kObject>(builder_stack_.back());
    field_index_ = parent->GetFieldIndex(key);
    if (ARROW_PREDICT_FALSE(field_index_ == -1)) {
      return false;
    }
    if (field_index_ < absent_fields_stack_.TopSize()) {
      *duplicate_keys = !absent_fields_stack_[field_index_];
    } else {
      // When field_index is beyond the range of absent_fields_stack_ we have a duplicated
      // field that wasn't declared in schema or previous records.
      *duplicate_keys = true;
    }
    if (*duplicate_keys) {
      status_ = ParseError("Column(", Path(), ") was specified twice in row ", num_rows_);
      return false;
    }
    builder_ = parent->field_builder(field_index_);
    absent_fields_stack_[field_index_] = false;
    return true;
  }

  Status EndObjectImpl() {
    auto parent = builder_stack_.back();

    auto expected_count = absent_fields_stack_.TopSize();
    for (int i = 0; i < expected_count; ++i) {
      if (!absent_fields_stack_[i]) {
        continue;
      }
      auto field_builder = Cast<Kind::kObject>(parent)->field_builder(i);
      if (ARROW_PREDICT_FALSE(!field_builder.nullable)) {
        return ParseError("a required field was absent");
      }
      RETURN_NOT_OK(builder_set_.AppendNull(parent, i, field_builder));
    }
    absent_fields_stack_.Pop();
    EndNested();
    return Status::OK();
  }

  Status StartArrayImpl() {
    constexpr auto kind = Kind::kArray;
    if (ARROW_PREDICT_FALSE(builder_.kind != kind)) {
      return IllegallyChangedTo(kind);
    }
    StartNested();
    // append to the list builder in EndArrayImpl
    builder_ = Cast<kind>(builder_)->value_builder();
    return Status::OK();
  }

  Status EndArrayImpl(size_t size) {
    EndNested();
    // append to list_builder here
    auto list_builder = Cast<Kind::kArray>(builder_);
    return list_builder->Append(size);
  }

  /// helper method for StartArray and StartObject
  /// adds the current builder to a stack so its
  /// children can be visited and parsed.
  void StartNested() {
    field_index_stack_.push_back(field_index_);
    field_index_ = -1;
    builder_stack_.push_back(builder_);
  }

  /// helper method for EndArray and EndObject
  /// replaces the current builder with its parent
  /// so parsing of the parent can continue
  void EndNested() {
    field_index_ = field_index_stack_.back();
    field_index_stack_.pop_back();
    builder_ = builder_stack_.back();
    builder_stack_.pop_back();
  }

  Status IllegallyChangedTo(Kind::type illegally_changed_to) {
    return ParseError("Column(", Path(), ") changed from ", Kind::Name(builder_.kind),
                      " to ", Kind::Name(illegally_changed_to), " in row ", num_rows_);
  }

  /// Reserve storage for scalars, these can occupy almost all of the JSON buffer
  Status ReserveScalarStorage(int64_t size) override {
    auto available_storage = scalar_values_builder_.value_data_capacity() -
                             scalar_values_builder_.value_data_length();
    if (size <= available_storage) {
      return Status::OK();
    }
    return scalar_values_builder_.ReserveData(size - available_storage);
  }

  Status status_;
  RawBuilderSet builder_set_;
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

  Status HandleUnexpectedField(std::string_view, sj::value) {
    return ParseError("unexpected field");
  }

};

template <>
class Handler<UnexpectedFieldBehavior::Ignore> : public HandlerBase {
 public:
  using HandlerBase::HandlerBase;

  Status Parse(const std::shared_ptr<Buffer>& json) override {
    return DoParse(*this, json);
  }

  Status HandleUnexpectedField(std::string_view, sj::value) {
    return Status::OK();
  }
};

template <>
class Handler<UnexpectedFieldBehavior::InferType> : public HandlerBase {
 public:
  using HandlerBase::HandlerBase;

  Status Parse(const std::shared_ptr<Buffer>& json) override {
    return DoParse(*this, json);
  }

  Status HandleUnexpectedField(std::string_view key, sj::value value) {
    auto struct_builder = Cast<Kind::kObject>(builder_stack_.back());
    auto leading_nulls = static_cast<uint32_t>(struct_builder->length() - 1);

    builder_ = BuilderPtr(Kind::kNull, leading_nulls, true);
    field_index_ = struct_builder->AddField(key, builder_);

    return ParseValue(*this, value);
  }
};

Status BlockParser::Make(MemoryPool* pool, const ParseOptions& options,
                         std::unique_ptr<BlockParser>* out) {
  DCHECK(options.unexpected_field_behavior == UnexpectedFieldBehavior::InferType ||
         options.explicit_schema != nullptr);

  switch (options.unexpected_field_behavior) {
    case UnexpectedFieldBehavior::Ignore: {
      *out = std::make_unique<Handler<UnexpectedFieldBehavior::Ignore>>(pool);
      break;
    }
    case UnexpectedFieldBehavior::Error: {
      *out = std::make_unique<Handler<UnexpectedFieldBehavior::Error>>(pool);
      break;
    }
    case UnexpectedFieldBehavior::InferType:
      *out = std::make_unique<Handler<UnexpectedFieldBehavior::InferType>>(pool);
      break;
  }
  return static_cast<HandlerBase&>(**out).Initialize(options.explicit_schema);
}

Status BlockParser::Make(const ParseOptions& options, std::unique_ptr<BlockParser>* out) {
  return BlockParser::Make(default_memory_pool(), options, out);
}

}  // namespace json
}  // namespace arrow
