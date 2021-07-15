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

#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/enum.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/reflection_internal.h"
#include "arrow/util/string.h"
#include "arrow/util/visibility.h"

namespace arrow {
struct Scalar;
struct StructScalar;
using ::arrow::internal::checked_cast;

namespace compute {
namespace internal {

class GenericOptionsType : public FunctionOptionsType {
 public:
  Result<std::shared_ptr<Buffer>> Serialize(const FunctionOptions&) const override;
  Result<std::unique_ptr<FunctionOptions>> Deserialize(
      const Buffer& buffer) const override;
  virtual Status ToStructScalar(const FunctionOptions& options,
                                std::vector<std::string>* field_names,
                                std::vector<std::shared_ptr<Scalar>>* values) const = 0;
  virtual Result<std::unique_ptr<FunctionOptions>> FromStructScalar(
      const StructScalar& scalar) const = 0;
};

ARROW_EXPORT
Result<std::shared_ptr<StructScalar>> FunctionOptionsToStructScalar(
    const FunctionOptions&);
ARROW_EXPORT
Result<std::unique_ptr<FunctionOptions>> FunctionOptionsFromStructScalar(
    const StructScalar&);
ARROW_EXPORT
Result<std::unique_ptr<FunctionOptions>> DeserializeFunctionOptions(const Buffer& buffer);

template <typename T>
static inline std::string GenericToString(const T& value) {
  std::stringstream ss;
  ss << value;
  return ss.str();
}

static inline std::string GenericToString(bool value) { return value ? "true" : "false"; }

static inline std::string GenericToString(const std::string& value) {
  std::stringstream ss;
  ss << '"' << value << '"';
  return ss.str();
}

template <typename T>
static inline std::string GenericToString(const TimeUnit::type value) {
  switch (value) {
    case TimeUnit::type::SECOND:
      return "SECOND";
    case TimeUnit::type::MILLI:
      return "MILLI";
    case TimeUnit::type::MICRO:
      return "MICRO";
    case TimeUnit::type::NANO:
      return "NANO";
  }
  return "<INVALID>";
}

template <typename Raw>
static inline std::string GenericToString(EnumType<Raw> value) {
  return value.ToString();
}

template <typename T>
static inline std::string GenericToString(const std::shared_ptr<T>& value) {
  std::stringstream ss;
  return value ? value->ToString() : "<NULLPTR>";
}

static inline std::string GenericToString(const std::shared_ptr<Scalar>& value) {
  std::stringstream ss;
  ss << value->type->ToString() << ":" << value->ToString();
  return ss.str();
}

static inline std::string GenericToString(
    const std::shared_ptr<const KeyValueMetadata>& value) {
  std::stringstream ss;
  ss << "KeyValueMetadata{";
  if (value) {
    bool first = true;
    for (const auto& pair : value->sorted_pairs()) {
      if (!first) ss << ", ";
      first = false;
      ss << pair.first << ':' << pair.second;
    }
  }
  ss << '}';
  return ss.str();
}

static inline std::string GenericToString(const Datum& value) {
  switch (value.kind()) {
    case Datum::NONE:
      return "<NULL DATUM>";
    case Datum::SCALAR:
      return GenericToString(value.scalar());
    case Datum::ARRAY: {
      std::stringstream ss;
      ss << value.type()->ToString() << ':' << value.make_array()->ToString();
      return ss.str();
    }
    case Datum::CHUNKED_ARRAY:
    case Datum::RECORD_BATCH:
    case Datum::TABLE:
    case Datum::COLLECTION:
      return value.ToString();
  }
  return value.ToString();
}

template <typename T>
static inline std::string GenericToString(const std::vector<T>& value) {
  std::stringstream ss;
  ss << "[";
  bool first = true;
  // Don't use range-for with auto& to avoid Clang -Wrange-loop-analysis
  for (auto it = value.begin(); it != value.end(); it++) {
    if (!first) ss << ", ";
    first = false;
    ss << GenericToString(*it);
  }
  ss << ']';
  return ss.str();
}

static inline std::string GenericToString(const std::vector<SortKey>& value) {
  std::stringstream ss;
  ss << '[';
  bool first = true;
  for (const auto& key : value) {
    if (!first) {
      ss << ", ";
    }
    first = false;
    ss << key.ToString();
  }
  ss << ']';
  return ss.str();
}

template <typename T>
static inline bool GenericEquals(const T& left, const T& right) {
  return left == right;
}

template <typename T>
static inline bool GenericEquals(const std::shared_ptr<T>& left,
                                 const std::shared_ptr<T>& right) {
  if (left && right) {
    return left->Equals(*right);
  }
  return left == right;
}

static inline bool IsEmpty(const std::shared_ptr<const KeyValueMetadata>& meta) {
  return !meta || meta->size() == 0;
}

static inline bool GenericEquals(const std::shared_ptr<const KeyValueMetadata>& left,
                                 const std::shared_ptr<const KeyValueMetadata>& right) {
  // Special case since null metadata is considered equivalent to empty
  if (IsEmpty(left) || IsEmpty(right)) {
    return IsEmpty(left) && IsEmpty(right);
  }
  return left->Equals(*right);
}

template <typename T>
static inline bool GenericEquals(const std::vector<T>& left,
                                 const std::vector<T>& right) {
  if (left.size() != right.size()) return false;
  for (size_t i = 0; i < left.size(); i++) {
    if (!GenericEquals(left[i], right[i])) return false;
  }
  return true;
}

template <typename T>
static inline decltype(TypeTraits<typename CTypeTraits<T>::ArrowType>::type_singleton())
GenericTypeSingleton() {
  return TypeTraits<typename CTypeTraits<T>::ArrowType>::type_singleton();
}

template <typename T>
static inline enable_if_t<is_reflection_enum<T>::value, std::shared_ptr<DataType>>
GenericTypeSingleton() {
  return int32();
}

template <typename T>
static inline enable_if_same<T, std::shared_ptr<const KeyValueMetadata>,
                             std::shared_ptr<DataType>>
GenericTypeSingleton() {
  return map(binary(), binary());
}

template <typename T>
static inline enable_if_same<T, TimeUnit::type, std::shared_ptr<DataType>>
GenericTypeSingleton() {
  return int32();
}

template <typename T>
static inline enable_if_same<T, SortKey, std::shared_ptr<DataType>>
GenericTypeSingleton() {
  std::vector<std::shared_ptr<Field>> fields;
  fields.emplace_back(new Field("name", GenericTypeSingleton<std::string>()));
  fields.emplace_back(new Field("order", GenericTypeSingleton<SortOrder>()));
  return std::make_shared<StructType>(std::move(fields));
}

// N.B. ordering of overloads is relatively fragile
template <typename T>
static inline Result<decltype(MakeScalar(std::declval<T>()))> GenericToScalar(
    const T& value) {
  return MakeScalar(value);
}

// For Clang/libc++: when iterating through vector<bool>, we can't
// pass it by reference so the overload above doesn't apply
static inline Result<std::shared_ptr<Scalar>> GenericToScalar(bool value) {
  return MakeScalar(value);
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(
    const TimeUnit::type value) {
  return GenericToScalar(static_cast<int32_t>(value));
}

template <typename Raw>
static inline Result<std::shared_ptr<Scalar>> GenericToScalar(const EnumType<Raw> value) {
  return std::make_shared<Int32Scalar>(*value);
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(const SortKey& value) {
  ARROW_ASSIGN_OR_RAISE(auto name, GenericToScalar(value.name));
  ARROW_ASSIGN_OR_RAISE(auto order, GenericToScalar(value.order));
  return StructScalar::Make({name, order}, {"name", "order"});
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(
    const std::shared_ptr<const KeyValueMetadata>& value) {
  auto ty = GenericTypeSingleton<std::shared_ptr<const KeyValueMetadata>>();
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(default_memory_pool(), ty, &builder));
  auto* map_builder = checked_cast<MapBuilder*>(builder.get());
  auto* key_builder = checked_cast<BinaryBuilder*>(map_builder->key_builder());
  auto* item_builder = checked_cast<BinaryBuilder*>(map_builder->item_builder());
  RETURN_NOT_OK(map_builder->Append());
  if (value) {
    RETURN_NOT_OK(key_builder->AppendValues(value->keys()));
    RETURN_NOT_OK(item_builder->AppendValues(value->values()));
  }
  std::shared_ptr<Array> arr;
  RETURN_NOT_OK(map_builder->Finish(&arr));
  return arr->GetScalar(0);
}

template <typename T>
static inline Result<std::shared_ptr<Scalar>> GenericToScalar(
    const std::vector<T>& value) {
  std::shared_ptr<DataType> type = GenericTypeSingleton<T>();
  std::vector<std::shared_ptr<Scalar>> scalars;
  scalars.reserve(value.size());
  // Don't use range-for with auto& to avoid Clang -Wrange-loop-analysis
  for (auto it = value.begin(); it != value.end(); it++) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, GenericToScalar(*it));
    scalars.push_back(std::move(scalar));
  }
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(
      MakeBuilder(default_memory_pool(), type ? type : scalars[0]->type, &builder));
  RETURN_NOT_OK(builder->AppendScalars(scalars));
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(builder->Finish(&out));
  return std::make_shared<ListScalar>(std::move(out));
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(
    const std::shared_ptr<DataType>& value) {
  if (!value) {
    return Status::Invalid("shared_ptr<DataType> is nullptr");
  }
  return MakeNullScalar(value);
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(
    const std::shared_ptr<Scalar>& value) {
  return value;
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(
    const std::shared_ptr<Array>& value) {
  return std::make_shared<ListScalar>(value);
}

static inline Result<std::shared_ptr<Scalar>> GenericToScalar(const Datum& value) {
  // TODO(ARROW-9434): store in a union instead.
  switch (value.kind()) {
    case Datum::ARRAY:
      return GenericToScalar(value.make_array());
      break;
    default:
      return Status::NotImplemented("Cannot serialize Datum kind ", value.kind());
  }
}

template <typename T>
static inline enable_if_primitive_ctype<typename CTypeTraits<T>::ArrowType, Result<T>>
GenericFromScalar(const std::shared_ptr<Scalar>& value) {
  using ArrowType = typename CTypeTraits<T>::ArrowType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;
  if (value->type->id() != ArrowType::type_id) {
    return Status::Invalid("Expected type ", ArrowType::type_id, " but got ",
                           value->type->ToString());
  }
  const auto& holder = checked_cast<const ScalarType&>(*value);
  if (!holder.is_valid) return Status::Invalid("Got null scalar");
  return holder.value;
}

template <typename T, typename U>
using enable_if_same_result = enable_if_same<T, U, Result<T>>;

template <typename T>
static inline enable_if_same_result<T, TimeUnit::type> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  ARROW_ASSIGN_OR_RAISE(auto raw_val, GenericFromScalar<int32_t>(value));
  switch (raw_val) {
    case static_cast<int32_t>(TimeUnit::type::SECOND):
      return TimeUnit::type::SECOND;
    case static_cast<int32_t>(TimeUnit::type::MILLI):
      return TimeUnit::type::MILLI;
    case static_cast<int32_t>(TimeUnit::type::MICRO):
      return TimeUnit::type::MICRO;
    case static_cast<int32_t>(TimeUnit::type::NANO):
      return TimeUnit::type::NANO;
  }
  return Status::Invalid("Invalid value for TimeUnit::type: ", raw_val);
}

template <typename T>
static inline enable_if_t<is_reflection_enum<T>::value, Result<T>> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  return T::Make(checked_cast<const Int32Scalar&>(*value).value);
}

template <typename T>
static inline enable_if_same_result<T, std::string> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  if (!is_base_binary_like(value->type->id())) {
    return Status::Invalid("Expected binary-like type but got ", value->type->ToString());
  }
  const auto& holder = checked_cast<const BaseBinaryScalar&>(*value);
  if (!holder.is_valid) return Status::Invalid("Got null scalar");
  return holder.value->ToString();
}

template <typename T>
static inline enable_if_same_result<T, SortKey> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  if (value->type->id() != Type::STRUCT) {
    return Status::Invalid("Expected type STRUCT but got ", value->type->id());
  }
  if (!value->is_valid) return Status::Invalid("Got null scalar");
  const auto& holder = checked_cast<const StructScalar&>(*value);
  ARROW_ASSIGN_OR_RAISE(auto name_holder, holder.field("name"));
  ARROW_ASSIGN_OR_RAISE(auto order_holder, holder.field("order"));
  ARROW_ASSIGN_OR_RAISE(auto name, GenericFromScalar<std::string>(name_holder));
  ARROW_ASSIGN_OR_RAISE(auto order, GenericFromScalar<SortOrder>(order_holder));
  return SortKey{std::move(name), order};
}

template <typename T>
static inline enable_if_same_result<T, std::shared_ptr<DataType>> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  return value->type;
}

template <typename T>
static inline enable_if_same_result<T, std::shared_ptr<Scalar>> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  return value;
}

template <typename T>
static inline enable_if_same_result<T, std::shared_ptr<const KeyValueMetadata>>
GenericFromScalar(const std::shared_ptr<Scalar>& value) {
  auto ty = GenericTypeSingleton<std::shared_ptr<const KeyValueMetadata>>();
  if (!value->type->Equals(ty)) {
    return Status::Invalid("Expected ", ty->ToString(), " but got ",
                           value->type->ToString());
  }
  const auto& holder = checked_cast<const MapScalar&>(*value);
  std::vector<std::string> keys;
  std::vector<std::string> values;
  const auto& list = checked_cast<const StructArray&>(*holder.value);
  const auto& key_arr = checked_cast<const BinaryArray&>(*list.field(0));
  const auto& value_arr = checked_cast<const BinaryArray&>(*list.field(1));
  for (int64_t i = 0; i < list.length(); i++) {
    keys.push_back(key_arr.GetString(i));
    values.push_back(value_arr.GetString(i));
  }
  return key_value_metadata(std::move(keys), std::move(values));
}

template <typename T>
static inline enable_if_same_result<T, Datum> GenericFromScalar(
    const std::shared_ptr<Scalar>& value) {
  if (value->type->id() == Type::LIST) {
    const auto& holder = checked_cast<const BaseListScalar&>(*value);
    return holder.value;
  }
  // TODO(ARROW-9434): handle other possible datum kinds by looking for a union
  return Status::Invalid("Cannot deserialize Datum from ", value->ToString());
}

template <typename T>
static enable_if_same<typename CTypeTraits<T>::ArrowType, ListType, Result<T>>
GenericFromScalar(const std::shared_ptr<Scalar>& value) {
  using ValueType = typename T::value_type;
  if (value->type->id() != Type::LIST) {
    return Status::Invalid("Expected type LIST but got ", value->type->ToString());
  }
  const auto& holder = checked_cast<const BaseListScalar&>(*value);
  if (!holder.is_valid) return Status::Invalid("Got null scalar");
  std::vector<ValueType> result;
  for (int i = 0; i < holder.value->length(); i++) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, holder.value->GetScalar(i));
    ARROW_ASSIGN_OR_RAISE(auto v, GenericFromScalar<ValueType>(scalar));
    result.push_back(std::move(v));
  }
  return result;
}

template <typename Options>
struct StringifyImpl {
  template <typename Tuple>
  StringifyImpl(const Options& obj, const Tuple& props)
      : obj_(obj), members_(props.size()) {
    props.ForEach(*this);
  }

  template <typename Property>
  void operator()(const Property& prop, size_t i) {
    std::stringstream ss;
    ss << prop.name() << '=' << GenericToString(prop.get(obj_));
    members_[i] = ss.str();
  }

  std::string Finish() {
    return "{" + arrow::internal::JoinStrings(members_, ", ") + "}";
  }

  const Options& obj_;
  std::vector<std::string> members_;
};

template <typename Options>
struct CompareImpl {
  template <typename Tuple>
  CompareImpl(const Options& l, const Options& r, const Tuple& props)
      : left_(l), right_(r) {
    props.ForEach(*this);
  }

  template <typename Property>
  void operator()(const Property& prop, size_t) {
    equal_ &= GenericEquals(prop.get(left_), prop.get(right_));
  }

  const Options& left_;
  const Options& right_;
  bool equal_ = true;
};

template <typename Options>
struct ToStructScalarImpl {
  template <typename Tuple>
  ToStructScalarImpl(const Options& obj, const Tuple& props,
                     std::vector<std::string>* field_names,
                     std::vector<std::shared_ptr<Scalar>>* values)
      : obj_(obj), field_names_(field_names), values_(values) {
    props.ForEach(*this);
  }

  template <typename Property>
  void operator()(const Property& prop, size_t) {
    if (!status_.ok()) return;
    auto result = GenericToScalar(prop.get(obj_));
    if (!result.ok()) {
      status_ = result.status().WithMessage("Could not serialize field ", prop.name(),
                                            " of options type ", Options::kTypeName, ": ",
                                            result.status().message());
      return;
    }
    field_names_->emplace_back(prop.name());
    values_->push_back(result.MoveValueUnsafe());
  }

  const Options& obj_;
  Status status_;
  std::vector<std::string>* field_names_;
  std::vector<std::shared_ptr<Scalar>>* values_;
};

template <typename Options>
struct FromStructScalarImpl {
  template <typename Tuple>
  FromStructScalarImpl(Options* obj, const StructScalar& scalar, const Tuple& props)
      : obj_(obj), scalar_(scalar) {
    props.ForEach(*this);
  }

  template <typename Property>
  void operator()(const Property& prop, size_t) {
    if (!status_.ok()) return;
    auto maybe_holder = scalar_.field(std::string(prop.name()));
    if (!maybe_holder.ok()) {
      status_ = maybe_holder.status().WithMessage(
          "Cannot deserialize field ", prop.name(), " of options type ",
          Options::kTypeName, ": ", maybe_holder.status().message());
      return;
    }
    auto holder = maybe_holder.MoveValueUnsafe();
    auto result = GenericFromScalar<typename Property::Type>(holder);
    if (!result.ok()) {
      status_ = result.status().WithMessage("Cannot deserialize field ", prop.name(),
                                            " of options type ", Options::kTypeName, ": ",
                                            result.status().message());
      return;
    }
    prop.set(obj_, result.MoveValueUnsafe());
  }

  Options* obj_;
  Status status_;
  const StructScalar& scalar_;
};

template <typename Options, typename... Properties>
const FunctionOptionsType* GetFunctionOptionsType(const Properties&... properties) {
  static const class OptionsType : public GenericOptionsType {
   public:
    explicit OptionsType(const arrow::internal::PropertyTuple<Properties...> properties)
        : properties_(properties) {}

    const char* type_name() const override { return Options::kTypeName; }

    std::string Stringify(const FunctionOptions& options) const override {
      const auto& self = checked_cast<const Options&>(options);
      return StringifyImpl<Options>(self, properties_).Finish();
    }
    bool Compare(const FunctionOptions& options,
                 const FunctionOptions& other) const override {
      const auto& lhs = checked_cast<const Options&>(options);
      const auto& rhs = checked_cast<const Options&>(other);
      return CompareImpl<Options>(lhs, rhs, properties_).equal_;
    }
    Status ToStructScalar(const FunctionOptions& options,
                          std::vector<std::string>* field_names,
                          std::vector<std::shared_ptr<Scalar>>* values) const override {
      const auto& self = checked_cast<const Options&>(options);
      RETURN_NOT_OK(
          ToStructScalarImpl<Options>(self, properties_, field_names, values).status_);
      return Status::OK();
    }
    Result<std::unique_ptr<FunctionOptions>> FromStructScalar(
        const StructScalar& scalar) const override {
      auto options = std::unique_ptr<Options>(new Options());
      RETURN_NOT_OK(
          FromStructScalarImpl<Options>(options.get(), scalar, properties_).status_);
      return std::move(options);
    }

   private:
    const arrow::internal::PropertyTuple<Properties...> properties_;
  } instance(arrow::internal::MakeProperties(properties...));
  return &instance;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
