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

#include <sstream>

#include <simdjson.h>

#include "arrow/extension/tensor_internal.h"
#include "arrow/extension/variable_shape_tensor.h"

#include "arrow/array/array_primitive.h"
#include "arrow/json/json_writer_internal.h"
#include "arrow/scalar.h"
#include "arrow/tensor.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/print_internal.h"
#include "arrow/util/simdjson_internal.h"
#include "arrow/util/sort_internal.h"
#include "arrow/util/string.h"

using ::arrow::json::JsonWriter;

namespace arrow::extension {

bool VariableShapeTensorType::ExtensionEquals(const ExtensionType& other) const {
  if (extension_name() != other.extension_name()) {
    return false;
  }
  const auto& other_ext = internal::checked_cast<const VariableShapeTensorType&>(other);
  if (this->ndim() != other_ext.ndim()) {
    return false;
  }

  const bool permutation_equivalent =
      (permutation_ == other_ext.permutation()) ||
      (internal::IsPermutationTrivial(permutation_) &&
       internal::IsPermutationTrivial(other_ext.permutation()));

  return (storage_type()->Equals(other_ext.storage_type())) &&
         (dim_names_ == other_ext.dim_names()) &&
         (uniform_shape_ == other_ext.uniform_shape()) && permutation_equivalent;
}

std::string VariableShapeTensorType::ToString(bool show_metadata) const {
  std::stringstream ss;
  ss << "extension<" << this->extension_name()
     << "[value_type=" << value_type_->ToString(show_metadata) << ", ndim=" << ndim_;

  if (!permutation_.empty()) {
    ss << ", permutation=" << ::arrow::internal::PrintVector{permutation_, ","};
  }
  if (!dim_names_.empty()) {
    ss << ", dim_names=[" << internal::JoinStrings(dim_names_, ",") << "]";
  }
  if (!uniform_shape_.empty()) {
    std::vector<std::string> uniform_shape;
    for (const auto& v : uniform_shape_) {
      if (v.has_value()) {
        uniform_shape.emplace_back(std::to_string(v.value()));
      } else {
        uniform_shape.emplace_back("null");
      }
    }
    ss << ", uniform_shape=[" << internal::JoinStrings(uniform_shape, ",") << "]";
  }
  ss << "]>";
  return ss.str();
}

std::string VariableShapeTensorType::Serialize() const {
  JsonWriter writer;

  writer.StartObject();

  if (!permutation_.empty()) {
    writer.Key("permutation");
    writer.StartArray();
    for (auto v : permutation_) {
      writer.Int64(v);
    }
    writer.EndArray();
  }

  if (!dim_names_.empty()) {
    writer.Key("dim_names");
    writer.StartArray();
    for (const auto& v : dim_names_) {
      writer.String(v);
    }
    writer.EndArray();
  }

  if (!uniform_shape_.empty()) {
    writer.Key("uniform_shape");
    writer.StartArray();
    for (const auto& v : uniform_shape_) {
      if (v.has_value()) {
        writer.Int64(*v);
      } else {
        writer.Null();
      }
    }
    writer.EndArray();
  }

  writer.EndObject();

  Result<std::string_view> json = writer.GetString();
  // can only fail in OutOfMemory scenarios
  ARROW_CHECK_OK(json.status());
  return std::string(*json);
}

Result<std::shared_ptr<DataType>> VariableShapeTensorType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized_data) const {
  if (storage_type->id() != Type::STRUCT) {
    return Status::Invalid("Expected Struct storage type, got ",
                           storage_type->ToString());
  }
  if (storage_type->num_fields() != 2) {
    return Status::Invalid("Expected Struct storage type with 2 fields, got ",
                           storage_type->num_fields());
  }
  if (storage_type->field(0)->type()->id() != Type::LIST) {
    return Status::Invalid("Expected List storage type, got ",
                           storage_type->field(0)->type()->ToString());
  }
  if (storage_type->field(1)->type()->id() != Type::FIXED_SIZE_LIST) {
    return Status::Invalid("Expected FixedSizeList storage type, got ",
                           storage_type->field(1)->type()->ToString());
  }
  if (internal::checked_cast<const FixedSizeListType&>(*storage_type->field(1)->type())
          .value_type() != int32()) {
    return Status::Invalid("Expected FixedSizeList value type int32, got ",
                           storage_type->field(1)->type()->ToString());
  }

  const auto value_type = storage_type->field(0)->type()->field(0)->type();
  const int32_t ndim =
      internal::checked_cast<const FixedSizeListType&>(*storage_type->field(1)->type())
          .list_size();

  simdjson::padded_string padded_json(serialized_data);
  simdjson::ondemand::parser parser;

  ARROW_ASSIGN_OR_RAISE(auto document,
                        internal::ResolveSimdjsonResult(parser.iterate(padded_json),
                                                        "Invalid serialized JSON data"));

  ARROW_ASSIGN_OR_RAISE(auto object,
                        internal::ResolveSimdjsonResult(document.get_object(),
                                                        "Invalid serialized JSON data"));

  std::vector<int64_t> permutation;
  std::vector<std::string> dim_names;
  std::vector<std::optional<int64_t>> uniform_shape;

  for (auto field_result : object) {
    ARROW_ASSIGN_OR_RAISE(auto field, internal::ResolveSimdjsonResult(
                                          field_result, "Invalid serialized JSON data"));

    ARROW_ASSIGN_OR_RAISE(
        auto key, internal::ResolveSimdjsonResult(field.unescaped_key(),
                                                  "Failed to get JSON object key"));

    auto value = field.value();

    if (key == "permutation") {
      ARROW_ASSIGN_OR_RAISE(auto type, internal::ResolveSimdjsonResult(
                                           value.type(), "Invalid serialized JSON data"));

      if (type != simdjson::ondemand::json_type::array) {
        return Status::Invalid("permutation must be an array, got ",
                               internal::JsonTypeName(type));
      }

      ARROW_ASSIGN_OR_RAISE(
          auto array, internal::ResolveSimdjsonResult(value.get_array(),
                                                      "Failed to get permutation array"));

      permutation.reserve(ndim);

      for (auto element_result : array) {
        ARROW_ASSIGN_OR_RAISE(auto element,
                              internal::ResolveSimdjsonResult(
                                  element_result, "Failed to iterate permutation array"));

        ARROW_ASSIGN_OR_RAISE(
            auto element_type,
            internal::ResolveSimdjsonResult(
                element.type(), "Failed to determine permutation element JSON type"));

        if (element_type != simdjson::ondemand::json_type::number) {
          return Status::Invalid("permutation must contain integers, got ",
                                 internal::JsonTypeName(element_type));
        }

        ARROW_ASSIGN_OR_RAISE(auto number_type,
                              internal::ResolveSimdjsonResult(
                                  element.get_number_type(),
                                  "Failed to determine permutation number type"));

        if (number_type != simdjson::ondemand::number_type::signed_integer) {
          return Status::Invalid("permutation must contain integers, got number");
        }

        ARROW_ASSIGN_OR_RAISE(
            auto number, internal::ResolveSimdjsonResult(
                             element.get_int64(), "Failed to get permutation integer"));

        permutation.emplace_back(number);
      }

      RETURN_NOT_OK(internal::IsPermutationValid(permutation));

    } else if (key == "dim_names") {
      ARROW_ASSIGN_OR_RAISE(auto type, internal::ResolveSimdjsonResult(
                                           value.type(), "Invalid serialized JSON data"));

      if (type != simdjson::ondemand::json_type::array) {
        return Status::Invalid("dim_names must be an array, got ",
                               internal::JsonTypeName(type));
      }

      ARROW_ASSIGN_OR_RAISE(
          auto array, internal::ResolveSimdjsonResult(value.get_array(),
                                                      "Failed to get dim_names array"));

      dim_names.reserve(ndim);

      for (auto element_result : array) {
        ARROW_ASSIGN_OR_RAISE(auto element,
                              internal::ResolveSimdjsonResult(
                                  element_result, "Failed to iterate dim_names array"));

        ARROW_ASSIGN_OR_RAISE(
            auto element_type,
            internal::ResolveSimdjsonResult(
                element.type(), "Failed to determine dim_names element JSON type"));

        if (element_type != simdjson::ondemand::json_type::string) {
          return Status::Invalid("dim_names must contain strings, got ",
                                 internal::JsonTypeName(element_type));
        }

        ARROW_ASSIGN_OR_RAISE(auto name,
                              internal::ResolveSimdjsonResult(element.get_string(),
                                                              "Failed to get dim_name"));

        dim_names.emplace_back(name);
      }

      if (dim_names.size() != static_cast<size_t>(ndim)) {
        return Status::Invalid("Invalid: dim_names");
      }

    } else if (key == "uniform_shape") {
      ARROW_ASSIGN_OR_RAISE(auto type, internal::ResolveSimdjsonResult(
                                           value.type(), "Invalid serialized JSON data"));

      if (type != simdjson::ondemand::json_type::array) {
        return Status::Invalid("uniform_shape must be an array, got ",
                               internal::JsonTypeName(type));
      }

      ARROW_ASSIGN_OR_RAISE(auto array,
                            internal::ResolveSimdjsonResult(
                                value.get_array(), "Failed to get uniform_shape array"));

      uniform_shape.reserve(ndim);

      for (auto element_result : array) {
        ARROW_ASSIGN_OR_RAISE(
            auto element, internal::ResolveSimdjsonResult(
                              element_result, "Failed to iterate uniform_shape array"));

        ARROW_ASSIGN_OR_RAISE(
            auto element_type,
            internal::ResolveSimdjsonResult(
                element.type(), "Failed to determine uniform_shape element JSON type"));

        if (element_type == simdjson::ondemand::json_type::null) {
          uniform_shape.emplace_back(std::nullopt);
          continue;
        }

        if (element_type != simdjson::ondemand::json_type::number) {
          return Status::Invalid("uniform_shape must contain integers or nulls, got ",
                                 internal::JsonTypeName(element_type));
        }

        ARROW_ASSIGN_OR_RAISE(auto number_type,
                              internal::ResolveSimdjsonResult(
                                  element.get_number_type(),
                                  "Failed to determine uniform_shape number type"));

        if (number_type != simdjson::ondemand::number_type::signed_integer) {
          return Status::Invalid(
              "uniform_shape must contain integers or nulls, got number");
        }

        ARROW_ASSIGN_OR_RAISE(
            auto number, internal::ResolveSimdjsonResult(
                             element.get_int64(), "Failed to get uniform_shape integer"));

        uniform_shape.emplace_back(number);
      }

      if (uniform_shape.size() != static_cast<size_t>(ndim)) {
        return Status::Invalid("Invalid: uniform_shape");
      }
    }
  }

  return VariableShapeTensorType::Make(value_type, ndim, permutation, dim_names,
                                       uniform_shape);
}

std::shared_ptr<Array> VariableShapeTensorType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("arrow.variable_shape_tensor",
            internal::checked_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<VariableShapeTensorArray>(data);
}

Result<std::shared_ptr<Tensor>> VariableShapeTensorType::MakeTensor(
    const std::shared_ptr<ExtensionScalar>& scalar) {
  const auto& tensor_scalar = internal::checked_cast<const StructScalar&>(*scalar->value);
  const auto& ext_type =
      internal::checked_cast<const VariableShapeTensorType&>(*scalar->type);

  if (!tensor_scalar.is_valid) {
    return Status::Invalid("Cannot convert null scalar to Tensor.");
  }
  ARROW_ASSIGN_OR_RAISE(const auto data_scalar, tensor_scalar.field(0));
  ARROW_ASSIGN_OR_RAISE(const auto shape_scalar, tensor_scalar.field(1));
  const auto data_array =
      internal::checked_pointer_cast<BaseListScalar>(data_scalar)->value;
  const auto shape_array = internal::checked_pointer_cast<Int32Array>(
      internal::checked_pointer_cast<FixedSizeListScalar>(shape_scalar)->value);

  const auto& value_type =
      internal::checked_cast<const FixedWidthType&>(*ext_type.value_type());

  if (data_array->null_count() > 0) {
    return Status::Invalid("Cannot convert data with nulls to Tensor.");
  }

  auto permutation = ext_type.permutation();
  if (permutation.empty()) {
    permutation.resize(ext_type.ndim());
    std::iota(permutation.begin(), permutation.end(), 0);
  }

  if (shape_array->length() != ext_type.ndim()) {
    return Status::Invalid("Expected shape array of length ", ext_type.ndim(), ", got ",
                           shape_array->length());
  }
  std::vector<int64_t> shape;
  shape.reserve(ext_type.ndim());
  for (int64_t j = 0; j < static_cast<int64_t>(ext_type.ndim()); ++j) {
    const auto size_value = shape_array->Value(j);
    if (size_value < 0) {
      return Status::Invalid("shape must have non-negative values");
    }
    shape.push_back(size_value);
  }

  std::vector<std::string> dim_names = ext_type.dim_names();
  if (!dim_names.empty()) {
    internal::Permute<std::string>(permutation, &dim_names);
  }

  ARROW_ASSIGN_OR_RAISE(
      auto strides, internal::ComputeStrides(ext_type.value_type(), shape, permutation));
  internal::Permute<int64_t>(permutation, &shape);

  ARROW_ASSIGN_OR_RAISE(const auto buffer,
                        internal::SliceTensorBuffer(*data_array, value_type, shape));

  return Tensor::Make(ext_type.value_type(), buffer, shape, strides, dim_names);
}

Result<std::shared_ptr<DataType>> VariableShapeTensorType::Make(
    const std::shared_ptr<DataType>& value_type, int32_t ndim,
    std::vector<int64_t> permutation, std::vector<std::string> dim_names,
    std::vector<std::optional<int64_t>> uniform_shape) {
  if (!is_fixed_width(*value_type)) {
    return Status::Invalid("Cannot convert non-fixed-width values to Tensor.");
  }
  if (ndim < 0) {
    return Status::Invalid("ndim must be non-negative. Got: ", ndim);
  }

  if (!dim_names.empty() && dim_names.size() != static_cast<size_t>(ndim)) {
    return Status::Invalid("dim_names size must match ndim. Expected: ", ndim,
                           " Got: ", dim_names.size());
  }
  if (!uniform_shape.empty() && uniform_shape.size() != static_cast<size_t>(ndim)) {
    return Status::Invalid("uniform_shape size must match ndim. Expected: ", ndim,
                           " Got: ", uniform_shape.size());
  }
  if (!uniform_shape.empty()) {
    for (const auto& v : uniform_shape) {
      if (v.has_value() && v.value() < 0) {
        return Status::Invalid("uniform_shape must have non-negative values");
      }
    }
  }
  if (!permutation.empty()) {
    if (permutation.size() != static_cast<size_t>(ndim)) {
      return Status::Invalid("permutation size must match ndim. Expected: ", ndim,
                             " Got: ", permutation.size());
    }
    RETURN_NOT_OK(internal::IsPermutationValid(permutation));
  }

  return std::make_shared<VariableShapeTensorType>(
      value_type, ndim, std::move(permutation), std::move(dim_names),
      std::move(uniform_shape));
}

std::shared_ptr<DataType> variable_shape_tensor(
    const std::shared_ptr<DataType>& value_type, int32_t ndim,
    std::vector<int64_t> permutation, std::vector<std::string> dim_names,
    std::vector<std::optional<int64_t>> uniform_shape) {
  auto maybe_type =
      VariableShapeTensorType::Make(value_type, ndim, std::move(permutation),
                                    std::move(dim_names), std::move(uniform_shape));
  ARROW_CHECK_OK(maybe_type.status());
  return maybe_type.MoveValueUnsafe();
}

}  // namespace arrow::extension
