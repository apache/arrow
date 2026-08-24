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

#include "arrow/extension/tensor_internal.h"

#include <numeric>

#include "arrow/array/array_base.h"
#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/print_internal.h"
#include "arrow/util/sort_internal.h"

namespace arrow::internal {

namespace {

const char* JsonTypeName(simdjson::dom::element_type type) {
  switch (type) {
    case simdjson::dom::element_type::ARRAY:
      return "array";
    case simdjson::dom::element_type::OBJECT:
      return "object";
    case simdjson::dom::element_type::INT64:
    case simdjson::dom::element_type::UINT64:
    case simdjson::dom::element_type::DOUBLE:
    case simdjson::dom::element_type::BIGINT:
      return "number";
    case simdjson::dom::element_type::STRING:
      return "string";
    case simdjson::dom::element_type::BOOL:
      return "boolean";
    case simdjson::dom::element_type::NULL_VALUE:
      return "null";
  }
  return "unknown";
}

Result<simdjson::dom::array> GetJsonArray(simdjson::dom::element value,
                                          std::string_view name) {
  if (!value.is_array()) {
    return Status::Invalid(name, " must be an array, got ", JsonTypeName(value.type()));
  }
  return ResolveSimdjsonResult(value.get_array(), "Failed to get JSON array");
}

Result<int64_t> GetJsonInt(simdjson::dom::element value, std::string_view name,
                           std::string_view expected) {
  if (!value.is_int64()) {
    return Status::Invalid(name, " must contain ", expected, ", got ",
                           JsonTypeName(value.type()));
  }
  return ResolveSimdjsonResult(value.get_int64(), "Failed to get JSON integer");
}

}  // namespace

Result<simdjson::dom::object> ParseJsonObject(simdjson::dom::parser& parser,
                                              const std::string& json) {
  return ResolveSimdjsonResult(parser.parse(json).get_object(),
                               "Invalid serialized JSON data");
}

Result<std::optional<simdjson::dom::element>> GetOptionalJsonField(
    const simdjson::dom::object& object, std::string_view key) {
  auto field = object.at_key(key);
  if (field.error() == simdjson::NO_SUCH_FIELD) {
    return std::nullopt;
  }

  ARROW_ASSIGN_OR_RAISE(
      auto value,
      ResolveSimdjsonResult(std::move(field), "Failed to get JSON object field"));

  return std::optional<simdjson::dom::element>(value);
}

Result<std::vector<int64_t>> GetJsonIntArray(simdjson::dom::element value,
                                             std::string_view name) {
  ARROW_ASSIGN_OR_RAISE(auto array, GetJsonArray(value, name));

  std::vector<int64_t> result;
  result.reserve(array.size());

  for (auto element : array) {
    ARROW_ASSIGN_OR_RAISE(auto number, GetJsonInt(element, name, "integers"));
    result.push_back(number);
  }

  return result;
}

Result<std::vector<std::optional<int64_t>>> GetJsonNullableIntArray(
    simdjson::dom::element value, std::string_view name) {
  ARROW_ASSIGN_OR_RAISE(auto array, GetJsonArray(value, name));

  std::vector<std::optional<int64_t>> result;
  result.reserve(array.size());

  for (auto element : array) {
    if (element.is_null()) {
      result.emplace_back(std::nullopt);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto number, GetJsonInt(element, name, "integers or nulls"));
      result.emplace_back(number);
    }
  }

  return result;
}

Result<std::vector<std::string>> GetJsonStringArray(simdjson::dom::element value,
                                                    std::string_view name) {
  ARROW_ASSIGN_OR_RAISE(auto array, GetJsonArray(value, name));

  std::vector<std::string> result;
  result.reserve(array.size());

  for (auto element : array) {
    if (!element.is_string()) {
      return Status::Invalid(name, " must contain strings, got ",
                             JsonTypeName(element.type()));
    }

    ARROW_ASSIGN_OR_RAISE(
        auto string,
        ResolveSimdjsonResult(element.get_string(), "Failed to get JSON string"));
    result.emplace_back(string);
  }

  return result;
}

Result<int64_t> ComputeShapeProduct(std::span<const int64_t> shape) {
  int64_t product = 1;
  for (const auto dim : shape) {
    if (MultiplyWithOverflow(product, dim, &product)) {
      return Status::Invalid(
          "Product of tensor shape dimensions would not fit in 64-bit integer");
    }
  }
  return product;
}

bool IsPermutationTrivial(std::span<const int64_t> permutation) {
  for (size_t i = 1; i < permutation.size(); ++i) {
    if (permutation[i - 1] + 1 != permutation[i]) {
      return false;
    }
  }
  return true;
}

Status IsPermutationValid(std::span<const int64_t> permutation) {
  const auto size = static_cast<int64_t>(permutation.size());
  std::vector<uint8_t> dim_seen(size, 0);

  for (const auto p : permutation) {
    if (p < 0 || p >= size || dim_seen[p] != 0) {
      return Status::Invalid(
          "Permutation indices for ", size,
          " dimensional tensors must be unique and within [0, ", size - 1,
          "] range. Got: ", ::arrow::internal::PrintVector{permutation, ","});
    }
    dim_seen[p] = 1;
  }
  return Status::OK();
}

Result<std::vector<int64_t>> ComputeStrides(const std::shared_ptr<DataType>& value_type,
                                            std::span<const int64_t> shape,
                                            std::span<const int64_t> permutation) {
  const auto ndim = shape.size();
  const int byte_width = value_type->byte_width();

  // Use identity permutation if none provided
  std::vector<int64_t> perm;
  if (permutation.empty()) {
    perm.resize(ndim);
    std::iota(perm.begin(), perm.end(), 0);
  } else {
    perm.assign(permutation.begin(), permutation.end());
  }

  int64_t remaining = 0;
  if (!shape.empty() && shape[0] > 0) {
    remaining = byte_width;
    for (auto i : perm) {
      if (i > 0) {
        if (MultiplyWithOverflow(remaining, shape[i], &remaining)) {
          return Status::Invalid(
              "Strides computed from shape would not fit in 64-bit integer");
        }
      }
    }
  }

  std::vector<int64_t> strides;
  if (remaining == 0) {
    strides.assign(ndim, byte_width);
    return strides;
  }

  strides.push_back(remaining);
  for (auto i : perm) {
    if (i > 0) {
      remaining /= shape[i];
      strides.push_back(remaining);
    }
  }
  Permute(perm, &strides);

  return strides;
}

Result<std::shared_ptr<Buffer>> SliceTensorBuffer(const Array& data_array,
                                                  const DataType& value_type,
                                                  std::span<const int64_t> shape) {
  const int64_t byte_width = value_type.byte_width();
  ARROW_ASSIGN_OR_RAISE(const int64_t size, ComputeShapeProduct(shape));
  if (size != data_array.length()) {
    return Status::Invalid("Expected data array of length ", size, ", got ",
                           data_array.length());
  }

  int64_t start_position = 0;
  if (MultiplyWithOverflow(data_array.offset(), byte_width, &start_position)) {
    return Status::Invalid("Data offset in bytes would not fit in 64-bit integer");
  }
  int64_t size_bytes = 0;
  if (MultiplyWithOverflow(size, byte_width, &size_bytes)) {
    return Status::Invalid("Tensor byte size would not fit in 64-bit integer");
  }

  return SliceBufferSafe(data_array.data()->buffers[1], start_position, size_bytes);
}

}  // namespace arrow::internal
