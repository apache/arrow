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

#include "arrow/matlab/array/proxy/struct_array.h"
#include "arrow/matlab/bit/pack.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/index/validate.h"
#include "arrow/matlab/proxy/wrap.h"
#include "arrow/util/utf8.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::array::proxy {

StructArray::StructArray(std::shared_ptr<arrow::StructArray> struct_array)
    : proxy::Array{std::move(struct_array)} {
  REGISTER_METHOD(StructArray, getNumFields);
  REGISTER_METHOD(StructArray, getFieldByIndex);
  REGISTER_METHOD(StructArray, getFieldByName);
  REGISTER_METHOD(StructArray, getFieldNames);
}

libmexclass::proxy::MakeResult StructArray::make(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  namespace mda = ::matlab::data;
  using libmexclass::proxy::ProxyManager;

  mda::StructArray opts = constructor_arguments[0];
  const mda::TypedArray<uint64_t> arrow_array_proxy_ids = opts[0]["ArrayProxyIDs"];
  const mda::StringArray field_names_mda = opts[0]["FieldNames"];
  const mda::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];

  std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
  arrow_arrays.reserve(arrow_array_proxy_ids.getNumberOfElements());

  // Retrieve all of the Arrow Array Proxy instances from the libmexclass ProxyManager.
  for (const auto& arrow_array_proxy_id : arrow_array_proxy_ids) {
    auto proxy = ProxyManager::getProxy(arrow_array_proxy_id);
    auto arrow_array_proxy = std::static_pointer_cast<proxy::Array>(proxy);
    auto arrow_array = arrow_array_proxy->unwrap();
    arrow_arrays.push_back(arrow_array);
  }

  // Convert the utf-16 encoded field names into utf-8 encoded strings
  std::vector<std::string> field_names;
  field_names.reserve(field_names_mda.getNumberOfElements());
  for (const auto& field_name : field_names_mda) {
    const auto field_name_utf16 = std::u16string(field_name);
    MATLAB_ASSIGN_OR_ERROR(const auto field_name_utf8,
                           arrow::util::UTF16StringToUTF8(field_name_utf16),
                           error::UNICODE_CONVERSION_ERROR_ID);
    field_names.push_back(field_name_utf8);
  }

  // Pack the validity bitmap values.
  MATLAB_ASSIGN_OR_ERROR(auto validity_bitmap_buffer, bit::packValid(validity_bitmap_mda),
                         error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

  // Create the StructArray
  MATLAB_ASSIGN_OR_ERROR(
      auto array,
      arrow::StructArray::Make(arrow_arrays, field_names, validity_bitmap_buffer),
      error::STRUCT_ARRAY_MAKE_FAILED);

  // Construct the StructArray Proxy
  auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
  return std::make_shared<proxy::StructArray>(std::move(struct_array));
}

void StructArray::getNumFields(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;

  mda::ArrayFactory factory;
  const auto num_fields = array->type()->num_fields();
  context.outputs[0] = factory.createScalar(num_fields);
}

void StructArray::getFieldByIndex(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using namespace libmexclass::proxy;

  mda::StructArray args = context.inputs[0];
  const mda::TypedArray<int32_t> index_mda = args[0]["Index"];
  const auto matlab_index = int32_t(index_mda[0]);

  auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);

  const auto num_fields = struct_array->type()->num_fields();

  // Validate there is at least 1 field
  MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(index::validateNonEmptyContainer(num_fields),
                                      context, error::INDEX_EMPTY_CONTAINER);

  // Validate the matlab index provided is within the range [1, num_fields]
  MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(index::validateInRange(matlab_index, num_fields),
                                      context, error::INDEX_OUT_OF_RANGE);

  // Note: MATLAB uses 1-based indexing, so subtract 1.
  const int32_t index = matlab_index - 1;

  auto field_array = struct_array->field(index);
  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(context.outputs[0],
                                      arrow::matlab::proxy::wrap_and_manage(field_array),
                                      context, error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);
}

void StructArray::getFieldByName(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using libmexclass::proxy::ProxyManager;

  mda::StructArray args = context.inputs[0];

  const mda::StringArray name_mda = args[0]["Name"];
  const auto name_utf16 = std::u16string(name_mda[0]);
  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto name,
                                      arrow::util::UTF16StringToUTF8(name_utf16), context,
                                      error::UNICODE_CONVERSION_ERROR_ID);

  auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
  auto field_array = struct_array->GetFieldByName(name);
  if (!field_array) {
    // Return an error if we could not query the field by name.
    const auto msg = "Could not find field named " + name + ".";
    context.error =
        libmexclass::error::Error{error::ARROW_TABULAR_SCHEMA_AMBIGUOUS_FIELD_NAME, msg};
    return;
  }

  // Wrap the array within a proxy object if possible.
  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(context.outputs[0],
                                      arrow::matlab::proxy::wrap_and_manage(field_array),
                                      context, error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);
}

void StructArray::getFieldNames(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;

  const auto& fields = array->type()->fields();
  const auto num_fields = fields.size();
  std::vector<mda::MATLABString> names;
  names.reserve(num_fields);

  for (size_t i = 0; i < num_fields; ++i) {
    auto str_utf8 = fields[i]->name();

    // MATLAB strings are UTF-16 encoded. Must convert UTF-8
    // encoded field names before returning to MATLAB.
    MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto str_utf16,
                                        arrow::util::UTF8StringToUTF16(str_utf8), context,
                                        error::UNICODE_CONVERSION_ERROR_ID);
    const mda::MATLABString matlab_string = mda::MATLABString(std::move(str_utf16));
    names.push_back(matlab_string);
  }

  mda::ArrayFactory factory;
  context.outputs[0] = factory.createArray({1, num_fields}, names.begin(), names.end());
}
}  // namespace arrow::matlab::array::proxy
