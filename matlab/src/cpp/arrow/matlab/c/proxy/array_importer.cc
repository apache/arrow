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

#include "arrow/array.h"
#include "arrow/c/bridge.h"

#include "arrow/matlab/c/proxy/array_importer.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/proxy/wrap.h"

namespace arrow::matlab::c::proxy {

ArrayImporter::ArrayImporter() { REGISTER_METHOD(ArrayImporter, import); }

libmexclass::proxy::MakeResult ArrayImporter::make(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  return std::make_shared<ArrayImporter>();
}

void ArrayImporter::import(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;

  mda::StructArray args = context.inputs[0];
  const mda::TypedArray<uint64_t> arrow_array_address_mda = args[0]["ArrowArrayAddress"];
  const mda::TypedArray<uint64_t> arrow_schema_address_mda =
      args[0]["ArrowSchemaAddress"];

  const auto arrow_array_address = uint64_t(arrow_array_address_mda[0]);
  const auto arrow_schema_address = uint64_t(arrow_schema_address_mda[0]);

  auto arrow_array = reinterpret_cast<struct ArrowArray*>(arrow_array_address);
  auto arrow_schema = reinterpret_cast<struct ArrowSchema*>(arrow_schema_address);

  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto array,
                                      arrow::ImportArray(arrow_array, arrow_schema),
                                      context, error::C_IMPORT_FAILED);

  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(context.outputs[0],
                                      arrow::matlab::proxy::wrap_and_manage(array),
                                      context, error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);
}

}  // namespace arrow::matlab::c::proxy
