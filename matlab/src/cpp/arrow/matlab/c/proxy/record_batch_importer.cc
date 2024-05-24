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

#include "arrow/c/bridge.h"

#include "arrow/matlab/c/proxy/record_batch_importer.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::c::proxy {

RecordBatchImporter::RecordBatchImporter() {
  REGISTER_METHOD(RecordBatchImporter, import);
}

libmexclass::proxy::MakeResult RecordBatchImporter::make(
    const libmexclass::proxy::FunctionArguments& constructor_arguments) {
  return std::make_shared<RecordBatchImporter>();
}

void RecordBatchImporter::import(libmexclass::proxy::method::Context& context) {
  namespace mda = ::matlab::data;
  using namespace libmexclass::proxy;
  using RecordBatchProxy = arrow::matlab::tabular::proxy::RecordBatch;

  mda::StructArray args = context.inputs[0];
  const mda::TypedArray<uint64_t> arrow_array_address_mda = args[0]["ArrowArrayAddress"];
  const mda::TypedArray<uint64_t> arrow_schema_address_mda =
      args[0]["ArrowSchemaAddress"];

  const auto arrow_array_address = uint64_t(arrow_array_address_mda[0]);
  const auto arrow_schema_address = uint64_t(arrow_schema_address_mda[0]);

  auto arrow_array = reinterpret_cast<struct ArrowArray*>(arrow_array_address);
  auto arrow_schema = reinterpret_cast<struct ArrowSchema*>(arrow_schema_address);

  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto record_batch,
                                      arrow::ImportRecordBatch(arrow_array, arrow_schema),
                                      context, error::C_IMPORT_FAILED);

  auto record_batch_proxy = std::make_shared<RecordBatchProxy>(std::move(record_batch));

  mda::ArrayFactory factory;
  const auto record_batch_proxy_id = ProxyManager::manageProxy(record_batch_proxy);
  const auto record_batch_proxy_id_mda = factory.createScalar(record_batch_proxy_id);

  context.outputs[0] = record_batch_proxy_id_mda;
}

}  // namespace arrow::matlab::c::proxy
