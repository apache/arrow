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

#include "arrow/compute/function_internal.h"

#include "arrow/array/util.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {
namespace internal {
using ::arrow::internal::checked_cast;

constexpr char kTypeNameField[] = "_type_name";

Result<std::shared_ptr<StructScalar>> FunctionOptionsToStructScalar(
    const FunctionOptions& options) {
  std::vector<std::string> field_names;
  std::vector<std::shared_ptr<Scalar>> values;
  const auto* options_type =
      dynamic_cast<const GenericOptionsType*>(options.options_type());
  if (!options_type) {
    return Status::NotImplemented("serializing ", options.type_name(),
                                  " to StructScalar");
  }
  RETURN_NOT_OK(options_type->ToStructScalar(options, &field_names, &values));
  field_names.push_back(kTypeNameField);
  const char* options_name = options.type_name();
  values.emplace_back(
      new BinaryScalar(Buffer::Wrap(options_name, std::strlen(options_name))));
  return StructScalar::Make(std::move(values), std::move(field_names));
}

Result<std::unique_ptr<FunctionOptions>> FunctionOptionsFromStructScalar(
    const StructScalar& scalar) {
  ARROW_ASSIGN_OR_RAISE(auto type_name_holder, scalar.field(kTypeNameField));
  const std::string type_name =
      checked_cast<const BinaryScalar&>(*type_name_holder).value->ToString();
  ARROW_ASSIGN_OR_RAISE(auto raw_options_type,
                        GetFunctionRegistry()->GetFunctionOptionsType(type_name));
  const auto* options_type = checked_cast<const GenericOptionsType*>(raw_options_type);
  return options_type->FromStructScalar(scalar);
}

Result<std::shared_ptr<Buffer>> GenericOptionsType::Serialize(
    const FunctionOptions& options) const {
  ARROW_ASSIGN_OR_RAISE(auto scalar, FunctionOptionsToStructScalar(options));
  ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*scalar, 1));
  auto batch =
      RecordBatch::Make(schema({field("", array->type())}), /*num_rows=*/1, {array});
  ARROW_ASSIGN_OR_RAISE(auto stream, io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, ipc::MakeFileWriter(stream, batch->schema()));
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  RETURN_NOT_OK(writer->Close());
  return stream->Finish();
}

Result<std::unique_ptr<FunctionOptions>> GenericOptionsType::Deserialize(
    const Buffer& buffer) const {
  return DeserializeFunctionOptions(buffer);
}

Result<std::unique_ptr<FunctionOptions>> DeserializeFunctionOptions(
    const Buffer& buffer) {
  io::BufferReader stream(buffer);
  ARROW_ASSIGN_OR_RAISE(auto reader, ipc::RecordBatchFileReader::Open(&stream));
  ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(0));
  if (batch->num_rows() != 1) {
    return Status::Invalid(
        "serialized FunctionOptions's batch repr was not a single row - had ",
        batch->num_rows());
  }
  if (batch->num_columns() != 1) {
    return Status::Invalid(
        "serialized FunctionOptions's batch repr was not a single column - had ",
        batch->num_columns());
  }
  auto column = batch->column(0);
  if (column->type()->id() != Type::STRUCT) {
    return Status::Invalid(
        "serialized FunctionOptions's batch repr was not a struct column - was ",
        column->type()->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto raw_scalar,
                        checked_cast<const StructArray&>(*column).GetScalar(0));
  auto scalar = checked_cast<const StructScalar&>(*raw_scalar);
  return FunctionOptionsFromStructScalar(scalar);
}

Status CheckAllArrayOrScalar(const std::vector<Datum>& values) {
  for (const auto& value : values) {
    if (!value.is_value()) {
      return Status::TypeError(
          "Tried executing function with non-array, non-scalar type: ", value.ToString());
    }
  }
  return Status::OK();
}

Result<std::vector<TypeHolder>> GetFunctionArgumentTypes(const std::vector<Datum>& args) {
  // type-check Datum arguments here. Really we'd like to avoid this as much as
  // possible
  RETURN_NOT_OK(CheckAllArrayOrScalar(args));
  std::vector<TypeHolder> inputs(args.size());
  for (size_t i = 0; i != args.size(); ++i) {
    inputs[i] = TypeHolder(args[i].type());
  }
  return inputs;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
