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

#include "./arrow_types.h"

#include <arrow/array/array_base.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <arrow/util/byte_size.h>
#include <arrow/util/key_value_metadata.h>

// [[arrow::export]]
int RecordBatch__num_columns(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_columns();
}

// [[arrow::export]]
r_vec_size RecordBatch__num_rows(const std::shared_ptr<arrow::RecordBatch>& x) {
  return r_vec_size(x->num_rows());
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> RecordBatch__schema(
    const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->schema();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__RenameColumns(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::vector<std::string>& names) {
  int n = batch->num_columns();
  if (names.size() != static_cast<size_t>(n)) {
    cpp11::stop("RecordBatch has %d columns but %d names were provided", n, names.size());
  }
  std::vector<std::shared_ptr<arrow::Field>> fields(n);
  for (int i = 0; i < n; i++) {
    fields[i] = batch->schema()->field(i)->WithName(names[i]);
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields));
  return arrow::RecordBatch::Make(schema, batch->num_rows(), batch->columns());
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__ReplaceSchemaMetadata(
    const std::shared_ptr<arrow::RecordBatch>& x, cpp11::strings metadata) {
  auto vec_metadata = cpp11::as_cpp<std::vector<std::string>>(metadata);
  auto names_metadata = cpp11::as_cpp<std::vector<std::string>>(metadata.names());
  auto kv = std::shared_ptr<arrow::KeyValueMetadata>(
      new arrow::KeyValueMetadata(names_metadata, vec_metadata));
  return x->ReplaceSchemaMetadata(kv);
}

// [[arrow::export]]
cpp11::list RecordBatch__columns(const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto nc = batch->num_columns();
  arrow::ArrayVector res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = batch->column(i);
  }
  return arrow::r::to_r_list(res);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> RecordBatch__column(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  arrow::r::validate_index(i, batch->num_columns());
  return batch->column(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> RecordBatch__GetColumnByName(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& name) {
  return batch->GetColumnByName(name);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__SelectColumns(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<int>& indices) {
  return ValueOrStop(batch->SelectColumns(indices));
}

// [[arrow::export]]
bool RecordBatch__Equals(const std::shared_ptr<arrow::RecordBatch>& self,
                         const std::shared_ptr<arrow::RecordBatch>& other,
                         bool check_metadata) {
  return self->Equals(*other, check_metadata);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__AddColumn(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i,
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Array>& column) {
  return ValueOrStop(batch->AddColumn(i, field, column));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__SetColumn(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i,
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::Array>& column) {
  return ValueOrStop(batch->SetColumn(i, field, column));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__RemoveColumn(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  arrow::r::validate_index(i, batch->num_columns());
  return ValueOrStop(batch->RemoveColumn(i));
}

// [[arrow::export]]
std::string RecordBatch__column_name(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     int i) {
  arrow::r::validate_index(i, batch->num_columns());
  return batch->column_name(i);
}

// [[arrow::export]]
cpp11::writable::strings RecordBatch__names(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  int n = batch->num_columns();
  cpp11::writable::strings names(n);
  for (int i = 0; i < n; i++) {
    names[i] = batch->column_name(i);
  }
  return names;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice1(
    const std::shared_ptr<arrow::RecordBatch>& self, R_xlen_t offset) {
  arrow::r::validate_slice_offset(offset, self->num_rows());
  return self->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice2(
    const std::shared_ptr<arrow::RecordBatch>& self, R_xlen_t offset, R_xlen_t length) {
  arrow::r::validate_slice_offset(offset, self->num_rows());
  arrow::r::validate_slice_length(length, self->num_rows() - offset);
  return self->Slice(offset, length);
}

// [[arrow::export]]
cpp11::raws ipc___SerializeRecordBatch__Raw(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  // how many bytes do we need ?
  int64_t size;
  StopIfNotOk(arrow::ipc::GetRecordBatchSize(*batch, &size));

  // allocate the result raw vector
  cpp11::writable::raws out(size);

  // serialize into the bytes of the raw vector
  auto buffer = std::make_shared<arrow::r::RBuffer<cpp11::raws>>(out);
  arrow::io::FixedSizeBufferWriter stream(buffer);
  StopIfNotOk(arrow::ipc::SerializeRecordBatch(
      *batch, arrow::ipc::IpcWriteOptions::Defaults(), &stream));
  StopIfNotOk(stream.Close());

  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ipc___ReadRecordBatch__InputStream__Schema(
    const std::shared_ptr<arrow::io::InputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema) {
  // TODO: promote to function arg
  arrow::ipc::DictionaryMemo memo;
  StopIfNotOk(memo.fields().AddSchemaFields(*schema));
  return ValueOrStop(arrow::ipc::ReadRecordBatch(
      schema, &memo, arrow::ipc::IpcReadOptions::Defaults(), stream.get()));
}

namespace arrow {
namespace r {

arrow::Status check_consistent_array_size(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays, int64_t* num_rows) {
  if (arrays.size()) {
    *num_rows = arrays[0]->length();

    for (const auto& array : arrays) {
      if (array->length() != *num_rows) {
        return arrow::Status::Invalid("All arrays must have the same length");
      }
    }
  }

  return arrow::Status::OK();
}

Status count_fields(SEXP lst, int* out) {
  int res = 0;
  R_xlen_t n = XLENGTH(lst);
  SEXP names = Rf_getAttrib(lst, R_NamesSymbol);
  for (R_xlen_t i = 0; i < n; i++) {
    if (LENGTH(STRING_ELT(names, i)) > 0) {
      ++res;
    } else {
      SEXP x = VECTOR_ELT(lst, i);
      if (Rf_inherits(x, "data.frame")) {
        res += XLENGTH(x);
      } else {
        return Status::RError(
            "only data frames are allowed as unnamed arguments to be auto spliced");
      }
    }
  }
  *out = res;
  return Status::OK();
}

}  // namespace r
}  // namespace arrow

std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays__known_schema(
    const std::shared_ptr<arrow::Schema>& schema, SEXP lst) {
  int num_fields;
  StopIfNotOk(arrow::r::count_fields(lst, &num_fields));

  if (schema->num_fields() != num_fields) {
    cpp11::stop("incompatible. schema has %d fields, and %d arrays are supplied",
                schema->num_fields(), num_fields);
  }

  // convert lst to a vector of arrow::Array
  std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);

  auto fill_array = [&arrays, &schema](int j, SEXP x, std::string name) {
    if (schema->field(j)->name() != name) {
      cpp11::stop("field at index %d has name '%s' != '%s'", j + 1,
                  schema->field(j)->name().c_str(), name.c_str());
    }
    arrays[j] = arrow::r::vec_to_arrow_Array(x, schema->field(j)->type(), false);
  };

  arrow::r::TraverseDots(lst, num_fields, fill_array);

  int64_t num_rows = 0;
  StopIfNotOk(arrow::r::check_consistent_array_size(arrays, &num_rows));
  return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

namespace arrow {
namespace r {

arrow::Status CollectRecordBatchArrays(
    SEXP lst, const std::shared_ptr<arrow::Schema>& schema, int num_fields, bool inferred,
    std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  auto extract_one_array = [&arrays, &schema, inferred](int j, SEXP x, cpp11::r_string) {
    arrays[j] = arrow::r::vec_to_arrow_Array(x, schema->field(j)->type(), inferred);
  };
  arrow::r::TraverseDots(lst, num_fields, extract_one_array);
  return arrow::Status::OK();
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays(SEXP schema_sxp, SEXP lst) {
  bool infer_schema = !Rf_inherits(schema_sxp, "Schema");

  int num_fields;
  StopIfNotOk(arrow::r::count_fields(lst, &num_fields));

  // schema + metadata
  std::shared_ptr<arrow::Schema> schema;
  StopIfNotOk(arrow::r::InferSchemaFromDots(lst, schema_sxp, num_fields, schema));
  StopIfNotOk(arrow::r::AddMetadataFromDots(lst, num_fields, schema));

  // RecordBatch
  if (!infer_schema) {
    return RecordBatch__from_arrays__known_schema(schema, lst);
  }

  // RecordBatch
  std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
  StopIfNotOk(
      arrow::r::CollectRecordBatchArrays(lst, schema, num_fields, infer_schema, arrays));

  // extract number of rows, and check their consistency
  int64_t num_rows = 0;
  StopIfNotOk(arrow::r::check_consistent_array_size(arrays, &num_rows));

  return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

// [[arrow::export]]
r_vec_size RecordBatch__ReferencedBufferSize(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  return r_vec_size(ValueOrStop(arrow::util::ReferencedBufferSize(*batch)));
}
