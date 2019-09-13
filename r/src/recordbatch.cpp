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

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

// [[arrow::export]]
int RecordBatch__num_columns(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_columns();
}

// [[arrow::export]]
int RecordBatch__num_rows(const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->num_rows();
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> RecordBatch__schema(
    const std::shared_ptr<arrow::RecordBatch>& x) {
  return x->schema();
}

// [[arrow::export]]
arrow::ArrayVector RecordBatch__columns(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto nc = batch->num_columns();
  arrow::ArrayVector res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = batch->column(i);
  }
  return res;
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> RecordBatch__column(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  return batch->column(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_dataframe(Rcpp::DataFrame tbl) {
  Rcpp::CharacterVector names = tbl.names();

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  for (int i = 0; i < tbl.size(); i++) {
    SEXP x = tbl[i];
    arrays.push_back(Array__from_vector(x, R_NilValue));
    fields.push_back(
        std::make_shared<arrow::Field>(std::string(names[i]), arrays[i]->type()));
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields));

  return arrow::RecordBatch::Make(schema, tbl.nrow(), std::move(arrays));
}

// [[arrow::export]]
bool RecordBatch__Equals(const std::shared_ptr<arrow::RecordBatch>& self,
                         const std::shared_ptr<arrow::RecordBatch>& other) {
  return self->Equals(*other);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__RemoveColumn(
    const std::shared_ptr<arrow::RecordBatch>& batch, int i) {
  std::shared_ptr<arrow::RecordBatch> res;
  STOP_IF_NOT_OK(batch->RemoveColumn(i, &res));
  return res;
}

// [[arrow::export]]
std::string RecordBatch__column_name(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     int i) {
  return batch->column_name(i);
}

// [[arrow::export]]
Rcpp::CharacterVector RecordBatch__names(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  int n = batch->num_columns();
  Rcpp::CharacterVector names(n);
  for (int i = 0; i < n; i++) {
    names[i] = batch->column_name(i);
  }
  return names;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice1(
    const std::shared_ptr<arrow::RecordBatch>& self, int offset) {
  return self->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__Slice2(
    const std::shared_ptr<arrow::RecordBatch>& self, int offset, int length) {
  return self->Slice(offset, length);
}

// [[arrow::export]]
Rcpp::RawVector ipc___SerializeRecordBatch__Raw(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  // how many bytes do we need ?
  int64_t size;
  STOP_IF_NOT_OK(arrow::ipc::GetRecordBatchSize(*batch, &size));

  // allocate the result raw vector
  Rcpp::RawVector out(Rcpp::no_init(size));

  // serialize into the bytes of the raw vector
  auto buffer = std::make_shared<arrow::r::RBuffer<RAWSXP, Rcpp::RawVector>>(out);
  arrow::io::FixedSizeBufferWriter stream(buffer);
  STOP_IF_NOT_OK(
      arrow::ipc::SerializeRecordBatch(*batch, arrow::default_memory_pool(), &stream));
  STOP_IF_NOT_OK(stream.Close());

  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ipc___ReadRecordBatch__InputStream__Schema(
    const std::shared_ptr<arrow::io::InputStream>& stream,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<arrow::RecordBatch> batch;
  // TODO: promote to function arg
  arrow::ipc::DictionaryMemo memo;
  STOP_IF_NOT_OK(arrow::ipc::ReadRecordBatch(schema, &memo, stream.get(), &batch));
  return batch;
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
  STOP_IF_NOT_OK(arrow::r::count_fields(lst, &num_fields));

  if (schema->num_fields() != num_fields) {
    Rcpp::stop("incompatible. schema has %d fields, and %d arrays are supplied",
               schema->num_fields(), num_fields);
  }

  // convert lst to a vector of arrow::Array
  std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
  SEXP names = Rf_getAttrib(lst, R_NamesSymbol);

  auto fill_array = [&arrays, &schema](int j, SEXP x, SEXP name) {
    if (schema->field(j)->name() != CHAR(name)) {
      Rcpp::stop("field at index %d has name '%s' != '%s'", j + 1,
                 schema->field(j)->name(), CHAR(name));
    }
    arrays[j] = arrow::r::Array__from_vector(x, schema->field(j)->type(), false);
  };

  for (R_xlen_t i = 0, j = 0; j < num_fields; i++) {
    SEXP name_i = STRING_ELT(names, i);
    SEXP x_i = VECTOR_ELT(lst, i);

    if (LENGTH(name_i) == 0) {
      SEXP names_x_i = Rf_getAttrib(x_i, R_NamesSymbol);
      for (R_xlen_t k = 0; k < XLENGTH(x_i); k++, j++) {
        fill_array(j, VECTOR_ELT(x_i, k), STRING_ELT(names_x_i, k));
      }
    } else {
      fill_array(j, x_i, name_i);
      j++;
    }
  }

  int64_t num_rows = 0;
  STOP_IF_NOT_OK(arrow::r::check_consistent_array_size(arrays, &num_rows));
  return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays(SEXP schema_sxp, SEXP lst) {
  if (Rf_inherits(schema_sxp, "Schema")) {
    return RecordBatch__from_arrays__known_schema(
        arrow::r::extract<arrow::Schema>(schema_sxp), lst);
  }

  int num_fields;
  STOP_IF_NOT_OK(arrow::r::count_fields(lst, &num_fields));

  // convert lst to a vector of arrow::Array
  std::vector<std::shared_ptr<arrow::Array>> arrays(num_fields);
  std::vector<std::string> arrays_names(num_fields);
  SEXP names = Rf_getAttrib(lst, R_NamesSymbol);

  auto fill_array = [&arrays, &arrays_names](int j, SEXP x, SEXP name) {
    arrays[j] = Array__from_vector(x, R_NilValue);
    arrays_names[j] = CHAR(name);
  };

  for (R_xlen_t i = 0, j = 0; j < num_fields; i++) {
    SEXP name_i = STRING_ELT(names, i);
    SEXP x_i = VECTOR_ELT(lst, i);
    if (LENGTH(name_i) == 0) {
      SEXP names_x_i = Rf_getAttrib(x_i, R_NamesSymbol);
      for (R_xlen_t k = 0; k < XLENGTH(x_i); k++, j++) {
        fill_array(j, VECTOR_ELT(x_i, k), STRING_ELT(names_x_i, k));
      }
    } else {
      fill_array(j, x_i, name_i);
      j++;
    }
  }

  // generate schema from the types that have been infered
  std::shared_ptr<arrow::Schema> schema;

  std::vector<std::shared_ptr<arrow::Field>> fields(num_fields);
  for (R_xlen_t i = 0; i < num_fields; i++) {
    fields[i] = std::make_shared<arrow::Field>(arrays_names[i], arrays[i]->type());
  }
  schema = std::make_shared<arrow::Schema>(std::move(fields));

  // check all sizes are the same
  int64_t num_rows = 0;
  STOP_IF_NOT_OK(arrow::r::check_consistent_array_size(arrays, &num_rows));

  return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

#endif
