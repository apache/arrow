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
#include <arrow/table.h>
#include <arrow/util/byte_size.h>
#include <arrow/util/key_value_metadata.h>

// [[arrow::export]]
r_vec_size Table__num_columns(const std::shared_ptr<arrow::Table>& x) {
  return r_vec_size(x->num_columns());
}

// [[arrow::export]]
r_vec_size Table__num_rows(const std::shared_ptr<arrow::Table>& x) {
  return r_vec_size(x->num_rows());
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Table__schema(const std::shared_ptr<arrow::Table>& x) {
  return x->schema();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__ReplaceSchemaMetadata(
    const std::shared_ptr<arrow::Table>& x, cpp11::strings metadata) {
  auto vec_metadata = cpp11::as_cpp<std::vector<std::string>>(metadata);
  auto names_metadata = cpp11::as_cpp<std::vector<std::string>>(metadata.names());
  auto kv = std::shared_ptr<arrow::KeyValueMetadata>(
      new arrow::KeyValueMetadata(names_metadata, vec_metadata));
  return x->ReplaceSchemaMetadata(kv);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> Table__column(
    const std::shared_ptr<arrow::Table>& table, R_xlen_t i) {
  arrow::r::validate_index(i, table->num_columns());
  return table->column(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> Table__field(const std::shared_ptr<arrow::Table>& table,
                                           R_xlen_t i) {
  arrow::r::validate_index(i, table->num_columns());
  return table->field(i);
}

// [[arrow::export]]
cpp11::list Table__columns(const std::shared_ptr<arrow::Table>& table) {
  auto nc = table->num_columns();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = table->column(i);
  }
  return arrow::r::to_r_list(res);
}

// [[arrow::export]]
std::vector<std::string> Table__ColumnNames(const std::shared_ptr<arrow::Table>& table) {
  return table->ColumnNames();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__RenameColumns(
    const std::shared_ptr<arrow::Table>& table, const std::vector<std::string>& names) {
  return ValueOrStop(table->RenameColumns(names));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Slice1(const std::shared_ptr<arrow::Table>& table,
                                            R_xlen_t offset) {
  arrow::r::validate_slice_offset(offset, table->num_rows());
  return table->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Slice2(const std::shared_ptr<arrow::Table>& table,
                                            R_xlen_t offset, R_xlen_t length) {
  arrow::r::validate_slice_offset(offset, table->num_rows());
  arrow::r::validate_slice_length(length, table->num_rows() - offset);
  return table->Slice(offset, length);
}

// [[arrow::export]]
bool Table__Equals(const std::shared_ptr<arrow::Table>& lhs,
                   const std::shared_ptr<arrow::Table>& rhs, bool check_metadata) {
  return lhs->Equals(*rhs.get(), check_metadata);
}

// [[arrow::export]]
bool Table__Validate(const std::shared_ptr<arrow::Table>& table) {
  StopIfNotOk(table->Validate());
  return true;
}

// [[arrow::export]]
bool Table__ValidateFull(const std::shared_ptr<arrow::Table>& table) {
  StopIfNotOk(table->ValidateFull());
  return true;
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> Table__GetColumnByName(
    const std::shared_ptr<arrow::Table>& table, const std::string& name) {
  return table->GetColumnByName(name);
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__RemoveColumn(
    const std::shared_ptr<arrow::Table>& table, R_xlen_t i) {
  return ValueOrStop(table->RemoveColumn(i));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__AddColumn(
    const std::shared_ptr<arrow::Table>& table, R_xlen_t i,
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::ChunkedArray>& column) {
  return ValueOrStop(table->AddColumn(i, field, column));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__SetColumn(
    const std::shared_ptr<arrow::Table>& table, R_xlen_t i,
    const std::shared_ptr<arrow::Field>& field,
    const std::shared_ptr<arrow::ChunkedArray>& column) {
  return ValueOrStop(table->SetColumn(i, field, column));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__SelectColumns(
    const std::shared_ptr<arrow::Table>& table, const std::vector<int>& indices) {
  return ValueOrStop(table->SelectColumns(indices));
}

namespace arrow {
namespace r {

arrow::Status InferSchemaFromDots(SEXP lst, SEXP schema_sxp, int num_fields,
                                  std::shared_ptr<arrow::Schema>& schema) {
  // maybe a schema was given
  if (Rf_inherits(schema_sxp, "Schema")) {
    schema = cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(schema_sxp);
    return arrow::Status::OK();
  }

  if (!Rf_isNull(schema_sxp)) {
    return arrow::Status::RError("`schema` must be an arrow::Schema or NULL");
  }

  // infer the schema from the `...`
  std::vector<std::shared_ptr<arrow::Field>> fields(num_fields);

  auto extract_one_field = [&fields](int j, SEXP x, std::string name) {
    if (Rf_inherits(x, "ChunkedArray")) {
      fields[j] = arrow::field(
          name, cpp11::as_cpp<std::shared_ptr<arrow::ChunkedArray>>(x)->type());
    } else if (Rf_inherits(x, "Array")) {
      fields[j] =
          arrow::field(name, cpp11::as_cpp<std::shared_ptr<arrow::Array>>(x)->type());
    } else {
      // TODO: we just need the type at this point
      fields[j] = arrow::field(name, arrow::r::InferArrowType(x));
    }
  };
  arrow::r::TraverseDots(lst, num_fields, extract_one_field);

  schema = std::make_shared<arrow::Schema>(std::move(fields));

  return arrow::Status::OK();
}

SEXP arrow_attributes(SEXP x, bool only_top_level) {
  SEXP call = PROTECT(
      Rf_lang3(arrow::r::symbols::arrow_attributes, x, Rf_ScalarLogical(only_top_level)));
  SEXP att = Rf_eval(call, arrow::r::ns::arrow);
  UNPROTECT(1);
  return att;
}

cpp11::writable::list CollectColumnMetadata(SEXP lst, int num_fields) {
  // Preallocate for the lambda to fill in
  cpp11::writable::list metadata_columns(num_fields);

  cpp11::writable::strings metadata_columns_names(num_fields);

  auto extract_one_metadata = [&metadata_columns, &metadata_columns_names](
                                  int j, SEXP x, std::string name) {
    metadata_columns_names[j] = name;

    // no metadata for arrow R6 objects
    if (Rf_inherits(x, "ArrowObject")) {
      return;
    }
    metadata_columns[j] = arrow_attributes(x, false);
  };
  arrow::r::TraverseDots(lst, num_fields, extract_one_metadata);

  metadata_columns.names() = metadata_columns_names;
  return metadata_columns;
}

arrow::Status AddMetadataFromDots(SEXP lst, int num_fields,
                                  std::shared_ptr<arrow::Schema>& schema) {
  // Preallocate the r_metadata object: list(attributes=list(), columns=namedList(fields))

  cpp11::writable::list metadata(2);
  metadata.names() = arrow::r::data::names_metadata;

  bool has_top_level_metadata = false;

  // "top level" attributes, only relevant if the first object is not named and a data
  // frame
  cpp11::strings names = Rf_getAttrib(lst, R_NamesSymbol);
  if (names[0] == "" && Rf_inherits(VECTOR_ELT(lst, 0), "data.frame")) {
    SEXP top_level = metadata[0] = arrow_attributes(VECTOR_ELT(lst, 0), true);
    if (!Rf_isNull(top_level) && XLENGTH(top_level) > 0) {
      has_top_level_metadata = true;
    }
  }

  // recurse to get all columns metadata
  cpp11::writable::list metadata_columns = CollectColumnMetadata(lst, num_fields);

  // Remove metadata for ExtensionType columns, because these have their own mechanism for
  // preserving R type information
  for (R_xlen_t i = 0; i < schema->num_fields(); i++) {
    if (schema->field(i)->type()->id() == Type::EXTENSION) {
      metadata_columns[i] = R_NilValue;
    }
  }

  // If all metadata_columns are NULL and there is no top-level metadata, set has_metadata
  // to false
  bool has_metadata = has_top_level_metadata;
  for (R_xlen_t i = 0; i < metadata_columns.size(); i++) {
    if (metadata_columns[i] != R_NilValue) {
      has_metadata = true;
      break;
    }
  }

  // Assign to the output metadata
  metadata[1] = metadata_columns;

  if (has_metadata) {
    SEXP serialise_call =
        PROTECT(Rf_lang2(arrow::r::symbols::serialize_arrow_r_metadata, metadata));
    SEXP serialised = PROTECT(Rf_eval(serialise_call, arrow::r::ns::arrow));

    schema = schema->WithMetadata(
        arrow::key_value_metadata({"r"}, {CHAR(STRING_ELT(serialised, 0))}));

    UNPROTECT(2);
  }

  return arrow::Status::OK();
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
bool all_record_batches(SEXP lst) {
  R_xlen_t n = XLENGTH(lst);
  for (R_xlen_t i = 0; i < n; i++) {
    if (!Rf_inherits(VECTOR_ELT(lst, i), "RecordBatch")) return false;
  }
  return true;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_record_batches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, SEXP schema_sxp) {
  bool infer_schema = !Rf_inherits(schema_sxp, "Schema");

  std::shared_ptr<arrow::Table> tab;

  if (infer_schema) {
    tab = ValueOrStop(arrow::Table::FromRecordBatches(std::move(batches)));
  } else {
    auto schema = cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(schema_sxp);
    tab = ValueOrStop(arrow::Table::FromRecordBatches(schema, std::move(batches)));
  }

  return tab;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_schema(
    const std::shared_ptr<arrow::Schema>& schema) {
  int64_t num_fields = schema->num_fields();

  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(num_fields);
  for (int i = 0; i < num_fields; i++) {
    auto maybe_column = arrow::ChunkedArray::Make({}, schema->field(i)->type());
    columns[i] = ValueOrStop(maybe_column);
  }

  return (arrow::Table::Make(schema, std::move(columns)));
}

// [[arrow::export]]
r_vec_size Table__ReferencedBufferSize(const std::shared_ptr<arrow::Table>& table) {
  return r_vec_size(ValueOrStop(arrow::util::ReferencedBufferSize(*table)));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__ConcatenateTables(
    const std::vector<std::shared_ptr<arrow::Table>>& tables, bool unify_schemas) {
  arrow::ConcatenateTablesOptions options;
  options.unify_schemas = unify_schemas;
  return ValueOrStop(arrow::ConcatenateTables(tables, options));
}
