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

#include <arrow/array/array_base.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

using Rcpp::DataFrame;

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_dataframe(DataFrame tbl) {
  auto rb = RecordBatch__from_dataframe(tbl);

  return ValueOrStop(arrow::Table::FromRecordBatches({std::move(rb)}));
}

// [[arrow::export]]
int Table__num_columns(const std::shared_ptr<arrow::Table>& x) {
  return x->num_columns();
}

// [[arrow::export]]
int Table__num_rows(const std::shared_ptr<arrow::Table>& x) { return x->num_rows(); }

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Table__schema(const std::shared_ptr<arrow::Table>& x) {
  return x->schema();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__ReplaceSchemaMetadata(
    const std::shared_ptr<arrow::Table>& x, Rcpp::CharacterVector metadata) {
  auto kv = std::shared_ptr<arrow::KeyValueMetadata>(new arrow::KeyValueMetadata(
      metadata.names(), Rcpp::as<std::vector<std::string>>(metadata)));
  return x->ReplaceSchemaMetadata(kv);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> Table__column(
    const std::shared_ptr<arrow::Table>& table, int i) {
  arrow::r::validate_index(i, table->num_columns());
  return table->column(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> Table__field(const std::shared_ptr<arrow::Table>& table,
                                           int i) {
  arrow::r::validate_index(i, table->num_columns());
  return table->field(i);
}

// [[arrow::export]]
std::vector<std::shared_ptr<arrow::ChunkedArray>> Table__columns(
    const std::shared_ptr<arrow::Table>& table) {
  auto nc = table->num_columns();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> res(nc);
  for (int i = 0; i < nc; i++) {
    res[i] = table->column(i);
  }
  return res;
}

// [[arrow::export]]
std::vector<std::string> Table__ColumnNames(const std::shared_ptr<arrow::Table>& table) {
  return table->ColumnNames();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Slice1(const std::shared_ptr<arrow::Table>& table,
                                            int offset) {
  arrow::r::validate_slice_offset(offset, table->num_rows());
  return table->Slice(offset);
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__Slice2(const std::shared_ptr<arrow::Table>& table,
                                            int offset, int length) {
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
std::shared_ptr<arrow::Table> Table__select(const std::shared_ptr<arrow::Table>& table,
                                            const Rcpp::IntegerVector& indices) {
  R_xlen_t n = indices.size();

  std::vector<std::shared_ptr<arrow::Field>> fields(n);
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(n);

  for (R_xlen_t i = 0; i < n; i++) {
    int pos = indices[i] - 1;
    fields[i] = table->schema()->field(pos);
    columns[i] = table->column(pos);
  }

  auto schema = std::make_shared<arrow::Schema>(std::move(fields));
  return arrow::Table::Make(schema, columns);
}

bool all_record_batches(SEXP lst) {
  R_xlen_t n = XLENGTH(lst);
  for (R_xlen_t i = 0; i < n; i++) {
    if (!Rf_inherits(VECTOR_ELT(lst, i), "RecordBatch")) return false;
  }
  return true;
}

namespace arrow {
namespace r {

arrow::Status InferSchemaFromDots(SEXP lst, SEXP schema_sxp, int num_fields,
                                  std::shared_ptr<arrow::Schema>& schema) {
  // maybe a schema was given
  if (Rf_inherits(schema_sxp, "Schema")) {
    schema = arrow::r::extract<arrow::Schema>(schema_sxp);
    return arrow::Status::OK();
  }

  if (!Rf_isNull(schema_sxp)) {
    return arrow::Status::RError("`schema` must be an arrow::Schema or NULL");
  }

  // infer the schema from the `...`
  std::vector<std::shared_ptr<arrow::Field>> fields(num_fields);

  auto extract_one_field = [&fields](int j, SEXP x, SEXP name) {
    // Make sure we're ingesting UTF-8
    name = Rf_mkCharCE(Rf_translateCharUTF8(name), CE_UTF8);
    if (Rf_inherits(x, "ChunkedArray")) {
      fields[j] =
          arrow::field(CHAR(name), arrow::r::extract<arrow::ChunkedArray>(x)->type());
    } else if (Rf_inherits(x, "Array")) {
      fields[j] = arrow::field(CHAR(name), arrow::r::extract<arrow::Array>(x)->type());
    } else {
      // TODO: we just need the type at this point
      fields[j] = arrow::field(CHAR(name), arrow::r::InferArrowType(x));
    }
  };
  arrow::r::TraverseDots(lst, num_fields, extract_one_field);

  schema = std::make_shared<arrow::Schema>(std::move(fields));

  return arrow::Status::OK();
}

SEXP CollectColumnMetadata(SEXP lst, int num_fields, bool& has_metadata) {
  // Preallocate for the lambda to fill in
  SEXP metadata_columns = PROTECT(Rf_allocVector(VECSXP, num_fields));
  SEXP metadata_columns_names = PROTECT(Rf_allocVector(STRSXP, num_fields));
  Rf_setAttrib(metadata_columns, R_NamesSymbol, metadata_columns_names);

  auto extract_one_metadata = [&metadata_columns, &metadata_columns_names, &has_metadata](
                                  int j, SEXP x, SEXP name) {
    // Make sure we're ingesting UTF-8
    name = Rf_mkCharCE(Rf_translateCharUTF8(name), CE_UTF8);
    SET_STRING_ELT(metadata_columns_names, j, name);
    // no metadata for arrow R6 objects
    if (Rf_inherits(x, "ArrowObject")) {
      return;
    }

    bool this_has_metadata = false;
    SEXP att = ATTRIB(x);
    if (!Rf_isNull(att) || Rf_inherits(x, "data.frame")) {
      // Each field in columns is also: list(attributes=list(), columns=namedList(fields))
      // Only nested types will have columns though
      SEXP r_meta = PROTECT(Rf_allocVector(VECSXP, 2));
      Rf_setAttrib(r_meta, R_NamesSymbol, arrow::r::data::names_metadata);
      if (!Rf_isNull(att)) {
        SEXP att_list =
            PROTECT(Rf_eval(Rf_lang2(arrow::r::symbols::as_list, att), R_GlobalEnv));

        // Pop off attributes that are already preserved in the Arrow types
        std::vector<std::string> bad_fields = {};
        auto class_attr = Rf_getAttrib(x, R_ClassSymbol);
        if (Rf_length(class_attr) == 1) {
          std::string class_name(CHAR(STRING_ELT(class_attr, 0)));
          if (class_name == "factor") {
            bad_fields = {"class", "levels"};
          } else if (class_name == "Date" || class_name == "integer64") {
            bad_fields = {"class"};
          } else if (class_name == "data.frame") {
            bad_fields = {"class", "names", "row.names"};  // TODO: preserve row names?
          }
        } else if (Rf_inherits(x, "tbl_df")) {
          // TODO: what about subclass of tibble?
          bad_fields = {"class", "names", "row.names"};
        } else if (Rf_inherits(x, "data.frame")) {
          bad_fields = {"names", "row.names"};  // TODO: preserve row names?
        } else if (Rf_inherits(x, "POSIXct")) {
          // Note that "tzone" is optional so it may not exist
          bad_fields = {"class", "tzone"};
        } else if (Rf_inherits(x, "hms") && Rf_inherits(x, "difftime")) {
          bad_fields = {"class", "units"};
        }
        if (Rf_length(att_list) > (int)bad_fields.size()) {
          // If the fields we should exclude are the only ones we have,
          // there's nothing to do. Otherwise, set what we have.
          SET_VECTOR_ELT(r_meta, 0, att_list);
          this_has_metadata = true;
          // TODO: We could do something like this to just drop those fields
          // if (bad_fields.size() > 0) {
          //   // Make a new list without them
          //   R_xlen_t new_size = Rf_length(att_list) - bad_fields.size();
          //   SEXP new_list = PROTECT(Rf_allocVector(VECSXP, new_size));
          //   SEXP new_list_names = PROTECT(Rf_allocVector(STRSXP, new_size));
          //   Rf_setAttrib(new_list, R_NamesSymbol, new_list_names);
          //
          //   SEXP att_list_names = Rf_getAttrib(att_list, R_NamesSymbol);
          //   SEXP old_name;
          //   R_xlen_t new_i = 0;
          //   for (R_xlen_t name_i = 0; name_i < Rf_length(att_list_names); name_i++) {
          //     old_name = STRING_ELT(att_list_names, name_i);
          //     if (old_name not in bad_fields) {  // TODO, obviously
          //       SET_VECTOR_ELT(new_list, new_i, VECTOR_ELT(att_list, name_i));
          //       SET_STRING_ELT(new_list_names, new_i, old_name);
          //       new_i++;
          //     }
          //   }
          //   att_list = new_list;
          //   UNPROTECT(2);
          // }
        }
        UNPROTECT(1);
      }
      if (Rf_inherits(x, "data.frame")) {
        int inner_num_fields;
        StopIfNotOk(arrow::r::count_fields(x, &inner_num_fields));
        SET_VECTOR_ELT(r_meta, 1,
                       CollectColumnMetadata(x, inner_num_fields, has_metadata));
        UNPROTECT(2);  // CollectColumnMetadata adds 2 PROTECTS
        this_has_metadata = true;
      }
      if (this_has_metadata) {
        SET_VECTOR_ELT(metadata_columns, j, r_meta);
        has_metadata = true;
      }
      UNPROTECT(1);
    }
  };

  arrow::r::TraverseDots(lst, num_fields, extract_one_metadata);
  return metadata_columns;
}

arrow::Status AddMetadataFromDots(SEXP lst, int num_fields,
                                  std::shared_ptr<arrow::Schema>& schema) {
  // Preallocate the r_metadata object: list(attributes=list(), columns=namedList(fields))
  SEXP metadata = PROTECT(Rf_allocVector(VECSXP, 2));
  Rf_setAttrib(metadata, R_NamesSymbol, arrow::r::data::names_metadata);

  bool has_metadata = false;
  // TODO: we want to keep any top-level data-frame attributes
  // but the auto-splice code has stripped them out by the time we get here
  // https://issues.apache.org/jira/browse/ARROW-9271
  //
  // SEXP att = ATTRIB(lst);
  // if (!Rf_isNull(att)) {
  //   SEXP att_list_call = PROTECT(Rf_lang2(arrow::r::symbols::as_list, att));
  //   SET_VECTOR_ELT(metadata, 0, PROTECT(Rf_eval(att_list_call, R_GlobalEnv)));
  //   UNPROTECT(2);
  //   has_metadata = true;
  // }
  SET_VECTOR_ELT(metadata, 1, CollectColumnMetadata(lst, num_fields, has_metadata));
  UNPROTECT(2);  // CollectColumnMetadata adds 2 PROTECTS

  if (has_metadata) {
    SEXP serialise_call =
        PROTECT(Rf_lang2(arrow::r::symbols::serialize_arrow_r_metadata, metadata));
    SEXP serialised = PROTECT(Rf_eval(serialise_call, arrow::r::ns::arrow));

    schema = schema->WithMetadata(
        arrow::key_value_metadata({"r"}, {CHAR(STRING_ELT(serialised, 0))}));

    UNPROTECT(2);
  }
  UNPROTECT(1);

  return arrow::Status::OK();
}

arrow::Status CollectTableColumns(
    SEXP lst, const std::shared_ptr<arrow::Schema>& schema, int num_fields, bool inferred,
    std::vector<std::shared_ptr<arrow::ChunkedArray>>& columns) {
  auto extract_one_column = [&columns, &schema, inferred](int j, SEXP x, SEXP name) {
    if (Rf_inherits(x, "ChunkedArray")) {
      columns[j] = arrow::r::extract<arrow::ChunkedArray>(x);
    } else if (Rf_inherits(x, "Array")) {
      columns[j] =
          std::make_shared<arrow::ChunkedArray>(arrow::r::extract<arrow::Array>(x));
    } else {
      auto array = arrow::r::Array__from_vector(x, schema->field(j)->type(), inferred);
      columns[j] = std::make_shared<arrow::ChunkedArray>(array);
    }
  };
  arrow::r::TraverseDots(lst, num_fields, extract_one_column);
  return arrow::Status::OK();
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_dots(SEXP lst, SEXP schema_sxp) {
  bool infer_schema = !Rf_inherits(schema_sxp, "Schema");

  if (all_record_batches(lst)) {
    auto batches = arrow::r::List_to_shared_ptr_vector<arrow::RecordBatch>(lst);
    std::shared_ptr<arrow::Table> tab;

    if (infer_schema) {
      tab = ValueOrStop(arrow::Table::FromRecordBatches(std::move(batches)));
    } else {
      auto schema = arrow::r::extract<arrow::Schema>(schema_sxp);
      tab = ValueOrStop(arrow::Table::FromRecordBatches(schema, std::move(batches)));
    }

    return tab;
  }

  int num_fields;
  StopIfNotOk(arrow::r::count_fields(lst, &num_fields));

  // schema + metadata
  std::shared_ptr<arrow::Schema> schema;
  StopIfNotOk(arrow::r::InferSchemaFromDots(lst, schema_sxp, num_fields, schema));
  StopIfNotOk(arrow::r::AddMetadataFromDots(lst, num_fields, schema));

  // table
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(num_fields);
  StopIfNotOk(
      arrow::r::CollectTableColumns(lst, schema, num_fields, infer_schema, columns));

  return arrow::Table::Make(schema, columns);
}

#endif
