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
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

// [[arrow::export]]
std::shared_ptr<arrow::Schema> schema_(Rcpp::List fields) {
  return arrow::schema(arrow::r::List_to_shared_ptr_vector<arrow::Field>(fields));
}

// [[arrow::export]]
std::string Schema__ToString(const std::shared_ptr<arrow::Schema>& s) {
  return s->ToString();
}

// [[arrow::export]]
int Schema__num_fields(const std::shared_ptr<arrow::Schema>& s) {
  return s->num_fields();
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> Schema__field(const std::shared_ptr<arrow::Schema>& s,
                                            int i) {
  if (i >= s->num_fields() || i < 0) {
    Rcpp::stop("Invalid field index for schema.");
  }

  return s->field(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> Schema__GetFieldByName(
    const std::shared_ptr<arrow::Schema>& s, std::string x) {
  return s->GetFieldByName(x);
}

// [[arrow::export]]
std::vector<std::shared_ptr<arrow::Field>> Schema__fields(
    const std::shared_ptr<arrow::Schema>& schema) {
  return schema->fields();
}

// [[arrow::export]]
std::vector<std::string> Schema__field_names(
    const std::shared_ptr<arrow::Schema>& schema) {
  return schema->field_names();
}

// [[arrow::export]]
bool Schema__HasMetadata(const std::shared_ptr<arrow::Schema>& schema) {
  return schema->HasMetadata();
}

// [[arrow::export]]
Rcpp::List Schema__metadata(const std::shared_ptr<arrow::Schema>& schema) {
  auto meta = schema->metadata();
  int64_t n = 0;
  if (schema->HasMetadata()) {
    n = meta->size();
  }

  Rcpp::List out(n);
  std::vector<std::string> names_out(n);

  for (int i = 0; i < n; i++) {
    auto key = meta->key(i);
    out[i] = meta->value(i);
    if (key == "r") {
      Rf_setAttrib(out[i], R_ClassSymbol, arrow::r::data::classes_metadata_r);
    }
    names_out[i] = key;
  }
  out.attr("names") = names_out;
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__WithMetadata(
    const std::shared_ptr<arrow::Schema>& schema, Rcpp::CharacterVector metadata) {
  auto kv = std::shared_ptr<arrow::KeyValueMetadata>(new arrow::KeyValueMetadata(
      metadata.names(), Rcpp::as<std::vector<std::string>>(metadata)));
  return schema->WithMetadata(kv);
}

// [[arrow::export]]
Rcpp::RawVector Schema__serialize(const std::shared_ptr<arrow::Schema>& schema) {
  arrow::ipc::DictionaryMemo empty_memo;
  std::shared_ptr<arrow::Buffer> out =
      ValueOrStop(arrow::ipc::SerializeSchema(*schema, &empty_memo));

  auto n = out->size();
  Rcpp::RawVector vec(out->size());
  std::copy_n(out->data(), n, vec.begin());

  return vec;
}

// [[arrow::export]]
bool Schema__Equals(const std::shared_ptr<arrow::Schema>& schema,
                    const std::shared_ptr<arrow::Schema>& other, bool check_metadata) {
  return schema->Equals(*other, check_metadata);
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> arrow__UnifySchemas(
    const std::vector<std::shared_ptr<arrow::Schema>>& schemas) {
  return ValueOrStop(arrow::UnifySchemas(schemas));
}

#endif
