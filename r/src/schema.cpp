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
Rcpp::CharacterVector Schema__names(const std::shared_ptr<arrow::Schema>& schema) {
  auto fields = schema->fields();
  return Rcpp::CharacterVector(
    fields.begin(), fields.end(),
    [](const std::shared_ptr<arrow::Field>& field) { return field->name(); });
}

#endif
