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

#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__from_fields(
    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
  return arrow::schema(fields);
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__from_list(cpp11::list field_list) {
  int n = field_list.size();

  bool nullable = true;
  cpp11::strings names(field_list.attr(R_NamesSymbol));

  std::vector<std::shared_ptr<arrow::Field>> fields(n);

  for (int i = 0; i < n; i++) {
    fields[i] = arrow::field(
        names[i], cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(field_list[i]),
        nullable);
  }
  return arrow::schema(fields);
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
    cpp11::stop("Invalid field index for schema.");
  }

  return s->field(i);
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__AddField(
    const std::shared_ptr<arrow::Schema>& s, int i,
    const std::shared_ptr<arrow::Field>& field) {
  return ValueOrStop(s->AddField(i, field));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__SetField(
    const std::shared_ptr<arrow::Schema>& s, int i,
    const std::shared_ptr<arrow::Field>& field) {
  return ValueOrStop(s->SetField(i, field));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__RemoveField(
    const std::shared_ptr<arrow::Schema>& s, int i) {
  return ValueOrStop(s->RemoveField(i));
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> Schema__GetFieldByName(
    const std::shared_ptr<arrow::Schema>& s, std::string x) {
  return s->GetFieldByName(x);
}

// [[arrow::export]]
cpp11::list Schema__fields(const std::shared_ptr<arrow::Schema>& schema) {
  return arrow::r::to_r_list(schema->fields());
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
cpp11::writable::list Schema__metadata(const std::shared_ptr<arrow::Schema>& schema) {
  auto meta = schema->metadata();
  int64_t n = 0;
  if (schema->HasMetadata()) {
    n = meta->size();
  }

  cpp11::writable::list out(n);
  std::vector<std::string> names_out(n);

  for (int i = 0; i < n; i++) {
    auto key = meta->key(i);
    out[i] = cpp11::as_sexp(meta->value(i));
    if (key == "r") {
      Rf_classgets(out[i], arrow::r::data::classes_metadata_r);
    }
    names_out[i] = key;
  }
  out.names() = names_out;
  return out;
}

std::shared_ptr<arrow::KeyValueMetadata> strings_to_kvm(cpp11::strings metadata) {
  auto values = cpp11::as_cpp<std::vector<std::string>>(metadata);
  auto names = cpp11::as_cpp<std::vector<std::string>>(metadata.attr("names"));

  return std::make_shared<arrow::KeyValueMetadata>(std::move(names), std::move(values));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> Schema__WithMetadata(
    const std::shared_ptr<arrow::Schema>& schema, cpp11::strings metadata) {
  auto kv = strings_to_kvm(metadata);
  return schema->WithMetadata(std::move(kv));
}

// [[arrow::export]]
cpp11::writable::raws Schema__serialize(const std::shared_ptr<arrow::Schema>& schema) {
  auto out = ValueOrStop(arrow::ipc::SerializeSchema(*schema));
  auto n = out->size();
  return cpp11::writable::raws(out->data(), out->data() + n);
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
