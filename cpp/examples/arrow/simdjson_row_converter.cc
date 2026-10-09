// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/table_builder.h>
#include <arrow/util/iterator.h>

#include <simdjson.h>

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Transforming dynamic row data into Arrow data
// When building connectors to other data systems, it's common to receive data in
// row-based structures. While the row_wise_conversion_example.cc shows how to
// handle this conversion for fixed schemas, this example demonstrates how to
// convert between row-based JSON data and Arrow data for arbitrary schemas.
//
// As an example, this conversion is between JSON strings and Arrow tables.
//
// We use the following helpers and patterns here:
//  * simdjson's DOM API for parsing JSON rows
//  * arrow::RecordBatchBuilder for constructing Arrow arrays from row data
//  * arrow::TableBatchReader and Arrow iterators for converting Arrow tables back
//    into row-based JSON data

// Convert a simdjson result into an arrow::Result, so that errors are reported
// as arrow::Status rather than as exceptions.
template <typename T>
arrow::Result<T> Unwrap(simdjson::simdjson_result<T> result, const char* what) {
  T value{};
  simdjson::error_code error = std::move(result).get(value);
  if (error) {
    return arrow::Status::Invalid(what, ": ", simdjson::error_message(error));
  }
  return value;
}

// Look up a field of a JSON object. Returns std::nullopt if the field is missing.
arrow::Result<std::optional<simdjson::dom::element>> GetField(
    simdjson::dom::object object, std::string_view name) {
  simdjson::dom::element value;
  simdjson::error_code error = object.at_key(name).get(value);
  if (error == simdjson::NO_SUCH_FIELD) {
    return std::nullopt;
  }
  if (error) {
    return arrow::Status::Invalid("Failed to get JSON field '", name,
                                  "': ", simdjson::error_message(error));
  }
  return value;
}

// Append a JSON value to an Arrow builder according to the expected Arrow type.
// This example only handles the types used by the example schema; extend this
// switch when adapting it to schemas with additional Arrow types.
arrow::Status AppendJsonValue(simdjson::dom::element value,
                              const std::shared_ptr<arrow::DataType>& type,
                              arrow::ArrayBuilder* builder);

arrow::Status AppendJsonStruct(simdjson::dom::element value,
                               const arrow::StructType& type,
                               arrow::StructBuilder* builder) {
  ARROW_ASSIGN_OR_RAISE(auto object,
                        Unwrap(value.get_object(), "Failed to get JSON object"));

  for (int i = 0; i < type.num_fields(); ++i) {
    const auto& field = type.field(i);

    ARROW_ASSIGN_OR_RAISE(auto child, GetField(object, field->name()));

    if (!child.has_value()) {
      ARROW_RETURN_NOT_OK(builder->child_builder(i)->AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(
          AppendJsonValue(*child, field->type(), builder->child_builder(i).get()));
    }
  }

  return builder->Append();
}

arrow::Status AppendJsonList(simdjson::dom::element value, const arrow::ListType& type,
                             arrow::ListBuilder* builder) {
  ARROW_ASSIGN_OR_RAISE(auto array,
                        Unwrap(value.get_array(), "Failed to get JSON array"));

  ARROW_RETURN_NOT_OK(builder->Append());

  for (simdjson::dom::element element : array) {
    ARROW_RETURN_NOT_OK(
        AppendJsonValue(element, type.value_field()->type(), builder->value_builder()));
  }

  return arrow::Status::OK();
}

arrow::Status AppendJsonValue(simdjson::dom::element value,
                              const std::shared_ptr<arrow::DataType>& type,
                              arrow::ArrayBuilder* builder) {
  if (value.is_null()) {
    return builder->AppendNull();
  }

  switch (type->id()) {
    case arrow::Type::INT64: {
      ARROW_ASSIGN_OR_RAISE(auto number,
                            Unwrap(value.get_int64(), "Failed to get JSON integer"));
      return static_cast<arrow::Int64Builder*>(builder)->Append(number);
    }

    case arrow::Type::DOUBLE: {
      ARROW_ASSIGN_OR_RAISE(auto number,
                            Unwrap(value.get_double(), "Failed to get JSON double"));
      return static_cast<arrow::DoubleBuilder*>(builder)->Append(number);
    }

    case arrow::Type::STRING: {
      ARROW_ASSIGN_OR_RAISE(auto string,
                            Unwrap(value.get_string(), "Failed to get JSON string"));
      return static_cast<arrow::StringBuilder*>(builder)->Append(string);
    }

    case arrow::Type::BOOL: {
      ARROW_ASSIGN_OR_RAISE(auto boolean,
                            Unwrap(value.get_bool(), "Failed to get JSON boolean"));
      return static_cast<arrow::BooleanBuilder*>(builder)->Append(boolean);
    }

    case arrow::Type::STRUCT:
      return AppendJsonStruct(value, *static_cast<const arrow::StructType*>(type.get()),
                              static_cast<arrow::StructBuilder*>(builder));

    case arrow::Type::LIST:
      return AppendJsonList(value, *static_cast<const arrow::ListType*>(type.get()),
                            static_cast<arrow::ListBuilder*>(builder));

    default:
      return arrow::Status::NotImplemented(
          "Cannot convert JSON value to Arrow array of type ", type->ToString());
  }
}  // AppendJsonValue

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
    const std::vector<std::string>& rows, std::shared_ptr<arrow::Schema> schema) {
  // RecordBatchBuilder will create array builders for us for each field in our
  // schema. By passing the number of output rows (`rows.size()`) we can
  // pre-allocate the correct size of arrays, except of course in the case of
  // string, byte, and list arrays, which have dynamic lengths.
  std::unique_ptr<arrow::RecordBatchBuilder> batch_builder;
  ARROW_ASSIGN_OR_RAISE(
      batch_builder,
      arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), rows.size()));

  // DOM elements are only valid until the next parse() on the same parser, so each
  // row is fully appended to the builders before the next row is parsed.
  simdjson::dom::parser parser;

  // Parse each row and append its values to the corresponding Arrow builders.
  for (const auto& json : rows) {
    ARROW_ASSIGN_OR_RAISE(auto element,
                          Unwrap(parser.parse(json), "Failed to parse JSON row"));
    ARROW_ASSIGN_OR_RAISE(auto object,
                          Unwrap(element.get_object(), "Expected a JSON object"));

    for (int i = 0; i < schema->num_fields(); ++i) {
      const auto& field = schema->field(i);
      arrow::ArrayBuilder* builder = batch_builder->GetField(i);

      ARROW_ASSIGN_OR_RAISE(auto value, GetField(object, field->name()));

      if (!value.has_value()) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(AppendJsonValue(*value, field->type(), builder));
      }
    }
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_ASSIGN_OR_RAISE(batch, batch_builder->Flush());

  // Use RecordBatch::ValidateFull() to make sure arrays were correctly constructed.
  ARROW_RETURN_NOT_OK(batch->ValidateFull());
  return batch;
}  // ConvertToRecordBatch

// Append `value` to `out` as a quoted and escaped JSON string.
void AppendJsonString(std::string_view value, std::string* out) {
  out->push_back('"');
  for (unsigned char c : value) {
    switch (c) {
      case '"':
        out->append("\\\"");
        break;
      case '\\':
        out->append("\\\\");
        break;
      case '\n':
        out->append("\\n");
        break;
      case '\r':
        out->append("\\r");
        break;
      case '\t':
        out->append("\\t");
        break;
      default:
        if (c < 0x20) {
          char buffer[8];
          std::snprintf(buffer, sizeof(buffer), "\\u%04x", c);
          out->append(buffer);
        } else {
          out->push_back(static_cast<char>(c));
        }
    }
  }
  out->push_back('"');
}

// Write an Arrow value as JSON according to its Arrow type.
// This example only handles the types used by the example schema; extend this
// switch when adapting it to schemas with additional Arrow types.
arrow::Status WriteJsonValue(const arrow::Array& array, int64_t index, std::string* out);

arrow::Status WriteJsonStruct(const arrow::StructArray& array, int64_t index,
                              std::string* out) {
  const arrow::StructType& type = *array.struct_type();

  out->push_back('{');
  for (int i = 0; i < type.num_fields(); ++i) {
    if (i > 0) {
      out->push_back(',');
    }
    AppendJsonString(type.field(i)->name(), out);
    out->push_back(':');
    ARROW_RETURN_NOT_OK(WriteJsonValue(*array.field(i), index, out));
  }
  out->push_back('}');
  return arrow::Status::OK();
}

arrow::Status WriteJsonList(const arrow::ListArray& array, int64_t index,
                            std::string* out) {
  const int64_t offset = array.value_offset(index);
  const int64_t length = array.value_length(index);
  const auto& values = *array.values();

  out->push_back('[');
  for (int64_t i = 0; i < length; ++i) {
    if (i > 0) {
      out->push_back(',');
    }
    ARROW_RETURN_NOT_OK(WriteJsonValue(values, offset + i, out));
  }
  out->push_back(']');
  return arrow::Status::OK();
}

arrow::Status WriteJsonValue(const arrow::Array& array, int64_t index, std::string* out) {
  if (array.IsNull(index)) {
    out->append("null");
    return arrow::Status::OK();
  }

  switch (array.type_id()) {
    case arrow::Type::INT64:
      out->append(
          std::to_string(static_cast<const arrow::Int64Array&>(array).Value(index)));
      return arrow::Status::OK();

    case arrow::Type::DOUBLE: {
      double value = static_cast<const arrow::DoubleArray&>(array).Value(index);
      if (std::isfinite(value)) {
        char buffer[32];
        std::snprintf(buffer, sizeof(buffer), "%.17g", value);
        out->append(buffer);
      } else {
        // JSON has no representation for NaN or infinity.
        out->append("null");
      }
      return arrow::Status::OK();
    }

    case arrow::Type::STRING:
      AppendJsonString(static_cast<const arrow::StringArray&>(array).GetView(index), out);
      return arrow::Status::OK();

    case arrow::Type::BOOL:
      out->append(static_cast<const arrow::BooleanArray&>(array).Value(index) ? "true"
                                                                              : "false");
      return arrow::Status::OK();

    case arrow::Type::STRUCT:
      return WriteJsonStruct(static_cast<const arrow::StructArray&>(array), index, out);

    case arrow::Type::LIST:
      return WriteJsonList(static_cast<const arrow::ListArray&>(array), index, out);

    default:
      return arrow::Status::NotImplemented("Cannot convert Arrow array of type ",
                                           array.type()->ToString(), " to JSON");
  }
}  // WriteJsonValue

// Convert a single row of an Arrow record batch into a JSON object.
arrow::Result<std::string> ConvertRowToJson(const arrow::RecordBatch& batch,
                                            int64_t row) {
  std::string json = "{";
  for (int i = 0; i < batch.num_columns(); ++i) {
    if (i > 0) {
      json.push_back(',');
    }
    AppendJsonString(batch.schema()->field(i)->name(), &json);
    json.push_back(':');
    ARROW_RETURN_NOT_OK(WriteJsonValue(*batch.column(i), row, &json));
  }
  json.push_back('}');
  return json;
}

// Convert a single batch of Arrow data into JSON rows.
// Rows are held by shared_ptr because Arrow iterators signal end-of-iteration
// with a sentinel value (a null pointer here).
arrow::Result<std::vector<std::shared_ptr<std::string>>> ConvertToVector(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  std::vector<std::shared_ptr<std::string>> rows;
  rows.reserve(batch->num_rows());

  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto row, ConvertRowToJson(*batch, i));
    rows.push_back(std::make_shared<std::string>(std::move(row)));
  }

  return rows;
}

class ArrowToJsonConverter {
 public:
  /// Convert an Arrow table into an iterator of JSON rows
  arrow::Iterator<std::shared_ptr<std::string>> ConvertToIterator(
      std::shared_ptr<arrow::Table> table, size_t batch_size) {
    // Use TableBatchReader to divide table into smaller batches. The batches
    // created are zero-copy slices with *at most* `batch_size` rows.
    auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
    batch_reader->set_chunksize(batch_size);

    auto read_batch = [](const std::shared_ptr<arrow::RecordBatch>& batch)
        -> arrow::Result<arrow::Iterator<std::shared_ptr<std::string>>> {
      ARROW_ASSIGN_OR_RAISE(auto rows, ConvertToVector(batch));
      return arrow::MakeVectorIterator(std::move(rows));
    };

    auto nested_iter = arrow::MakeMaybeMapIterator(
        read_batch, arrow::MakeIteratorFromReader(std::move(batch_reader)));

    return arrow::MakeFlattenIterator(std::move(nested_iter));
  }
};  // ArrowToJsonConverter

arrow::Status DoRowConversion(int32_t num_rows, int32_t batch_size) {
  //(Doc section: Convert to Arrow)
  // Write JSON records
  std::vector<std::string> json_records = {
      R"({"pk": 1, "date_created": "2020-10-01", "data": {"deleted": true, "metrics": [{"key": "x", "value": 1}]}})",
      R"({"pk": 2, "date_created": "2020-10-03", "data": {"deleted": false, "metrics": []}})",
      R"({"pk": 3, "date_created": "2020-10-05", "data": {"deleted": false, "metrics": [{"key": "x", "value": 33}, {"key": "x", "value": 42}]}})"};

  std::vector<std::string> records;
  records.reserve(num_rows);
  for (int32_t i = 0; i < num_rows; ++i) {
    records.push_back(json_records[i % json_records.size()]);
  }

  for (const auto& json : records) {
    std::cout << json << std::endl;
  }

  auto tags_schema = arrow::list(arrow::struct_({
      arrow::field("key", arrow::utf8()),
      arrow::field("value", arrow::int64()),
  }));
  auto schema = arrow::schema(
      {arrow::field("pk", arrow::int64()), arrow::field("date_created", arrow::utf8()),
       arrow::field("data", arrow::struct_({arrow::field("deleted", arrow::boolean()),
                                            arrow::field("metrics", tags_schema)}))});

  // Convert records into a table
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                        ConvertToRecordBatch(records, schema));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table,
                        arrow::Table::FromRecordBatches({batch}));

  // Print table
  std::cout << table->ToString() << std::endl;
  ARROW_RETURN_NOT_OK(table->ValidateFull());
  //(Doc section: Convert to Arrow)

  //(Doc section: Convert to Rows)
  // Create converter
  ArrowToJsonConverter to_json_converter;

  // Convert table into JSON (row) iterator
  auto json_iter = to_json_converter.ConvertToIterator(table, batch_size);

  // Print each row
  for (arrow::Result<std::shared_ptr<std::string>> json_result : json_iter) {
    ARROW_ASSIGN_OR_RAISE(auto json, std::move(json_result));
    std::cout << *json << std::endl;
  }
  //(Doc section: Convert to Rows)

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  int32_t num_rows = argc > 1 ? std::atoi(argv[1]) : 100;
  int32_t batch_size = argc > 2 ? std::atoi(argv[2]) : 100;

  arrow::Status status = DoRowConversion(num_rows, batch_size);

  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
