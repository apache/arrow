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
#include <arrow/type_traits.h>
#include <arrow/util/iterator.h>
#include <arrow/visit_array_inline.h>
#include <arrow/visit_type_inline.h>

#include <simdjson.h>

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <locale>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

// Transforming dynamic row data into Arrow data
// When building connectors to other data systems, it's common to receive data in
// row-based structures. While the row_wise_conversion_example.cc shows how to
// handle this conversion for fixed schemas, this example demonstrates how to
// convert between row-based JSON data and Arrow data for a useful subset of
// nested Arrow schemas.
//
// As an example, this conversion is between JSON strings and Arrow tables.
//
// We use the following helpers and patterns here:
//  * simdjson's DOM API for parsing JSON rows
//  * arrow::RecordBatchBuilder for constructing Arrow arrays from row data
//  * arrow::TableBatchReader and Arrow iterators for converting Arrow tables back
//    into row-based JSON data
//  * arrow::VisitTypeInline and arrow::VisitArrayInline for implementing a visitor
//    pattern with Arrow to handle different types and array types
//  * arrow::enable_if_primitive_ctype to create a template method that handles
//    conversion for Arrow types that have corresponding C types (bool, integer,
//    float).

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

// A JSON value, or std::nullopt if it is missing.
using JsonValue = std::optional<simdjson::dom::element>;

bool IsNull(const JsonValue& value) { return !value.has_value() || value->is_null(); }

// Collect the values of the given fields from each JSON object, one vector per
// field. Missing fields and fields of null objects are returned as std::nullopt.
arrow::Result<std::vector<std::vector<JsonValue>>> GetAllChildValues(
    const std::vector<JsonValue>& objects, const arrow::FieldVector& fields) {
  std::vector<std::vector<JsonValue>> children(fields.size());
  for (auto& values : children) {
    values.reserve(objects.size());
  }

  for (const JsonValue& value : objects) {
    if (IsNull(value)) {
      for (auto& values : children) {
        values.push_back(std::nullopt);
      }
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto object,
                          Unwrap(value->get_object(), "Failed to get JSON object"));
    for (size_t i = 0; i < fields.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto child, GetField(object, fields[i]->name()));
      children[i].push_back(child);
    }
  }
  return children;
}  // GetAllChildValues

/// \brief Visitor that appends the JSON values of one column to an Arrow builder.
///
/// Implements Visit() methods for each Arrow type; each one appends one value per row.
class JsonValueConverter {
 public:
  /// \brief Append `values` (one per row) to `builder`, according to `type`.
  arrow::Status Convert(const arrow::DataType& type, arrow::ArrayBuilder* builder,
                        const std::vector<JsonValue>& values) {
    builder_ = builder;
    values_ = &values;
    return arrow::VisitTypeInline(type, this);
  }

  // Default implementation
  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::NotImplemented(
        "Cannot convert json value to Arrow array of type ", type.ToString());
  }

  arrow::Status Visit(const arrow::Int64Type& type) {
    auto* builder = static_cast<arrow::Int64Builder*>(builder_);
    for (const JsonValue& value : *values_) {
      if (IsNull(value)) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else if (value->is_uint64()) {
        ARROW_ASSIGN_OR_RAISE(auto number,
                              Unwrap(value->get_uint64(), "Failed to get JSON uint64"));
        if (number > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return arrow::Status::Invalid("JSON uint64 value does not fit in int64");
        }
        ARROW_RETURN_NOT_OK(builder->Append(static_cast<int64_t>(number)));
      } else {
        ARROW_ASSIGN_OR_RAISE(auto number,
                              Unwrap(value->get_int64(), "Failed to get JSON int64"));
        ARROW_RETURN_NOT_OK(builder->Append(number));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType& type) {
    auto* builder = static_cast<arrow::DoubleBuilder*>(builder_);
    for (const JsonValue& value : *values_) {
      if (IsNull(value)) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_ASSIGN_OR_RAISE(auto number,
                              Unwrap(value->get_double(), "Failed to get JSON double"));
        ARROW_RETURN_NOT_OK(builder->Append(number));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType& type) {
    auto* builder = static_cast<arrow::StringBuilder*>(builder_);
    for (const JsonValue& value : *values_) {
      if (IsNull(value)) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_ASSIGN_OR_RAISE(auto string,
                              Unwrap(value->get_string(), "Failed to get JSON string"));
        ARROW_RETURN_NOT_OK(builder->Append(string));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType& type) {
    auto* builder = static_cast<arrow::BooleanBuilder*>(builder_);
    for (const JsonValue& value : *values_) {
      if (IsNull(value)) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_ASSIGN_OR_RAISE(auto boolean,
                              Unwrap(value->get_bool(), "Failed to get JSON boolean"));
        ARROW_RETURN_NOT_OK(builder->Append(boolean));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructType& type) {
    auto* builder = static_cast<arrow::StructBuilder*>(builder_);

    // Collect all child values in one pass over the rows, then convert each
    // child column recursively.
    ARROW_ASSIGN_OR_RAISE(auto children, GetAllChildValues(*values_, type.fields()));
    for (int i = 0; i < type.num_fields(); ++i) {
      JsonValueConverter child_converter;
      ARROW_RETURN_NOT_OK(child_converter.Convert(
          *type.field(i)->type(), builder->child_builder(i).get(), children[i]));
    }

    // Make null bitmap
    for (const JsonValue& value : *values_) {
      ARROW_RETURN_NOT_OK(builder->Append(!IsNull(value)));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListType& type) {
    auto* builder = static_cast<arrow::ListBuilder*>(builder_);

    // Values and offsets need to be interleaved in ListBuilder, so first collect the
    // elements of all the lists and convert them into a temporary array.
    std::vector<JsonValue> child_values;
    std::vector<int64_t> sizes;
    for (const JsonValue& value : *values_) {
      int64_t size = 0;
      if (!IsNull(value)) {
        ARROW_ASSIGN_OR_RAISE(auto array,
                              Unwrap(value->get_array(), "Failed to get JSON array"));
        for (simdjson::dom::element element : array) {
          child_values.push_back(element);
          ++size;
        }
      }
      sizes.push_back(size);
    }

    std::unique_ptr<arrow::ArrayBuilder> tmp_value_builder;
    ARROW_ASSIGN_OR_RAISE(tmp_value_builder,
                          arrow::MakeBuilder(builder->value_builder()->type()));
    JsonValueConverter child_converter;
    ARROW_RETURN_NOT_OK(child_converter.Convert(*type.value_type(),
                                                tmp_value_builder.get(), child_values));

    std::shared_ptr<arrow::Array> values_array;
    ARROW_RETURN_NOT_OK(tmp_value_builder->Finish(&values_array));
    arrow::ArraySpan values_span(*values_array->data());

    arrow::ArrayBuilder* value_builder = builder->value_builder();
    int64_t offset = 0;
    for (size_t i = 0; i < values_->size(); ++i) {
      ARROW_RETURN_NOT_OK(builder->Append(!IsNull((*values_)[i])));
      if (sizes[i] > 0) {
        ARROW_RETURN_NOT_OK(
            value_builder->AppendArraySlice(values_span, offset, sizes[i]));
        offset += sizes[i];
      }
    }
    return arrow::Status::OK();
  }

 private:
  arrow::ArrayBuilder* builder_ = nullptr;
  const std::vector<JsonValue>* values_ = nullptr;
};  // JsonValueConverter

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

  // Parse each row into its own document, so that all DOM elements remain valid while
  // the converter visits the rows once per column. The documents must not move
  // after parsing, so the vector is sized once and never grown.
  simdjson::dom::parser parser;
  std::vector<simdjson::dom::document> documents(rows.size());

  std::vector<JsonValue> row_values;
  row_values.reserve(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto row,
                          Unwrap(parser.parse_into_document(documents[i], rows[i]),
                                 "Failed to parse JSON row"));
    row_values.emplace_back(row);
  }

  // Collect the values of all fields in one pass over the rows.
  ARROW_ASSIGN_OR_RAISE(auto children, GetAllChildValues(row_values, schema->fields()));

  // Inner converter will take the values of each field and be responsible for
  // appending them to the provided array builder.
  JsonValueConverter converter;
  for (int i = 0; i < batch_builder->num_fields(); ++i) {
    std::shared_ptr<arrow::Field> field = schema->field(i);
    ARROW_RETURN_NOT_OK(
        converter.Convert(*field->type(), batch_builder->GetField(i), children[i]));
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

// Return a boolean, integer or floating point value as JSON text.
template <typename T>
std::string FormatPrimitive(T value) {
  if constexpr (std::is_same_v<T, bool>) {
    return value ? "true" : "false";
  } else if constexpr (std::is_floating_point_v<T>) {
    if (!std::isfinite(value)) {
      // JSON has no representation for NaN or infinity.
      return "null";
    }
    std::ostringstream ss;
    ss.imbue(std::locale::classic());
    ss.precision(17);
    ss << value;
    return ss.str();
  } else {
    return std::to_string(value);
  }
}

// Join the converted columns into the JSON object of one row.
std::string ObjectToJson(const std::vector<std::shared_ptr<arrow::Field>>& fields,
                         const std::vector<std::vector<std::string>>& columns,
                         int64_t row) {
  std::string json = "{";
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i > 0) {
      json.push_back(',');
    }
    AppendJsonString(fields[i]->name(), &json);
    json.push_back(':');
    json += columns[i][row];
  }
  json.push_back('}');
  return json;
}

/// \brief Visitor that writes the elements of one Arrow array as JSON.
///
/// Implements Visit() methods for each type of Arrow Array. Each one produces the
/// JSON text of every element of the array ("null" for null elements).
class JsonColumnWriter {
 public:
  /// \brief Retrieve the JSON text of each element.
  std::vector<std::string> Values() && { return std::move(values_); }

  // Default implementation
  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented("Cannot convert to json for array of type ",
                                         array.type()->ToString());
  }

  // Handles booleans, integers, floats
  template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
  arrow::enable_if_primitive_ctype<DataClass, arrow::Status> Visit(
      const ArrayType& array) {
    values_.reserve(array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      values_.push_back(array.IsNull(i) ? "null" : FormatPrimitive(array.Value(i)));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray& array) {
    values_.reserve(array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        values_.push_back("null");
      } else {
        values_.emplace_back();
        AppendJsonString(array.GetView(i), &values_.back());
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructArray& array) {
    const arrow::StructType* type = array.struct_type();

    // Convert each child column recursively.
    std::vector<std::vector<std::string>> columns;
    for (int i = 0; i < type->num_fields(); ++i) {
      JsonColumnWriter child_writer;
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array.field(i), &child_writer));
      columns.push_back(std::move(child_writer).Values());
    }

    values_.reserve(array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      values_.push_back(array.IsNull(i) ? "null"
                                        : ObjectToJson(type->fields(), columns, i));
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListArray& array) {
    if (array.length() == 0) {
      return arrow::Status::OK();
    }

    // Only convert the part of the child values that this (possibly sliced) array
    // refers to.
    const int64_t begin = array.value_offset(0);
    const int64_t end = array.value_offset(array.length());
    JsonColumnWriter child_writer;
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(
        *array.values()->Slice(begin, end - begin), &child_writer));
    std::vector<std::string> child_values = std::move(child_writer).Values();

    values_.reserve(array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        values_.push_back("null");
        continue;
      }
      std::string json = "[";
      for (int64_t j = array.value_offset(i); j < array.value_offset(i + 1); ++j) {
        if (j > array.value_offset(i)) {
          json.push_back(',');
        }
        json += child_values[j - begin];
      }
      json.push_back(']');
      values_.push_back(std::move(json));
    }
    return arrow::Status::OK();
  }

 private:
  std::vector<std::string> values_;
};  // JsonColumnWriter

// Convert a single batch of Arrow data into JSON rows.
// Rows are held by shared_ptr because Arrow iterators signal end-of-iteration
// with a sentinel value (a null pointer here).
arrow::Result<std::vector<std::shared_ptr<std::string>>> ConvertToVector(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  // Visit each column of the batch, converting all of its values at once.
  std::vector<std::vector<std::string>> columns;
  for (int i = 0; i < batch->num_columns(); ++i) {
    JsonColumnWriter writer;
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*batch->column(i), &writer));
    columns.push_back(std::move(writer).Values());
  }

  // Then join the columns into one JSON object per row.
  std::vector<std::shared_ptr<std::string>> rows;
  rows.reserve(batch->num_rows());
  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    rows.push_back(std::make_shared<std::string>(
        ObjectToJson(batch->schema()->fields(), columns, i)));
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
    auto batch_reader = std::make_shared<arrow::TableBatchReader>(std::move(table));
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

  auto metrics_schema = arrow::list(arrow::struct_({
      arrow::field("key", arrow::utf8()),
      arrow::field("value", arrow::int64()),
  }));
  auto schema = arrow::schema(
      {arrow::field("pk", arrow::int64()), arrow::field("date_created", arrow::utf8()),
       arrow::field("data", arrow::struct_({arrow::field("deleted", arrow::boolean()),
                                            arrow::field("metrics", metrics_schema)}))});

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

  if (num_rows < 0 || batch_size <= 0) {
    std::cerr << "Error: num_rows must be >= 0 and batch_size must be > 0." << std::endl;
    return EXIT_FAILURE;
  }

  arrow::Status status = DoRowConversion(num_rows, batch_size);

  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
