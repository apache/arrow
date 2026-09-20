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

#include "arrow/api.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table_builder.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/simdjson_internal.h"

#include <simdjson.h>

#include <cstdlib>
#include <iostream>
#include <string>
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
//  * arrow::internal::JsonWriter for writing JSON values
//  * arrow::internal::ParseJsonObject and related helpers for parsing JSON
//  * arrow::RecordBatchBuilder for constructing Arrow arrays from row data
//  * arrow::TableBatchReader and Arrow iterators for converting Arrow tables back
//    into row-based JSON data

namespace arrow {

namespace {

namespace sj = simdjson;

// Append a JSON value to an Arrow builder according to the expected Arrow type.
// This example only handles the types used by the example schema; extend this
// switch when adapting it to schemas with additional Arrow types.
Status AppendJsonValue(const sj::dom::element& value,
                       const std::shared_ptr<DataType>& type, ArrayBuilder* builder);

Status AppendJsonStruct(const sj::dom::element& value, const StructType& type,
                        StructBuilder* builder) {
  if (value.is_null()) {
    for (int i = 0; i < type.num_fields(); ++i) {
      ARROW_RETURN_NOT_OK(builder->child_builder(i)->AppendNull());
    }
    return builder->AppendNull();
  }

  if (!value.is_object()) {
    return Status::TypeError("Expected JSON object for struct");
  }

  ARROW_ASSIGN_OR_RAISE(
      auto object,
      internal::ResolveSimdjsonResult(value.get_object(), "Failed to get JSON object"));

  for (int i = 0; i < type.num_fields(); ++i) {
    const auto& field = type.field(i);

    ARROW_ASSIGN_OR_RAISE(auto child,
                          internal::GetOptionalJsonField(object, field->name()));

    if (!child.has_value()) {
      ARROW_RETURN_NOT_OK(builder->child_builder(i)->AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(
          AppendJsonValue(*child, field->type(), builder->child_builder(i).get()));
    }
  }

  return builder->Append();
}

Status AppendJsonList(const sj::dom::element& value, const ListType& type,
                      ListBuilder* builder) {
  if (value.is_null()) {
    return builder->AppendNull();
  }

  ARROW_ASSIGN_OR_RAISE(auto array, internal::GetJsonArray(value, "JSON value"));

  ARROW_RETURN_NOT_OK(builder->Append());

  for (auto element : array) {
    ARROW_RETURN_NOT_OK(
        AppendJsonValue(element, type.value_field()->type(), builder->value_builder()));
  }

  return Status::OK();
}

Status AppendJsonValue(const sj::dom::element& value,
                       const std::shared_ptr<DataType>& type, ArrayBuilder* builder) {
  if (value.is_null()) {
    return builder->AppendNull();
  }

  switch (type->id()) {
    case Type::INT64: {
      ARROW_ASSIGN_OR_RAISE(auto number,
                            internal::GetJsonInt(value, "JSON value", "integers"));
      return static_cast<Int64Builder*>(builder)->Append(number);
    }

    case Type::DOUBLE: {
      ARROW_ASSIGN_OR_RAISE(auto number,
                            internal::ResolveSimdjsonResult(value.get_double(),
                                                            "Failed to get JSON double"));
      return static_cast<DoubleBuilder*>(builder)->Append(number);
    }

    case Type::STRING: {
      ARROW_ASSIGN_OR_RAISE(auto string,
                            internal::ResolveSimdjsonResult(value.get_string(),
                                                            "Failed to get JSON string"));
      return static_cast<StringBuilder*>(builder)->Append(string);
    }

    case Type::BOOL: {
      ARROW_ASSIGN_OR_RAISE(
          auto boolean, internal::ResolveSimdjsonResult(value.get_bool(),
                                                        "Failed to get JSON boolean"));
      return static_cast<BooleanBuilder*>(builder)->Append(boolean);
    }

    case Type::STRUCT:
      return AppendJsonStruct(value, *static_cast<const StructType*>(type.get()),
                              static_cast<StructBuilder*>(builder));

    case Type::LIST:
      return AppendJsonList(value, *static_cast<const ListType*>(type.get()),
                            static_cast<ListBuilder*>(builder));

    default:
      return Status::NotImplemented("Cannot convert JSON value to Arrow array of type ",
                                    type->ToString());
  }
}  // AppendJsonValue

// RecordBatchBuilder will create array builders for us for each field in our
// schema. By passing the number of output rows (`rows.size()`), we pre-allocate
// the correct size of arrays, except of course in the case of string and list
// arrays, which have dynamic lengths.
Result<std::shared_ptr<RecordBatch>> ConvertToRecordBatch(
    const std::vector<std::string>& rows, const std::shared_ptr<Schema>& schema) {
  std::unique_ptr<RecordBatchBuilder> batch_builder;

  ARROW_ASSIGN_OR_RAISE(batch_builder, RecordBatchBuilder::Make(
                                           schema, default_memory_pool(), rows.size()));

  sj::dom::parser parser;

  // Parse each row and append its values to the corresponding Arrow builders.
  for (const auto& json : rows) {
    ARROW_ASSIGN_OR_RAISE(auto object, internal::ParseJsonObject(parser, json));

    for (int i = 0; i < schema->num_fields(); ++i) {
      const auto& field = schema->field(i);
      auto builder = batch_builder->GetField(i);

      ARROW_ASSIGN_OR_RAISE(auto value,
                            internal::GetOptionalJsonField(object, field->name()));

      if (!value.has_value()) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(AppendJsonValue(*value, field->type(), builder));
      }
    }
  }

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> batch, batch_builder->Flush());

  ARROW_RETURN_NOT_OK(batch->ValidateFull());
  return batch;
}  // ConvertToRecordBatch

// Write an Arrow value as JSON according to its Arrow type.
// This example only handles the types used by the example schema; extend this
// switch when adapting it to schemas with additional Arrow types.
Status WriteJsonValue(const Array& array, int64_t index,
                      const std::shared_ptr<DataType>& type,
                      internal::JsonWriter* writer);

Status WriteJsonStruct(const StructArray& array, int64_t index, const StructType& type,
                       internal::JsonWriter* writer) {
  writer->StartObject();

  for (int i = 0; i < type.num_fields(); ++i) {
    const auto& field = type.field(i);
    const auto& child = array.field(i);

    writer->Key(field->name());

    if (child->IsNull(index)) {
      writer->Null();
    } else {
      ARROW_RETURN_NOT_OK(WriteJsonValue(*child, index, field->type(), writer));
    }
  }

  writer->EndObject();
  return Status::OK();
}

Status WriteJsonList(const ListArray& array, int64_t index, const ListType& type,
                     internal::JsonWriter* writer) {
  writer->StartArray();

  const int64_t offset = array.value_offset(index);
  const int64_t length = array.value_length(index);
  const auto& values = *array.values();

  for (int64_t i = 0; i < length; ++i) {
    ARROW_RETURN_NOT_OK(
        WriteJsonValue(values, offset + i, type.value_field()->type(), writer));
  }

  writer->EndArray();
  return Status::OK();
}

Status WriteJsonValue(const Array& array, int64_t index,
                      const std::shared_ptr<DataType>& type,
                      internal::JsonWriter* writer) {
  if (array.IsNull(index)) {
    writer->Null();
    return Status::OK();
  }

  switch (type->id()) {
    case Type::INT64:
      writer->Int64(static_cast<const Int64Array&>(array).Value(index));
      return Status::OK();

    case Type::DOUBLE:
      writer->Double(static_cast<const DoubleArray&>(array).Value(index));
      return Status::OK();

    case Type::STRING:
      writer->String(static_cast<const StringArray&>(array).GetView(index));
      return Status::OK();

    case Type::BOOL:
      writer->Bool(static_cast<const BooleanArray&>(array).Value(index));
      return Status::OK();

    case Type::STRUCT:
      return WriteJsonStruct(static_cast<const StructArray&>(array), index,
                             *static_cast<const StructType*>(type.get()), writer);

    case Type::LIST:
      return WriteJsonList(static_cast<const ListArray&>(array), index,
                           *static_cast<const ListType*>(type.get()), writer);

    default:
      return Status::NotImplemented("Cannot convert Arrow array of type ",
                                    type->ToString(), " to JSON");
  }
}  // WriteJsonValue

// Convert a single row of an Arrow record batch into a JSON object.
Result<std::string> ConvertRowToJson(const RecordBatch& batch, int64_t row) {
  internal::JsonWriter writer;

  writer.StartObject();

  for (int i = 0; i < batch.num_columns(); ++i) {
    const auto& field = batch.schema()->field(i);
    const auto& column = batch.column(i);

    writer.Key(field->name());
    ARROW_RETURN_NOT_OK(WriteJsonValue(*column, row, field->type(), &writer));
  }

  writer.EndObject();

  ARROW_ASSIGN_OR_RAISE(auto json, writer.GetString());

  return std::string(json);
}

// Convert a single batch of Arrow data into JSON rows.
Result<std::vector<std::shared_ptr<std::string>>> ConvertToVector(
    const std::shared_ptr<RecordBatch>& batch) {
  std::vector<std::shared_ptr<std::string>> rows;
  rows.reserve(batch->num_rows());

  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto row, ConvertRowToJson(*batch, i));
    rows.push_back(std::make_shared<std::string>(std::move(row)));
  }

  return rows;
}

// Convert an Arrow table into an iterator of JSON rows.
class ArrowToJsonConverter {
 public:
  Iterator<std::shared_ptr<std::string>> ConvertToIterator(std::shared_ptr<Table> table,
                                                           size_t batch_size) {
    // Use TableBatchReader to divide the table into smaller batches. The batches
    // created are zero-copy slices with *at most* `batch_size` rows.
    auto batch_reader = std::make_shared<TableBatchReader>(*table);
    batch_reader->set_chunksize(batch_size);

    auto read_batch = [](const std::shared_ptr<RecordBatch>& batch)
        -> Result<Iterator<std::shared_ptr<std::string>>> {
      ARROW_ASSIGN_OR_RAISE(auto rows, ConvertToVector(batch));
      return MakeVectorIterator(std::move(rows));
    };

    auto nested_iter =
        MakeMaybeMapIterator(read_batch, MakeIteratorFromReader(std::move(batch_reader)));

    return MakeFlattenIterator(std::move(nested_iter));
  }
};  // ArrowToJsonConverter

Status DoRowConversion(int32_t num_rows, int32_t batch_size) {
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

  auto tags_schema = list(struct_({
      field("key", utf8()),
      field("value", int64()),
  }));

  auto table_schema = schema({field("pk", int64()), field("date_created", utf8()),
                              field("data", struct_({field("deleted", boolean()),
                                                     field("metrics", tags_schema)}))});

  // Convert records into a table
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> batch,
                        ConvertToRecordBatch(records, table_schema));

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, Table::FromRecordBatches({batch}));

  std::cout << table->ToString() << std::endl;
  ARROW_RETURN_NOT_OK(table->ValidateFull());

  //(Doc section: Convert to Rows)
  ArrowToJsonConverter to_json_converter;

  auto json_iter = to_json_converter.ConvertToIterator(table, batch_size);

  for (Result<std::shared_ptr<std::string>> json_result : json_iter) {
    ARROW_ASSIGN_OR_RAISE(auto json, std::move(json_result));
    std::cout << *json << std::endl;
  }
  //(Doc section: Convert to Rows)

  return Status::OK();
}

}  // namespace

}  // namespace arrow

int main(int argc, char** argv) {
  int32_t num_rows = argc > 1 ? std::atoi(argv[1]) : 100;
  int32_t batch_size = argc > 2 ? std::atoi(argv[2]) : 100;

  arrow::Status status = arrow::DoRowConversion(num_rows, batch_size);

  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
