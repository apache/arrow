// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#define RAPIDJSON_HAS_STDSTRING 1

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/table_builder.h>
#include <arrow/type_traits.h>
#include <arrow/util/iterator.h>
#include <arrow/util/logging.h>
#include <arrow/visit_array_inline.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cassert>
#include <iostream>
#include <vector>

// Transforming dynamic row data into Arrow data
// When building connectors to other data systems, it's common to receive data in
// row-based structures. While the row_wise_conversion_example.cc shows how to
// handle this conversion for fixed schemas, this example demonstrates how to
// writer converters for arbitrary schemas.
//
// As an example, this conversion is between Arrow and rapidjson::Documents.
//
// We use the following helpers and patterns here:
//  * arrow::VisitArrayInline and arrow::VisitTypeInline for implementing a visitor
//    pattern with Arrow to handle different array types
//  * arrow::enable_if_primitive_ctype to create a template method that handles
//    conversion for Arrow types that have corresponding C types (bool, integer,
//    float).

const rapidjson::Value kNullJsonSingleton = rapidjson::Value();

/// \brief Builder that holds state for a single conversion.
///
/// Implements Visit() methods for each type of Arrow Array that set the values
/// of the corresponding fields in each row.
class RowBatchBuilder {
 public:
  explicit RowBatchBuilder(int64_t num_rows) : field_(nullptr) {
    // Reserve all of the space required up-front to avoid unnecessary resizing
    rows_.reserve(num_rows);

    for (int64_t i = 0; i < num_rows; ++i) {
      rows_.push_back(rapidjson::Document());
      rows_[i].SetObject();
    }
  }

  /// \brief Set which field to convert.
  void SetField(const arrow::Field* field) { field_ = field; }

  /// \brief Retrieve converted rows from builder.
  std::vector<rapidjson::Document> Rows() && { return std::move(rows_); }

  // Default implementation
  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented(
        "Can not convert to json document for array of type ", array.type()->ToString());
  }

  // Handles booleans, integers, floats
  template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
  arrow::enable_if_primitive_ctype<DataClass, arrow::Status> Visit(
      const ArrayType& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        rapidjson::Value str_key(field_->name(), rows_[i].GetAllocator());
        rows_[i].AddMember(str_key, array.Value(i), rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        rapidjson::Value str_key(field_->name(), rows_[i].GetAllocator());
        arrow::util::string_view value_view = array.Value(i);
        rapidjson::Value value;
        value.SetString(value_view.data(),
                        static_cast<rapidjson::SizeType>(value_view.size()),
                        rows_[i].GetAllocator());
        rows_[i].AddMember(str_key, value, rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructArray& array) {
    const arrow::StructType* type = array.struct_type();

    assert(static_cast<int64_t>(rows_.size()) == array.length());

    RowBatchBuilder child_builder(rows_.size());
    for (int i = 0; i < type->num_fields(); ++i) {
      const arrow::Field* child_field = type->field(i).get();
      child_builder.SetField(child_field);
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array.field(i).get(), &child_builder));
    }
    std::vector<rapidjson::Document> rows = std::move(child_builder).Rows();

    for (int64_t i = 0; i < array.length(); ++i) {
      if (!array.IsNull(i)) {
        rapidjson::Value str_key(field_->name(), rows_[i].GetAllocator());
        // Must copy value to new allocator
        rapidjson::Value row_val;
        row_val.CopyFrom(rows[i], rows_[i].GetAllocator());
        rows_[i].AddMember(str_key, row_val, rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListArray& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    // First create rows from values
    std::shared_ptr<arrow::Array> values = array.values();
    RowBatchBuilder child_builder(values->length());
    const arrow::Field* value_field = array.list_type()->value_field().get();
    std::string value_field_name = value_field->name();
    child_builder.SetField(value_field);
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*values.get(), &child_builder));

    std::vector<rapidjson::Document> rows = std::move(child_builder).Rows();

    int64_t values_i = 0;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) continue;

      rapidjson::Document::AllocatorType& allocator = rows_[i].GetAllocator();
      auto array_len = array.value_length(i);

      rapidjson::Value value;
      value.SetArray();
      value.Reserve(array_len, allocator);

      for (int64_t j = 0; j < array_len; ++j) {
        rapidjson::Value row_val;
        // Must copy value to new allocator
        row_val.CopyFrom(rows[values_i][value_field_name], allocator);
        value.PushBack(row_val, allocator);
        ++values_i;
      }

      rapidjson::Value str_key(field_->name(), allocator);
      rows_[i].AddMember(str_key, value, allocator);
    }

    return arrow::Status::OK();
  }

 private:
  const arrow::Field* field_;
  std::vector<rapidjson::Document> rows_;
};  // RowBatchBuilder

class ArrowToDocumentConverter {
 public:
  /// Convert a single batch of Arrow data into Documents
  arrow::Result<std::vector<rapidjson::Document>> ConvertToVector(
      std::shared_ptr<arrow::RecordBatch> batch) {
    RowBatchBuilder builder{batch->num_rows()};

    for (int i = 0; i < batch->num_columns(); ++i) {
      builder.SetField(batch->schema()->field(i).get());
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*batch->column(i).get(), &builder));
    }

    return std::move(builder).Rows();
  }

  /// Convert an Arrow table into an iterator of Documents
  arrow::Iterator<rapidjson::Document> ConvertToIterator(
      std::shared_ptr<arrow::Table> table, size_t batch_size) {
    // Use TableBatchReader to divide table into smaller batches. The batches
    // created are zero-copy slices with *at most* `batch_size` rows.
    auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
    batch_reader->set_chunksize(batch_size);

    auto read_batch = [this](const std::shared_ptr<arrow::RecordBatch>& batch)
        -> arrow::Result<arrow::Iterator<rapidjson::Document>> {
      ARROW_ASSIGN_OR_RAISE(auto rows, ConvertToVector(batch));
      return arrow::MakeVectorIterator(std::move(rows));
    };

    auto nested_iter = arrow::MakeMaybeMapIterator(
        read_batch, arrow::MakeIteratorFromReader(std::move(batch_reader)));

    return arrow::MakeFlattenIterator(std::move(nested_iter));
  }
};  // ArrowToDocumentConverter

/// \brief Iterator over rows values of a document for a given field
///
/// path and array_levels are used to address each field in a JSON document. As
/// an example, consider this JSON document:
/// {
///     "x": 3,                   // path: ["x"],             array_levels: 0
///     "files": [                // path: ["files"],         array_levels: 0
///         {                     // path: ["files"],         array_levels: 1
///             "path": "my_str", // path: ["files", "path"], array_levels: 1
///             "sizes": [        // path: ["files", "size"], array_levels: 1
///                 20,           // path: ["files", "size"], array_levels: 2
///                 22
///             ]
///         }
///     ]
/// },
class DocValuesIterator {
 public:
  /// \param rows vector of rows
  /// \param path field names to enter
  /// \param array_levels number of arrays to enter
  DocValuesIterator(const std::vector<rapidjson::Document>& rows,
                    std::vector<std::string> path, int64_t array_levels)
      : rows(rows), path(std::move(path)), array_levels(array_levels) {}

  const rapidjson::Value* NextArrayOrRow(const rapidjson::Value* value, size_t* path_i,
                                         int64_t* arr_i) {
    while (array_stack.size() > 0) {
      ArrayPosition& pos = array_stack.back();
      // Try to get next position in Array
      if (pos.index + 1 < pos.array_node->Size()) {
        ++pos.index;
        value = &(*pos.array_node)[pos.index];
        *path_i = pos.path_index;
        *arr_i = array_stack.size();
        return value;
      } else {
        array_stack.pop_back();
      }
    }
    ++row_i;
    if (row_i < rows.size()) {
      value = static_cast<const rapidjson::Value*>(&rows[row_i]);
    } else {
      value = nullptr;
    }
    *path_i = 0;
    *arr_i = 0;
    return value;
  }

  arrow::Result<const rapidjson::Value*> Next() {
    const rapidjson::Value* value = nullptr;
    size_t path_i;
    int64_t arr_i;
    // Can either start at document or at last array level
    if (array_stack.size() > 0) {
      auto pos = array_stack.back();
      value = pos.array_node;
      path_i = pos.path_index;
      arr_i = array_stack.size() - 1;
    }

    value = NextArrayOrRow(value, &path_i, &arr_i);

    // Traverse to desired level (with possible backtracking as needed)
    while (path_i < path.size() || arr_i < array_levels) {
      if (value == nullptr) {
        return value;
      } else if (value->IsArray() && value->Size() > 0) {
        ArrayPosition pos;
        pos.array_node = value;
        pos.path_index = path_i;
        pos.index = 0;
        array_stack.push_back(pos);

        value = &(*value)[0];
        ++arr_i;
      } else if (value->IsArray()) {
        // Empty array means we need to backtrack and go to next array or row
        value = NextArrayOrRow(value, &path_i, &arr_i);
      } else if (value->HasMember(path[path_i])) {
        value = &(*value)[path[path_i]];
        ++path_i;
      } else {
        return &kNullJsonSingleton;
      }
    }

    // Return value
    return value;
  }

 private:
  const std::vector<rapidjson::Document>& rows;
  std::vector<std::string> path;
  int64_t array_levels;
  size_t row_i = -1;  // index of current row

  // Info about array position for one array level in array stack
  struct ArrayPosition {
    const rapidjson::Value* array_node;
    int64_t path_index;
    rapidjson::SizeType index;
  };
  std::vector<ArrayPosition> array_stack;
};

class JsonValueConverter {
 public:
  explicit JsonValueConverter(const std::vector<rapidjson::Document>& rows)
      : rows_(rows), array_levels_(0) {}

  JsonValueConverter(const std::vector<rapidjson::Document>& rows,
                     const std::vector<std::string>& root_path, int64_t array_levels)
      : rows_(rows), root_path_(root_path), array_levels_(array_levels) {}

  /// \brief For field passed in, append corresponding values to builder
  arrow::Status Convert(const arrow::Field& field, arrow::ArrayBuilder* builder) {
    return Convert(field, field.name(), builder);
  }

  /// \brief For field passed in, append corresponding values to builder
  arrow::Status Convert(const arrow::Field& field, const std::string& field_name,
                        arrow::ArrayBuilder* builder) {
    field_name_ = field_name;
    builder_ = builder;
    ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field.type().get(), this));
    return arrow::Status::OK();
  }

  // Default implementation
  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::NotImplemented(
        "Can not convert json value to Arrow array of type ", type.ToString());
  }

  arrow::Status Visit(const arrow::Int64Type& type) {
    arrow::Int64Builder* builder = static_cast<arrow::Int64Builder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
      ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
      if (value->IsNull()) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        if (value->IsUint()) {
          ARROW_RETURN_NOT_OK(builder->Append(value->GetUint()));
        } else if (value->IsInt()) {
          ARROW_RETURN_NOT_OK(builder->Append(value->GetInt()));
        } else if (value->IsUint64()) {
          ARROW_RETURN_NOT_OK(builder->Append(value->GetUint64()));
        } else if (value->IsInt64()) {
          ARROW_RETURN_NOT_OK(builder->Append(value->GetInt64()));
        } else {
          return arrow::Status::Invalid("Value is not an integer");
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType& type) {
    arrow::DoubleBuilder* builder = static_cast<arrow::DoubleBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
      ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
      if (value->IsNull()) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(builder->Append(value->GetDouble()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType& type) {
    arrow::StringBuilder* builder = static_cast<arrow::StringBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
      ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
      if (value->IsNull()) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(builder->Append(value->GetString()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType& type) {
    arrow::BooleanBuilder* builder = static_cast<arrow::BooleanBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
      ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
      if (value->IsNull()) {
        ARROW_RETURN_NOT_OK(builder->AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(builder->Append(value->GetBool()));
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StructType& type) {
    arrow::StructBuilder* builder = static_cast<arrow::StructBuilder*>(builder_);

    std::vector<std::string> child_path(root_path_);
    if (field_name_.size() > 0) {
      child_path.push_back(field_name_);
    }
    auto child_converter = JsonValueConverter(rows_, child_path, array_levels_);

    for (int i = 0; i < type.num_fields(); ++i) {
      std::shared_ptr<arrow::Field> child_field = type.field(i);
      std::shared_ptr<arrow::ArrayBuilder> child_builder = builder->child_builder(i);

      ARROW_RETURN_NOT_OK(
          child_converter.Convert(*child_field.get(), child_builder.get()));
    }

    // Make null bitmap
    for (const auto& maybe_value : FieldValues()) {
      ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
      ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
    }

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ListType& type) {
    arrow::ListBuilder* builder = static_cast<arrow::ListBuilder*>(builder_);

    // Values and offsets needs to be interleaved in ListBuilder, so first collect the
    // values
    std::unique_ptr<arrow::ArrayBuilder> tmp_value_builder;
    ARROW_ASSIGN_OR_RAISE(tmp_value_builder,
                          arrow::MakeBuilder(builder->value_builder()->type()));
    std::vector<std::string> child_path(root_path_);
    child_path.push_back(field_name_);
    auto child_converter = JsonValueConverter(rows_, child_path, array_levels_ + 1);
    ARROW_RETURN_NOT_OK(
        child_converter.Convert(*type.value_field().get(), "", tmp_value_builder.get()));

    std::shared_ptr<arrow::Array> values_array;
    ARROW_RETURN_NOT_OK(tmp_value_builder->Finish(&values_array));
    std::shared_ptr<arrow::ArrayData> values_data = values_array->data();

    arrow::ArrayBuilder* value_builder = builder->value_builder();
    int64_t offset = 0;
    for (const auto& maybe_value : FieldValues()) {
      ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
      ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
      if (!value->IsNull() && value->Size() > 0) {
        ARROW_RETURN_NOT_OK(
            value_builder->AppendArraySlice(*values_data.get(), offset, value->Size()));
        offset += value->Size();
      }
    }

    return arrow::Status::OK();
  }

 private:
  std::string field_name_;
  arrow::ArrayBuilder* builder_;
  const std::vector<rapidjson::Document>& rows_;
  std::vector<std::string> root_path_;
  int64_t array_levels_;

  /// Return a flattened iterator over values at nested location
  arrow::Iterator<const rapidjson::Value*> FieldValues() {
    std::vector<std::string> path(root_path_);
    if (field_name_.size() > 0) {
      path.push_back(field_name_);
    }
    auto iter = DocValuesIterator(rows_, std::move(path), array_levels_);
    auto fn = [iter]() mutable -> arrow::Result<const rapidjson::Value*> {
      return iter.Next();
    };

    return arrow::MakeFunctionIterator(fn);
  }
};  // JsonValueConverter

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
    const std::vector<rapidjson::Document>& rows, std::shared_ptr<arrow::Schema> schema) {
  // RecordBatchBuilder will create array builders for us for each field in our
  // schema. By passing the number of output rows (`rows.size()`) we can
  // pre-allocate the correct size of arrays, except of course in the case of
  // string, byte, and list arrays, which have dynamic lengths.
  std::unique_ptr<arrow::RecordBatchBuilder> batch_builder;
  ARROW_ASSIGN_OR_RAISE(
      batch_builder,
      arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), rows.size()));

  // Inner converter will take rows and be responsible for appending values
  // to provided array builders.
  JsonValueConverter converter(rows);
  for (int i = 0; i < batch_builder->num_fields(); ++i) {
    std::shared_ptr<arrow::Field> field = schema->field(i);
    arrow::ArrayBuilder* builder = batch_builder->GetField(i);
    ARROW_RETURN_NOT_OK(converter.Convert(*field.get(), builder));
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_ASSIGN_OR_RAISE(batch, batch_builder->Flush());

  // Use RecordBatch::ValidateFull() to make sure arrays were correctly constructed.
  DCHECK_OK(batch->ValidateFull());
  return batch;
}  // ConvertToRecordBatch

arrow::Status DoRowConversion(int32_t num_rows, int32_t batch_size) {
  //(Doc section: Convert to Arrow)
  // Write JSON records
  std::vector<std::string> json_records = {
      R"({"pk": 1, "date_created": "2020-10-01", "data": {"deleted": true, "metrics": [{"key": "x", "value": 1}]}})",
      R"({"pk": 2, "date_created": "2020-10-03", "data": {"deleted": false, "metrics": []}})",
      R"({"pk": 3, "date_created": "2020-10-05", "data": {"deleted": false, "metrics": [{"key": "x", "value": 33}, {"key": "x", "value": 42}]}})"};

  std::vector<rapidjson::Document> records;
  records.reserve(num_rows);
  for (int32_t i = 0; i < num_rows; ++i) {
    rapidjson::Document document;
    document.Parse(json_records[i % json_records.size()]);
    records.push_back(std::move(document));
  }

  for (const rapidjson::Document& doc : records) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    doc.Accept(writer);
    std::cout << sb.GetString() << std::endl;
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
  ArrowToDocumentConverter to_doc_converter;

  // Convert table into document (row) iterator
  arrow::Iterator<rapidjson::Document> document_iter =
      to_doc_converter.ConvertToIterator(table, batch_size);

  // Print each row
  for (arrow::Result<rapidjson::Document> doc_result : document_iter) {
    ARROW_ASSIGN_OR_RAISE(rapidjson::Document doc, std::move(doc_result));

    assert(doc.HasMember("pk"));
    assert(doc["pk"].IsInt64());
    assert(doc.HasMember("date_created"));
    assert(doc["date_created"].IsString());
    assert(doc.HasMember("data"));
    assert(doc["data"].IsObject());
    assert(doc["data"].HasMember("deleted"));
    assert(doc["data"]["deleted"].IsBool());
    assert(doc["data"].HasMember("metrics"));
    assert(doc["data"]["metrics"].IsArray());
    if (doc["data"]["metrics"].Size() > 0) {
      auto metric = &doc["data"]["metrics"][0];
      assert(metric->IsObject());
      assert(metric->HasMember("key"));
      assert((*metric)["key"].IsString());
      assert(metric->HasMember("value"));
      assert((*metric)["value"].IsInt64());
    }

    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    doc.Accept(writer);
    std::cout << sb.GetString() << std::endl;
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
