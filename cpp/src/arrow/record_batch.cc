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

#include "arrow/record_batch.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_union.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/statistics.h"
#include "arrow/array/validate.h"
#include "arrow/c/abi.h"
#include "arrow/compare.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/vector.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

Result<std::shared_ptr<RecordBatch>> RecordBatch::AddColumn(
    int i, std::string field_name, const std::shared_ptr<Array>& column) const {
  auto field = ::arrow::field(std::move(field_name), column->type());
  return AddColumn(i, field, column);
}

std::shared_ptr<Array> RecordBatch::GetColumnByName(const std::string& name) const {
  auto i = schema_->GetFieldIndex(name);
  return i == -1 ? NULLPTR : column(i);
}

int RecordBatch::num_columns() const { return schema_->num_fields(); }

/// \class SimpleRecordBatch
/// \brief A basic, non-lazy in-memory record batch
class SimpleRecordBatch : public RecordBatch {
 public:
  SimpleRecordBatch(std::shared_ptr<Schema> schema, int64_t num_rows,
                    std::vector<std::shared_ptr<Array>> columns,
                    std::shared_ptr<Device::SyncEvent> sync_event = nullptr)
      : RecordBatch(std::move(schema), num_rows),
        boxed_columns_(std::move(columns)),
        device_type_(DeviceAllocationType::kCPU),
        sync_event_(std::move(sync_event)) {
    if (boxed_columns_.size() > 0) {
      device_type_ = boxed_columns_[0]->device_type();
    }

    columns_.resize(boxed_columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
      columns_[i] = boxed_columns_[i]->data();
      DCHECK_EQ(device_type_, columns_[i]->device_type());
    }
  }

  SimpleRecordBatch(std::shared_ptr<Schema> schema, int64_t num_rows,
                    std::vector<std::shared_ptr<ArrayData>> columns,
                    DeviceAllocationType device_type = DeviceAllocationType::kCPU,
                    std::shared_ptr<Device::SyncEvent> sync_event = nullptr)
      : RecordBatch(std::move(schema), num_rows),
        columns_(std::move(columns)),
        device_type_(device_type),
        sync_event_(std::move(sync_event)) {
    boxed_columns_.resize(schema_->num_fields());
  }

  const std::vector<std::shared_ptr<Array>>& columns() const override {
    for (int i = 0; i < num_columns(); ++i) {
      // Force all columns to be boxed
      column(i);
    }
    return boxed_columns_;
  }

  std::shared_ptr<Array> column(int i) const override {
    std::shared_ptr<Array> result = std::atomic_load(&boxed_columns_[i]);
    if (!result) {
      auto new_array = MakeArray(columns_[i]);
      // Be careful not to overwrite existing entry if another thread has been calling
      // `column(i)` at the same time, since the `boxed_columns_` contents are exposed
      // by `columns()` (see GH-45371).
      if (std::atomic_compare_exchange_strong(&boxed_columns_[i], &result, new_array)) {
        return new_array;
      }
    }
    return result;
  }

  std::shared_ptr<ArrayData> column_data(int i) const override { return columns_[i]; }

  const ArrayDataVector& column_data() const override { return columns_; }

  Result<std::shared_ptr<RecordBatch>> AddColumn(
      int i, const std::shared_ptr<Field>& field,
      const std::shared_ptr<Array>& column) const override {
    ARROW_CHECK(field != nullptr);
    ARROW_CHECK(column != nullptr);
    ARROW_CHECK(column->device_type() == device_type_);

    if (!field->type()->Equals(column->type())) {
      return Status::TypeError("Column data type ", field->type()->name(),
                               " does not match field data type ",
                               column->type()->name());
    }
    if (column->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match record batch's length. Expected length ",
          num_rows_, " but got length ", column->length());
    }

    ARROW_ASSIGN_OR_RAISE(auto new_schema, schema_->AddField(i, field));
    return RecordBatch::Make(std::move(new_schema), num_rows_,
                             internal::AddVectorElement(columns_, i, column->data()),
                             device_type_, sync_event_);
  }

  Result<std::shared_ptr<RecordBatch>> SetColumn(
      int i, const std::shared_ptr<Field>& field,
      const std::shared_ptr<Array>& column) const override {
    ARROW_CHECK(field != nullptr);
    ARROW_CHECK(column != nullptr);
    ARROW_CHECK(column->device_type() == device_type_);

    if (!field->type()->Equals(column->type())) {
      return Status::TypeError("Column data type ", field->type()->name(),
                               " does not match field data type ",
                               column->type()->name());
    }
    if (column->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match record batch's length. Expected length ",
          num_rows_, " but got length ", column->length());
    }

    ARROW_ASSIGN_OR_RAISE(auto new_schema, schema_->SetField(i, field));
    return RecordBatch::Make(std::move(new_schema), num_rows_,
                             internal::ReplaceVectorElement(columns_, i, column->data()),
                             device_type_, sync_event_);
  }

  Result<std::shared_ptr<RecordBatch>> RemoveColumn(int i) const override {
    ARROW_ASSIGN_OR_RAISE(auto new_schema, schema_->RemoveField(i));
    return RecordBatch::Make(std::move(new_schema), num_rows_,
                             internal::DeleteVectorElement(columns_, i), device_type_,
                             sync_event_);
  }

  std::shared_ptr<RecordBatch> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const override {
    auto new_schema = schema_->WithMetadata(metadata);
    return RecordBatch::Make(std::move(new_schema), num_rows_, columns_, device_type_,
                             sync_event_);
  }

  std::shared_ptr<RecordBatch> Slice(int64_t offset, int64_t length) const override {
    std::vector<std::shared_ptr<ArrayData>> arrays;
    arrays.reserve(num_columns());
    for (const auto& field : columns_) {
      arrays.emplace_back(field->Slice(offset, length));
    }
    int64_t num_rows = std::min(num_rows_ - offset, length);
    return std::make_shared<SimpleRecordBatch>(schema_, num_rows, std::move(arrays),
                                               device_type_, sync_event_);
  }

  Status Validate() const override {
    if (static_cast<int>(columns_.size()) != schema_->num_fields()) {
      return Status::Invalid("Number of columns did not match schema");
    }
    return RecordBatch::Validate();
  }

  const std::shared_ptr<Device::SyncEvent>& GetSyncEvent() const override {
    return sync_event_;
  }

  DeviceAllocationType device_type() const override { return device_type_; }

 private:
  std::vector<std::shared_ptr<ArrayData>> columns_;

  // Caching boxed array data
  mutable std::vector<std::shared_ptr<Array>> boxed_columns_;

  // the type of device that the buffers for columns are allocated on.
  // all columns should be on the same type of device.
  DeviceAllocationType device_type_;
  std::shared_ptr<Device::SyncEvent> sync_event_;
};

RecordBatch::RecordBatch(std::shared_ptr<Schema> schema, int64_t num_rows)
    : schema_(std::move(schema)), num_rows_(num_rows) {}

std::shared_ptr<RecordBatch> RecordBatch::Make(
    std::shared_ptr<Schema> schema, int64_t num_rows,
    std::vector<std::shared_ptr<Array>> columns,
    std::shared_ptr<Device::SyncEvent> sync_event) {
  DCHECK_EQ(schema->num_fields(), static_cast<int>(columns.size()));
  return std::make_shared<SimpleRecordBatch>(std::move(schema), num_rows,
                                             std::move(columns), std::move(sync_event));
}

std::shared_ptr<RecordBatch> RecordBatch::Make(
    std::shared_ptr<Schema> schema, int64_t num_rows,
    std::vector<std::shared_ptr<ArrayData>> columns, DeviceAllocationType device_type,
    std::shared_ptr<Device::SyncEvent> sync_event) {
  DCHECK_EQ(schema->num_fields(), static_cast<int>(columns.size()));
  return std::make_shared<SimpleRecordBatch>(std::move(schema), num_rows,
                                             std::move(columns), device_type,
                                             std::move(sync_event));
}

Result<std::shared_ptr<RecordBatch>> RecordBatch::MakeEmpty(
    std::shared_ptr<Schema> schema, MemoryPool* memory_pool) {
  ArrayVector empty_batch(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); i++) {
    ARROW_ASSIGN_OR_RAISE(empty_batch[i],
                          MakeEmptyArray(schema->field(i)->type(), memory_pool));
  }
  return RecordBatch::Make(std::move(schema), 0, std::move(empty_batch));
}

Result<std::shared_ptr<RecordBatch>> RecordBatch::FromStructArray(
    const std::shared_ptr<Array>& array, MemoryPool* memory_pool) {
  if (array->type_id() != Type::STRUCT) {
    return Status::TypeError("Cannot construct record batch from array of type ",
                             *array->type());
  }
  if (array->null_count() != 0 || array->offset() != 0) {
    // If the struct array has a validity map or offset we need to push those into
    // the child arrays via Flatten since the RecordBatch doesn't have validity/offset
    const std::shared_ptr<StructArray>& struct_array =
        internal::checked_pointer_cast<StructArray>(array);
    ARROW_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Array>> fields,
                          struct_array->Flatten(memory_pool));
    return Make(arrow::schema(array->type()->fields()), array->length(),
                std::move(fields));
  }
  return Make(arrow::schema(array->type()->fields()), array->length(),
              array->data()->child_data);
}

namespace {

Status ValidateColumnLength(const RecordBatch& batch, int i) {
  const auto& array = *batch.column(i);
  if (ARROW_PREDICT_FALSE(array.length() != batch.num_rows())) {
    return Status::Invalid("Number of rows in column ", i,
                           " did not match batch: ", array.length(), " vs ",
                           batch.num_rows());
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<StructArray>> RecordBatch::ToStructArray() const {
  if (num_columns() != 0) {
    // Only check the first column because `StructArray::Make` already checks that the
    // child lengths are equal.
    RETURN_NOT_OK(ValidateColumnLength(*this, 0));
    return StructArray::Make(columns(), schema()->fields());
  }
  return std::make_shared<StructArray>(arrow::struct_({}), num_rows_,
                                       std::vector<std::shared_ptr<Array>>{},
                                       /*null_bitmap=*/nullptr,
                                       /*null_count=*/0,
                                       /*offset=*/0);
}

Result<std::shared_ptr<Tensor>> RecordBatch::ToTensor(bool null_to_nan, bool row_major,
                                                      MemoryPool* pool) const {
  std::shared_ptr<Tensor> tensor;
  ARROW_RETURN_NOT_OK(
      internal::RecordBatchToTensor(*this, null_to_nan, row_major, pool, &tensor));
  return tensor;
}

const std::string& RecordBatch::column_name(int i) const {
  return schema_->field(i)->name();
}

namespace {

bool ContainFloatType(const std::shared_ptr<DataType>& type) {
  if (is_floating(type->id())) {
    return true;
  }

  for (const auto& field : type->fields()) {
    if (ContainFloatType(field->type())) {
      return true;
    }
  }

  return false;
}

bool ContainFloatType(const Schema& schema) {
  for (auto& field : schema.fields()) {
    if (ContainFloatType(field->type())) {
      return true;
    }
  }
  return false;
}

bool CanIgnoreNaNInEquality(const RecordBatch& batch, const EqualOptions& opts) {
  if (opts.nans_equal()) {
    return true;
  } else if (!ContainFloatType(*batch.schema())) {
    return true;
  } else {
    return false;
  }
}

}  // namespace

bool RecordBatch::Equals(const RecordBatch& other, bool check_metadata,
                         const EqualOptions& opts) const {
  return Equals(other, opts.use_metadata(check_metadata));
}

bool RecordBatch::Equals(const RecordBatch& other, const EqualOptions& opts) const {
  if (this == &other) {
    if (CanIgnoreNaNInEquality(*this, opts)) {
      return true;
    }
  } else {
    if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
      return false;
    } else if (opts.use_schema() &&
               !schema_->Equals(*other.schema(), opts.use_metadata())) {
      return false;
    } else if (device_type() != other.device_type()) {
      return false;
    }
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->Equals(other.column(i), opts)) {
      return false;
    }
  }

  return true;
}

Result<std::shared_ptr<RecordBatch>> RecordBatch::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  if (schema_->num_fields() != schema->num_fields())
    return Status::Invalid("RecordBatch schema fields", schema_->num_fields(),
                           ", did not match new schema fields: ", schema->num_fields());
  auto fields = schema_->fields();
  int n_fields = static_cast<int>(fields.size());
  for (int i = 0; i < n_fields; i++) {
    auto old_type = fields[i]->type();
    auto replace_type = schema->field(i)->type();
    if (!old_type->Equals(replace_type)) {
      return Status::Invalid(
          "RecordBatch schema field index ", i, " type is ", old_type->ToString(),
          ", did not match new schema field type: ", replace_type->ToString());
    }
  }
  return RecordBatch::Make(std::move(schema), num_rows(), columns(), GetSyncEvent());
}

std::vector<std::string> RecordBatch::ColumnNames() const {
  std::vector<std::string> names(num_columns());
  for (int i = 0; i < num_columns(); ++i) {
    names[i] = schema()->field(i)->name();
  }
  return names;
}

Result<std::shared_ptr<RecordBatch>> RecordBatch::RenameColumns(
    const std::vector<std::string>& names) const {
  int n = num_columns();

  if (static_cast<int>(names.size()) != n) {
    return Status::Invalid("tried to rename a record batch of ", n, " columns but only ",
                           names.size(), " names were provided");
  }

  ArrayVector columns(n);
  FieldVector fields(n);

  for (int i = 0; i < n; ++i) {
    columns[i] = column(i);
    fields[i] = schema()->field(i)->WithName(names[i]);
  }

  return RecordBatch::Make(::arrow::schema(std::move(fields)), num_rows(),
                           std::move(columns), GetSyncEvent());
}

Result<std::shared_ptr<RecordBatch>> RecordBatch::SelectColumns(
    const std::vector<int>& indices) const {
  int n = static_cast<int>(indices.size());

  FieldVector fields(n);
  ArrayVector columns(n);

  for (int i = 0; i < n; i++) {
    int pos = indices[i];
    if (pos < 0 || pos > num_columns() - 1) {
      return Status::Invalid("Invalid column index ", pos, " to select columns.");
    }
    fields[i] = schema()->field(pos);
    columns[i] = column(pos);
  }

  auto new_schema =
      std::make_shared<arrow::Schema>(std::move(fields), schema()->metadata());
  return RecordBatch::Make(std::move(new_schema), num_rows(), std::move(columns),
                           GetSyncEvent());
}

std::shared_ptr<RecordBatch> RecordBatch::Slice(int64_t offset) const {
  return Slice(offset, this->num_rows() - offset);
}

std::string RecordBatch::ToString() const {
  std::stringstream ss;
  ARROW_CHECK_OK(PrettyPrint(*this, 0, &ss));
  return ss.str();
}

namespace {

Status ValidateBatch(const RecordBatch& batch, bool full_validation) {
  for (int i = 0; i < batch.num_columns(); ++i) {
    RETURN_NOT_OK(ValidateColumnLength(batch, i));
    const auto& array = *batch.column(i);
    const auto& schema_type = batch.schema()->field(i)->type();
    if (!array.type()->Equals(schema_type)) {
      return Status::Invalid("Column ", i,
                             " type not match schema: ", array.type()->ToString(), " vs ",
                             schema_type->ToString());
    }
    const auto st = full_validation ? internal::ValidateArrayFull(array)
                                    : internal::ValidateArray(array);
    if (!st.ok()) {
      return Status::Invalid("In column ", i, ": ", st.ToString());
    }
  }
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<RecordBatch>> RecordBatch::CopyTo(
    const std::shared_ptr<MemoryManager>& to) const {
  ArrayVector copied_columns;
  copied_columns.reserve(num_columns());
  for (const auto& col : columns()) {
    ARROW_ASSIGN_OR_RAISE(auto c, col->CopyTo(to));
    copied_columns.push_back(std::move(c));
  }

  return Make(schema_, num_rows(), std::move(copied_columns));
}

Result<std::shared_ptr<RecordBatch>> RecordBatch::ViewOrCopyTo(
    const std::shared_ptr<MemoryManager>& to) const {
  ArrayVector copied_columns;
  copied_columns.reserve(num_columns());
  for (const auto& col : columns()) {
    ARROW_ASSIGN_OR_RAISE(auto c, col->ViewOrCopyTo(to));
    copied_columns.push_back(std::move(c));
  }

  return Make(schema_, num_rows(), std::move(copied_columns));
}

namespace {
struct EnumeratedStatistics {
  int nth_statistics = 0;
  bool start_new_column = false;
  std::optional<int32_t> nth_column = std::nullopt;
  const char* key = nullptr;
  std::shared_ptr<DataType> type = nullptr;
  ArrayStatistics::ValueType value = false;
};
using OnStatistics =
    std::function<Status(const EnumeratedStatistics& enumerated_statistics)>;
Status EnumerateStatistics(const RecordBatch& record_batch, OnStatistics on_statistics) {
  EnumeratedStatistics statistics;
  statistics.nth_statistics = 0;
  statistics.start_new_column = true;
  statistics.nth_column = std::nullopt;

  statistics.key = ARROW_STATISTICS_KEY_ROW_COUNT_EXACT;
  statistics.type = int64();
  statistics.value = record_batch.num_rows();
  RETURN_NOT_OK(on_statistics(statistics));
  statistics.start_new_column = false;

  const auto& schema = record_batch.schema();
  const auto num_fields = schema->num_fields();
  for (int nth_column = 0; nth_column < num_fields; ++nth_column) {
    const auto& field = schema->field(nth_column);
    auto column_statistics = record_batch.column(nth_column)->statistics();
    if (!column_statistics) {
      continue;
    }

    statistics.start_new_column = true;
    statistics.nth_column = nth_column;
    if (column_statistics->null_count.has_value()) {
      statistics.nth_statistics++;
      statistics.key = ARROW_STATISTICS_KEY_NULL_COUNT_EXACT;
      statistics.type = int64();
      statistics.value = column_statistics->null_count.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->distinct_count.has_value()) {
      statistics.nth_statistics++;
      if (std::holds_alternative<int64_t>(column_statistics->distinct_count.value())) {
        statistics.key = ARROW_STATISTICS_KEY_DISTINCT_COUNT_EXACT;
        statistics.type = int64();
        statistics.value = std::get<int64_t>(column_statistics->distinct_count.value());
      } else {
        statistics.key = ARROW_STATISTICS_KEY_DISTINCT_COUNT_APPROXIMATE;
        statistics.type = float64();
        statistics.value = std::get<double>(column_statistics->distinct_count.value());
      }

      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->average_byte_width.has_value()) {
      statistics.nth_statistics++;
      if (column_statistics->is_average_byte_width_exact) {
        statistics.key = ARROW_STATISTICS_KEY_AVERAGE_BYTE_WIDTH_EXACT;
      } else {
        statistics.key = ARROW_STATISTICS_KEY_AVERAGE_BYTE_WIDTH_APPROXIMATE;
      }
      statistics.type = float64();
      statistics.value = column_statistics->average_byte_width.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->min.has_value()) {
      statistics.nth_statistics++;
      if (column_statistics->is_min_exact) {
        statistics.key = ARROW_STATISTICS_KEY_MIN_VALUE_EXACT;
      } else {
        statistics.key = ARROW_STATISTICS_KEY_MIN_VALUE_APPROXIMATE;
      }
      statistics.type = column_statistics->MinArrowType(field->type());
      statistics.value = column_statistics->min.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }

    if (column_statistics->max.has_value()) {
      statistics.nth_statistics++;
      if (column_statistics->is_max_exact) {
        statistics.key = ARROW_STATISTICS_KEY_MAX_VALUE_EXACT;
      } else {
        statistics.key = ARROW_STATISTICS_KEY_MAX_VALUE_APPROXIMATE;
      }
      statistics.type = column_statistics->MaxArrowType(field->type());
      statistics.value = column_statistics->max.value();
      RETURN_NOT_OK(on_statistics(statistics));
      statistics.start_new_column = false;
    }
  }
  return Status::OK();
}
struct StringBuilderVisitor {
  template <typename DataType>
  enable_if_has_string_view<DataType, Status> Visit(const DataType&,
                                                    ArrayBuilder* raw_builder,
                                                    const std::string& value) {
    using Builder = typename TypeTraits<DataType>::BuilderType;
    auto builder = static_cast<Builder*>(raw_builder);
    return builder->Append(value);
  }

  Status Visit(const DataType& type, ArrayBuilder*, const std::string&) {
    return Status::Invalid("Only string types are supported and the current type is ",
                           type.ToString());
  }
};
}  // namespace

Result<std::shared_ptr<Array>> RecordBatch::MakeStatisticsArray(
    MemoryPool* memory_pool) const {
  // Statistics schema:
  // struct<
  //   column: int32,
  //   statistics: map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  // >

  // Statistics schema doesn't define static dense union type for
  // values. Each statistics schema have a dense union type that has
  // needled value types. The following block collects these types.
  std::vector<std::shared_ptr<Field>> values_types;
  std::vector<int8_t> values_type_indexes;
  RETURN_NOT_OK(EnumerateStatistics(*this, [&](const EnumeratedStatistics& statistics) {
    int8_t i = 0;
    for (const auto& field : values_types) {
      if (field->type()->Equals(statistics.type)) {
        break;
      }
      i++;
    }
    if (i == static_cast<int8_t>(values_types.size())) {
      values_types.push_back(field(statistics.type->name(), statistics.type));
    }
    values_type_indexes.push_back(i);
    return Status::OK();
  }));

  // statistics.key: dictionary<indices: int32, dictionary: utf8>
  auto keys_type = dictionary(int32(), utf8(), false);
  // statistics.items: dense_union<...all needed types...>
  auto values_type = dense_union(values_types);
  // struct<
  //   column: int32,
  //   statistics: map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  // >
  auto statistics_type =
      struct_({field("column", int32()),
               field("statistics", map(keys_type, values_type, false))});

  std::vector<std::shared_ptr<ArrayBuilder>> field_builders;
  // columns: int32
  auto columns_builder = std::make_shared<Int32Builder>(memory_pool);
  field_builders.push_back(std::static_pointer_cast<ArrayBuilder>(columns_builder));
  // statistics.key: dictionary<indices: int32, dictionary: utf8>
  auto keys_builder = std::make_shared<StringDictionary32Builder>();
  // statistics.items: dense_union<...all needed types...>
  std::vector<std::shared_ptr<ArrayBuilder>> values_builders;
  for (const auto& values_type : values_types) {
    std::unique_ptr<ArrayBuilder> values_builder;
    RETURN_NOT_OK(MakeBuilder(memory_pool, values_type->type(), &values_builder));
    values_builders.push_back(std::shared_ptr<ArrayBuilder>(std::move(values_builder)));
  }
  auto items_builder = std::make_shared<DenseUnionBuilder>(
      memory_pool, std::move(values_builders), values_type);
  // statistics:
  //   map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  auto values_builder = std::make_shared<MapBuilder>(
      memory_pool, std::static_pointer_cast<ArrayBuilder>(keys_builder),
      std::static_pointer_cast<ArrayBuilder>(items_builder));
  field_builders.push_back(std::static_pointer_cast<ArrayBuilder>(values_builder));
  // struct<
  //   column: int32,
  //   statistics: map<
  //     key: dictionary<
  //       indices: int32,
  //       dictionary: utf8,
  //     >,
  //     items: dense_union<...all needed types...>,
  //   >
  // >
  StructBuilder builder(statistics_type, memory_pool, std::move(field_builders));

  // Append statistics.
  RETURN_NOT_OK(EnumerateStatistics(*this, [&](const EnumeratedStatistics& statistics) {
    if (statistics.start_new_column) {
      RETURN_NOT_OK(builder.Append());
      if (statistics.nth_column.has_value()) {
        // Add Columns
        RETURN_NOT_OK(columns_builder->Append(statistics.nth_column.value()));
      } else {
        // Add RecordBatch
        RETURN_NOT_OK(columns_builder->AppendNull());
      }
      RETURN_NOT_OK(values_builder->Append());
    }
    RETURN_NOT_OK(keys_builder->Append(statistics.key,
                                       static_cast<int32_t>(strlen(statistics.key))));
    const auto values_type_index = values_type_indexes[statistics.nth_statistics];
    RETURN_NOT_OK(items_builder->Append(values_type_index));
    struct Visitor {
      ArrayBuilder* builder;

      Status operator()(const bool& value) {
        return static_cast<BooleanBuilder*>(builder)->Append(value);
      }
      Status operator()(const int64_t& value) {
        return static_cast<Int64Builder*>(builder)->Append(value);
      }
      Status operator()(const uint64_t& value) {
        return static_cast<UInt64Builder*>(builder)->Append(value);
      }
      Status operator()(const double& value) {
        return static_cast<DoubleBuilder*>(builder)->Append(value);
      }
      Status operator()(const std::string& value) {
        StringBuilderVisitor visitor;
        return VisitTypeInline(*builder->type(), &visitor, builder, value);
      }
    } visitor;
    visitor.builder = values_builders[values_type_index].get();
    RETURN_NOT_OK(std::visit(visitor, statistics.value));
    return Status::OK();
  }));

  return builder.Finish();
}

Status RecordBatch::Validate() const {
  return ValidateBatch(*this, /*full_validation=*/false);
}

Status RecordBatch::ValidateFull() const {
  return ValidateBatch(*this, /*full_validation=*/true);
}

// ----------------------------------------------------------------------
// Base record batch reader

Result<RecordBatchVector> RecordBatchReader::ToRecordBatches() {
  RecordBatchVector batches;
  while (true) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(ReadNext(&batch));
    if (!batch) {
      break;
    }
    batches.emplace_back(std::move(batch));
  }
  return batches;
}

Result<std::shared_ptr<Table>> RecordBatchReader::ToTable() {
  ARROW_ASSIGN_OR_RAISE(auto batches, ToRecordBatches());
  return Table::FromRecordBatches(schema(), std::move(batches));
}

class SimpleRecordBatchReader : public RecordBatchReader {
 public:
  SimpleRecordBatchReader(Iterator<std::shared_ptr<RecordBatch>> it,
                          std::shared_ptr<Schema> schema,
                          DeviceAllocationType device_type = DeviceAllocationType::kCPU)
      : schema_(std::move(schema)), it_(std::move(it)), device_type_(device_type) {}

  SimpleRecordBatchReader(std::vector<std::shared_ptr<RecordBatch>> batches,
                          std::shared_ptr<Schema> schema,
                          DeviceAllocationType device_type = DeviceAllocationType::kCPU)
      : schema_(std::move(schema)),
        it_(MakeVectorIterator(std::move(batches))),
        device_type_(device_type) {}

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    return it_.Next().Value(batch);
  }

  std::shared_ptr<Schema> schema() const override { return schema_; }

  DeviceAllocationType device_type() const override { return device_type_; }

 protected:
  std::shared_ptr<Schema> schema_;
  Iterator<std::shared_ptr<RecordBatch>> it_;
  DeviceAllocationType device_type_;
};

Result<std::shared_ptr<RecordBatchReader>> RecordBatchReader::Make(
    std::vector<std::shared_ptr<RecordBatch>> batches, std::shared_ptr<Schema> schema,
    DeviceAllocationType device_type) {
  if (schema == nullptr) {
    if (batches.size() == 0 || batches[0] == nullptr) {
      return Status::Invalid("Cannot infer schema from empty vector or nullptr");
    }

    schema = batches[0]->schema();
  }

  return std::make_shared<SimpleRecordBatchReader>(std::move(batches), std::move(schema),
                                                   device_type);
}

Result<std::shared_ptr<RecordBatchReader>> RecordBatchReader::MakeFromIterator(
    Iterator<std::shared_ptr<RecordBatch>> batches, std::shared_ptr<Schema> schema,
    DeviceAllocationType device_type) {
  if (schema == nullptr) {
    return Status::Invalid("Schema cannot be nullptr");
  }

  return std::make_shared<SimpleRecordBatchReader>(std::move(batches), std::move(schema),
                                                   device_type);
}

RecordBatchReader::~RecordBatchReader() {
  ARROW_WARN_NOT_OK(this->Close(), "Implicitly called RecordBatchReader::Close failed");
}

Result<std::shared_ptr<RecordBatch>> ConcatenateRecordBatches(
    const RecordBatchVector& batches, MemoryPool* pool) {
  int64_t length = 0;
  size_t n = batches.size();
  if (n == 0) {
    return Status::Invalid("Must pass at least one recordbatch");
  }
  int cols = batches[0]->num_columns();
  auto schema = batches[0]->schema();
  for (size_t i = 0; i < batches.size(); ++i) {
    if (auto sync = batches[i]->GetSyncEvent()) {
      ARROW_RETURN_NOT_OK(sync->Wait());
    }

    length += batches[i]->num_rows();
    if (!schema->Equals(batches[i]->schema())) {
      return Status::Invalid(
          "Schema of RecordBatch index ", i, " is ", batches[i]->schema()->ToString(),
          ", which does not match index 0 recordbatch schema: ", schema->ToString());
    }
  }

  std::vector<std::shared_ptr<Array>> concatenated_columns;
  concatenated_columns.reserve(cols);
  for (int col = 0; col < cols; ++col) {
    ArrayVector column_arrays;
    column_arrays.reserve(batches.size());
    for (const auto& batch : batches) {
      column_arrays.emplace_back(batch->column(col));
    }
    ARROW_ASSIGN_OR_RAISE(auto concatenated_column, Concatenate(column_arrays, pool))
    concatenated_columns.emplace_back(std::move(concatenated_column));
  }
  return RecordBatch::Make(std::move(schema), length, std::move(concatenated_columns));
}

}  // namespace arrow
