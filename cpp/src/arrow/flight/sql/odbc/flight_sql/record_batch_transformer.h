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

#pragma once

#include <arrow/flight/client.h>
#include <arrow/type.h>
#include <memory>

namespace driver {
namespace flight_sql {

using arrow::Array;
using arrow::DataType;
using arrow::Field;
using arrow::RecordBatch;
using arrow::Schema;

typedef std::function<std::shared_ptr<Array>(
    const std::shared_ptr<RecordBatch>& original_record_batch,
    const std::shared_ptr<Schema>& transformed_schema)>
    TransformTask;

/// A base class to implement different types of transformer.
class RecordBatchTransformer {
 public:
  virtual ~RecordBatchTransformer() = default;

  /// Execute the transformation based on predeclared tasks created by
  /// RenameField() method and/or AddFieldOfNulls().
  /// \param original     The original RecordBatch that will be used as base
  ///                     for the transformation.
  /// \return The new transformed RecordBatch.
  virtual std::shared_ptr<RecordBatch> Transform(
      const std::shared_ptr<RecordBatch>& original) = 0;

  /// Use the new list of fields constructed during creation of task
  /// to return the new schema.
  /// \return     the schema from the transformedRecordBatch.
  virtual std::shared_ptr<Schema> GetTransformedSchema() = 0;
};

class RecordBatchTransformerWithTasksBuilder {
 private:
  std::vector<std::shared_ptr<Field>> new_fields_;
  std::vector<TransformTask> task_collection_;
  std::shared_ptr<Schema> schema_;

 public:
  /// Based on the original array name and in a target array name it prepares
  /// a task that will execute the transformation.
  /// \param original_name     The original name of the field.
  /// \param transformed_name  The name after the transformation.
  RecordBatchTransformerWithTasksBuilder& RenameField(
      const std::string& original_name, const std::string& transformed_name);

  /// Add an empty field to the transformed record batch.
  /// \param field_name   The name of the empty fields.
  /// \param data_type    The target data type for the new fields.
  RecordBatchTransformerWithTasksBuilder& AddFieldOfNulls(
      const std::string& field_name, const std::shared_ptr<DataType>& data_type);

  /// It creates an object of RecordBatchTransformerWithTasksBuilder
  /// \return a RecordBatchTransformerWithTasksBuilder object.
  std::shared_ptr<RecordBatchTransformer> Build();

  /// Instantiate a RecordBatchTransformerWithTasksBuilder object.
  /// \param schema   The schema from the original RecordBatch.
  explicit RecordBatchTransformerWithTasksBuilder(std::shared_ptr<Schema> schema);
};

}  // namespace flight_sql
}  // namespace driver
