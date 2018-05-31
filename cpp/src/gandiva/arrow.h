/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_EXPR_ARROW_H
#define GANDIVA_EXPR_ARROW_H

#include <memory>
#include <vector>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace gandiva {

using ArrayPtr = std::shared_ptr<arrow::Array>;

using ArrayBuilderPtr = std::shared_ptr<arrow::ArrayBuilder>;
using ArrayBuilderVector = std::vector<ArrayBuilderPtr>;

using DataTypePtr = std::shared_ptr<arrow::DataType>;
using DataTypeVector = std::vector<DataTypePtr>;

using FieldPtr = std::shared_ptr<arrow::Field>;
using FieldVector = std::vector<FieldPtr>;

using RecordBatchPtr = std::shared_ptr<arrow::RecordBatch>;

using SchemaPtr = std::shared_ptr<arrow::Schema>;

} // namespace gandiva

#endif // GANDIVA_EXPR_ARROW_H
