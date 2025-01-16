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

#include "arrow/compute/test_util_internal.h"

#include "arrow/array/array_base.h"
#include "arrow/array/validate.h"
#include "arrow/chunked_array.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/vector.h"

namespace arrow::compute {

using compute::ExecBatch;
using internal::MapVector;

ExecBatch ExecBatchFromJSON(const std::vector<TypeHolder>& types, std::string_view json) {
  auto fields = ::arrow::internal::MapVector(
      [](const TypeHolder& th) { return field("", th.GetSharedPtr()); }, types);

  ExecBatch batch{*RecordBatchFromJSON(schema(std::move(fields)), json)};

  return batch;
}

ExecBatch ExecBatchFromJSON(const std::vector<TypeHolder>& types,
                            const std::vector<ArgShape>& shapes, std::string_view json) {
  DCHECK_EQ(types.size(), shapes.size());

  ExecBatch batch = ExecBatchFromJSON(types, json);

  auto value_it = batch.values.begin();
  for (ArgShape shape : shapes) {
    if (shape == ArgShape::SCALAR) {
      if (batch.length == 0) {
        *value_it = MakeNullScalar(value_it->type());
      } else {
        *value_it = value_it->make_array()->GetScalar(0).ValueOrDie();
      }
    }
    ++value_it;
  }

  return batch;
}

namespace {

void ValidateOutputImpl(const ArrayData& output) {
  ASSERT_OK(::arrow::internal::ValidateArrayFull(output));
  TestInitialized(output);
}

void ValidateOutputImpl(const ChunkedArray& output) {
  ASSERT_OK(output.ValidateFull());
  for (const auto& chunk : output.chunks()) {
    TestInitialized(*chunk);
  }
}

void ValidateOutputImpl(const RecordBatch& output) {
  ASSERT_OK(output.ValidateFull());
  for (const auto& column : output.column_data()) {
    TestInitialized(*column);
  }
}

void ValidateOutputImpl(const Table& output) {
  ASSERT_OK(output.ValidateFull());
  for (const auto& column : output.columns()) {
    for (const auto& chunk : column->chunks()) {
      TestInitialized(*chunk);
    }
  }
}

void ValidateOutputImpl(const Scalar& output) { ASSERT_OK(output.ValidateFull()); }

}  // namespace

void ValidateOutput(const Datum& output) {
  switch (output.kind()) {
    case Datum::ARRAY:
      ValidateOutputImpl(*output.array());
      break;
    case Datum::CHUNKED_ARRAY:
      ValidateOutputImpl(*output.chunked_array());
      break;
    case Datum::RECORD_BATCH:
      ValidateOutputImpl(*output.record_batch());
      break;
    case Datum::TABLE:
      ValidateOutputImpl(*output.table());
      break;
    case Datum::SCALAR:
      ValidateOutputImpl(*output.scalar());
      break;
    default:
      break;
  }
}

}  // namespace arrow::compute
