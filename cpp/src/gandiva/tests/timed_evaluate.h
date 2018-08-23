// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>
#include <vector>
#include "gandiva/arrow.h"
#include "gandiva/projector.h"
#include "integ/generate_data.h"

#ifndef GANDIVA_TIMED_EVALUATE_H
#define GANDIVA_TIMED_EVALUATE_H

#define THOUSAND (1024)
#define MILLION (1024 * 1024)

namespace gandiva {

template <typename C_TYPE>
std::vector<C_TYPE> GenerateData(int num_records, DataGenerator<C_TYPE> &data_generator) {
  std::vector<C_TYPE> data;

  for (int i = 0; i < num_records; i++) {
    data.push_back(data_generator.GenerateData());
  }

  return data;
}

template <typename TYPE, typename C_TYPE>
Status TimedEvaluate(SchemaPtr schema, std::shared_ptr<Projector> projector,
                     DataGenerator<C_TYPE> &data_generator, arrow::MemoryPool *pool,
                     int num_records, int batch_size, int64_t &num_millis) {
  int num_remaining = num_records;
  int num_fields = schema->num_fields();
  int num_calls = 0;
  Status status;
  std::chrono::duration<int64_t, std::micro> micros(0);
  std::chrono::time_point<std::chrono::high_resolution_clock> start;
  std::chrono::time_point<std::chrono::high_resolution_clock> finish;

  while (num_remaining > 0) {
    int num_in_batch = batch_size;
    if (batch_size > num_remaining) {
      num_in_batch = num_remaining;
    }

    // generate data for all columns in the schema
    std::vector<ArrayPtr> columns;
    for (int col = 0; col < num_fields; col++) {
      std::vector<C_TYPE> data = GenerateData<C_TYPE>(num_in_batch, data_generator);
      std::vector<bool> validity(num_in_batch, true);
      ArrayPtr col_data = MakeArrowArray<TYPE, C_TYPE>(data, validity);

      columns.push_back(col_data);
    }

    // make the record batch
    auto in_batch = arrow::RecordBatch::Make(schema, num_in_batch, columns);

    // evaluate
    arrow::ArrayVector outputs;
    start = std::chrono::high_resolution_clock::now();
    status = projector->Evaluate(*in_batch, pool, &outputs);
    finish = std::chrono::high_resolution_clock::now();
    if (!status.ok()) {
      return status;
    }

    micros += std::chrono::duration_cast<std::chrono::microseconds>(finish - start);
    num_calls++;
    num_remaining -= num_in_batch;
  }

  num_millis = micros.count() / 1000;
  return Status::OK();
}

}  // namespace gandiva

#endif  // GANDIVA_TIMED_EVALUATE_H
