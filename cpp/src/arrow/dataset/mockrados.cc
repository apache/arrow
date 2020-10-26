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

#include "arrow/dataset/mockrados.h"

#include <time.h>

namespace arrow {
namespace dataset {

std::shared_ptr<RecordBatch> generate_test_record_batch() {
    // Initialize random seed
    srand (time(NULL));

    // The number of rows that the Record Batch will contain
    int64_t row_count = 100;

    // Define a schema
    auto schema_ = schema({field("f1", int64()), field("f2", int64())});
    
    // Build the `f1` column
    auto f1_builder = std::make_shared<Int64Builder>();
    f1_builder->Reset();
    for(auto i = 0; i < row_count; i++) {
        f1_builder->Append(rand());
    }
    std::shared_ptr<Array> batch_size_array;
    f1_builder->Finish(&batch_size_array);

    // Build the `f2` column
    auto f2_builder = std::make_shared<Int64Builder>();
    f2_builder->Reset();
    for(auto i = 0; i < row_count; i++) {
        f2_builder->Append(rand());
    }
    std::shared_ptr<Array> seq_num_array;
    f2_builder->Finish(&seq_num_array);

    // Build the Record Batch
    std::vector<std::shared_ptr<Array>> columns = {
            batch_size_array,
            seq_num_array
    };
    return RecordBatch::Make(schema_, row_count, columns);
}

std::shared_ptr<Table> generate_test_table() {
    RecordBatchVector batches;
    for (int i = 0; i < 8; i++) {
        batches.push_back(generate_test_record_batch());
    }
    // Build a Table having 8 Record Batches
    auto table = Table::FromRecordBatches(batches).ValueOrDie();
    return table;
}

}  // namespace dataset
}  // namespace arrow