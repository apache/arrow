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

#include "arrow/type.h"
#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/dataset/rados_utils.h"

namespace arrow {
namespace dataset {

std::shared_ptr<RecordBatch> GenerateTestRecordBatch(){
    /* initialize random seed: */
    srand (time(NULL));

    int64_t rows_num = 100;

    std::shared_ptr<Field> f1 = field("f1", int64());
    std::shared_ptr<Field> f2 = field("f2", int64());

    std::vector<std::shared_ptr<Field>> schema_vector = {
            f1,
            f2,
    };
    std::shared_ptr<Schema> schema = std::make_shared<arrow::Schema>(schema_vector);

    auto f1_builder = std::make_shared<Int64Builder>();
    f1_builder->Reset();
    for(auto i = 0; i < rows_num; i++) {
        f1_builder->Append(rand());
    }
    std::shared_ptr<Array> batch_size_array;
    f1_builder->Finish(&batch_size_array);

    auto f2_builder = std::make_shared<Int64Builder>();
    f2_builder->Reset();
    for(auto i = 0; i < rows_num; i++) {
        f2_builder->Append(rand());
    }
    std::shared_ptr<Array> seq_num_array;
    f2_builder->Finish(&seq_num_array);

    std::vector<std::shared_ptr<Array>> columns = {
            batch_size_array,
            seq_num_array
    };

    return RecordBatch::Make(schema, rows_num, columns);
}

std::shared_ptr<Table> GenerateTestTable() {
    RecordBatchVector batches;
    for (int i = 0; i < 8; i++) {
        batches.push_back(GenerateTestRecordBatch());
    }
    return Table::FromRecordBatches(batches).ValueOrDie();
}

Status get_test_bufferlist(librados::bufferlist &bl) {
    librados::bufferlist result;
    auto table = GenerateTestTable();
    write_table_to_bufferlist(table, result);

    bl = result;
    return Status::OK();
}

}  // namespace dataset
}  // namespace arrow