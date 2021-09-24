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

#include <arrow/api.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <iostream>
#include <vector>

// Many operations in Apache arrow operate on
// columns of data, and the columns of data are
// assembled into a table. In this example, we
// examine how to compare two arrays which are
// combined to form a table that is then written
// out to a CSV file.

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);


int main(int argc, char** argv){
        const char* csv_filename = "output.csv";

        // Make Arrays
        arrow::NumericBuilder<arrow::Int64Type>  int64_builder;
        arrow::BooleanBuilder boolean_builder;

        // Make place for 8 values in total
        ABORT_ON_FAILURE(int64_builder.Resize(8));
        ABORT_ON_FAILURE(boolean_builder.Resize(8));

        // Bulk append the given values
        std::vector<int64_t> int64_values = {1, 2, 3, 4, 5, 6, 7, 8};
        ABORT_ON_FAILURE(int64_builder.AppendValues(int64_values));
        std::shared_ptr<arrow::Array> array_a;
        ABORT_ON_FAILURE(int64_builder.Finish(&array_a));
        int64_builder.Reset();
        int64_values = {2, 5, 1, 3, 6, 2, 7, 4};
        std::shared_ptr<arrow::Array> array_b;
        ABORT_ON_FAILURE(int64_builder.AppendValues(int64_values));
        ABORT_ON_FAILURE(int64_builder.Finish(&array_b));

        // Cast the Arrays to their actual types
        auto int64_array_a = std::static_pointer_cast<arrow::Int64Array>(array_a);
        auto int64_array_b = std::static_pointer_cast<arrow::Int64Array>(array_b);
        for(int64_t i=0; i < 8; i++)
        {
                if( (!int64_array_a->IsNull(i)) & 
                    (!int64_array_b->IsNull(i))    )
                {
                  bool comparison_result = int64_array_a->Value(i) > 
                                           int64_array_b->Value(i);
                  boolean_builder.UnsafeAppend(comparison_result);
                }
        }
        std::shared_ptr<arrow::Array> array_c;
        ABORT_ON_FAILURE(boolean_builder.Finish(&array_c));
        //auto bool_array_c  = std::static_pointer_cast<arrow::BooleanArray>(array_c);
        std::cout << "Array created" << std::endl;

        // Try a compute function
        arrow::Datum compared_datum;
        std::shared_ptr<arrow::Array> compared_array;
        //auto bool_compared_array  = std::static_pointer_cast<arrow::BooleanArray>(compared_array);
        arrow::Result<arrow::Datum> st_compared_datum = 
                arrow::compute::CallFunction("greater",{array_a,array_b});
        if (st_compared_datum.ok()) {
                compared_datum = std::move(st_compared_datum).ValueOrDie();
                compared_array = compared_datum.make_array();
        }else{
                std::cerr << st_compared_datum.status() << std::endl;
        }
        // Create a table
        auto schema = arrow::schema({arrow::field("a", arrow::int64()),
                                     arrow::field("b", arrow::int64()),
                                     arrow::field("c", arrow::boolean()),
                                     arrow::field("d", arrow::boolean())  });
        std::shared_ptr<arrow::Table> my_table = arrow::Table::Make(schema, {array_a,array_b,array_c,
                                                           compared_array});

        std::cout << "Table created" << std::endl;

        // Write table to CSV file
        std::shared_ptr<arrow::io::FileOutputStream> outstream;
        arrow::Result<std::shared_ptr<arrow::io::FileOutputStream>> st = 
                arrow::io::FileOutputStream::Open(csv_filename, false);
        if (st.ok()) {
                outstream = std::move(st).ValueOrDie();
        }else{
                std::cerr << st.status() << std::endl;
        }

        auto write_options = arrow::csv::WriteOptions::Defaults();
        std::cout << "Writing CSV file" << std::endl;
        if (arrow::csv::WriteCSV(*my_table, write_options, outstream.get()).ok()) 
        {
                std::cout << "Writing CSV file completed" << std::endl;
        }else{
                std::cout << "Writing CSV file failed" << std::endl;
        }

        return 0;
}
