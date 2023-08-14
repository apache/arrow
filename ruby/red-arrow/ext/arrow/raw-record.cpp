/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "converters.hpp"

namespace red_arrow {
  namespace {
    class RawRecordProducer : private Converter, public arrow::ArrayVisitor {
    public:
      explicit RawRecordProducer(int n_columns)
        : Converter(),
          record_(Qnil),
          n_columns_(n_columns) {
      }

      void produce(const arrow::Table& table) {
        rb::protect([&] {
          const auto n_rows = table.num_rows();
          for (int64_t i = 0; i < n_rows; ++i) {
            row_offset_ = i;
            record_ = rb_ary_new_capa(n_columns_);

            for (int i = 0; i < n_columns_; ++i) {
              const auto& chunked_array = table.column(i).get();
              column_index_ = i;

              for (const auto array : chunked_array->chunks()) {
                check_status(array->Accept(this),
                            "[table][each-raw-record]");
              }
            }
            rb_yield(record_);
          }
          return Qnil;
        });
      }

#define VISIT(TYPE)                                                     \
      arrow::Status Visit(const arrow::TYPE ## Array& array) override { \
        convert(array);                                                 \
        return arrow::Status::OK();                                     \
      }

      VISIT(Null)
      VISIT(Boolean)
      VISIT(Int8)
      VISIT(Int16)
      VISIT(Int32)
      VISIT(Int64)
      VISIT(UInt8)
      VISIT(UInt16)
      VISIT(UInt32)
      VISIT(UInt64)
      VISIT(HalfFloat)
      VISIT(Float)
      VISIT(Double)
      VISIT(Binary)
      VISIT(String)
      VISIT(FixedSizeBinary)
      VISIT(Date32)
      VISIT(Date64)
      VISIT(Time32)
      VISIT(Time64)
      VISIT(Timestamp)
      VISIT(MonthInterval)
      VISIT(DayTimeInterval)
      VISIT(MonthDayNanoInterval)
      VISIT(List)
      VISIT(Struct)
      VISIT(Map)
      VISIT(SparseUnion)
      VISIT(DenseUnion)
      VISIT(Dictionary)
      VISIT(Decimal128)
      VISIT(Decimal256)
      // TODO
      // VISIT(Extension)

#undef VISIT

    private:
      template <typename ArrayType>
      void convert(const ArrayType& array) {
        auto value = Qnil;
        if (!array.IsNull(row_offset_)) {
          value = convert_value(array, row_offset_);
        }
        rb_ary_store(record_, column_index_, value);
      }

      // Destination for converted record.
      VALUE record_;

      // The current column index.
      int column_index_;

      // The current row offset.
      int64_t row_offset_;

      // The number of columns.
      const int n_columns_;
    };
  }

  VALUE
  table_each_raw_record(VALUE rb_table) {
    auto garrow_table = GARROW_TABLE(RVAL2GOBJ(rb_table));
    auto table = garrow_table_get_raw(garrow_table).get();
    const auto n_columns = table->num_columns();

    try {
      RawRecordProducer producer(n_columns);
      producer.produce(*table);
    } catch (rb::State& state) {
      state.jump();
    }

    return Qnil;
  }
}
