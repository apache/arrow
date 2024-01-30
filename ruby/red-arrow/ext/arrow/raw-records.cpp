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
    class RawRecordsBuilder : private Converter, public arrow::ArrayVisitor {
    public:
      explicit RawRecordsBuilder(VALUE records, int n_columns)
        : Converter(),
          records_(records),
          n_columns_(n_columns) {
      }

      void build(const arrow::RecordBatch& record_batch) {
        rb::protect([&] {
          const auto n_rows = record_batch.num_rows();
          for (int64_t i = 0; i < n_rows; ++i) {
            auto record = rb_ary_new_capa(n_columns_);
            rb_ary_push(records_, record);
          }
          row_offset_ = 0;
          for (int i = 0; i < n_columns_; ++i) {
            const auto array = record_batch.column(i).get();
            column_index_ = i;
            check_status(array->Accept(this),
                         "[record-batch][raw-records]");
          }
          return Qnil;
        });
      }

      void build(const arrow::Table& table) {
        rb::protect([&] {
          const auto n_rows = table.num_rows();
          for (int64_t i = 0; i < n_rows; ++i) {
            auto record = rb_ary_new_capa(n_columns_);
            rb_ary_push(records_, record);
          }
          for (int i = 0; i < n_columns_; ++i) {
            const auto& chunked_array = table.column(i).get();
            column_index_ = i;
            row_offset_ = 0;
            for (const auto array : chunked_array->chunks()) {
              check_status(array->Accept(this),
                           "[table][raw-records]");
              row_offset_ += array->length();
            }
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
        const auto n = array.length();
        if (array.null_count() > 0) {
          for (int64_t i = 0, ii = row_offset_; i < n; ++i, ++ii) {
            auto value = Qnil;
            if (!array.IsNull(i)) {
              value = convert_value(array, i);
            }
            auto record = rb_ary_entry(records_, ii);
            rb_ary_store(record, column_index_, value);
          }
        } else {
          for (int64_t i = 0, ii = row_offset_; i < n; ++i, ++ii) {
            auto record = rb_ary_entry(records_, ii);
            rb_ary_store(record, column_index_, convert_value(array, i));
          }
        }
      }

      // Destination for converted records.
      VALUE records_;

      // The current column index.
      int column_index_;

      // The current row offset.
      int64_t row_offset_;

      // The number of columns.
      const int n_columns_;
    };

    class RawRecordsProducer : private Converter, public arrow::ArrayVisitor {
    public:
      explicit RawRecordsProducer()
        : Converter(),
          record_(Qnil),
          column_index_(0),
          row_offset_(0) {
      }

      void produce(const arrow::RecordBatch& record_batch) {
        rb::protect([&] {
          const auto n_columns = record_batch.num_columns();
          const auto n_rows = record_batch.num_rows();
          for (int64_t i = 0; i < n_rows; ++i) {
            record_ = rb_ary_new_capa(n_columns);
            row_offset_ = i;
            for (int i = 0; i < n_columns; ++i) {
              const auto array = record_batch.column(i).get();
              column_index_ = i;
              check_status(array->Accept(this),
                           "[record-batch][each-raw-record]");
            }
            rb_yield(record_);
          }
          return Qnil;
        });
      }

      void produce(const arrow::Table& table) {
        rb::protect([&] {
          const auto n_columns = table.num_columns();
          const auto n_rows = table.num_rows();
          std::vector<int> chunk_indexes(n_columns);
          std::vector<int64_t> row_offsets(n_columns);
          for (int64_t i_row = 0; i_row < n_rows; ++i_row) {
            record_ = rb_ary_new_capa(n_columns);
            for (int i_column = 0; i_column < n_columns; ++i_column) {
              column_index_ = i_column;
              const auto chunked_array = table.column(i_column).get();
              auto& chunk_index = chunk_indexes[i_column];
              auto& row_offset = row_offsets[i_column];
              auto array = chunked_array->chunk(chunk_index).get();
              while (array->length() == row_offset) {
                ++chunk_index;
                row_offset = 0;
                array = chunked_array->chunk(chunk_index).get();
              }
              row_offset_ = row_offset;
              check_status(array->Accept(this),
                           "[table][each-raw-record]");
              ++row_offset;
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
    };
  }

  VALUE
  record_batch_raw_records(VALUE rb_record_batch) {
    auto garrow_record_batch = GARROW_RECORD_BATCH(RVAL2GOBJ(rb_record_batch));
    auto record_batch = garrow_record_batch_get_raw(garrow_record_batch).get();
    const auto n_rows = record_batch->num_rows();
    const auto n_columns = record_batch->num_columns();
    auto records = rb_ary_new_capa(n_rows);

    try {
      RawRecordsBuilder builder(records, n_columns);
      builder.build(*record_batch);
    } catch (rb::State& state) {
      state.jump();
    }

    return records;
  }

  VALUE
  table_raw_records(VALUE rb_table) {
    auto garrow_table = GARROW_TABLE(RVAL2GOBJ(rb_table));
    auto table = garrow_table_get_raw(garrow_table).get();
    const auto n_rows = table->num_rows();
    const auto n_columns = table->num_columns();
    auto records = rb_ary_new_capa(n_rows);

    try {
      RawRecordsBuilder builder(records, n_columns);
      builder.build(*table);
    } catch (rb::State& state) {
      state.jump();
    }

    return records;
  }

  VALUE
  record_batch_each_raw_record(VALUE rb_record_batch) {
    auto garrow_record_batch = GARROW_RECORD_BATCH(RVAL2GOBJ(rb_record_batch));
    auto record_batch = garrow_record_batch_get_raw(garrow_record_batch).get();
    RETURN_SIZED_ENUMERATOR(rb_record_batch, 0, nullptr, record_batch->num_rows());

    try {
      RawRecordsProducer producer;
      producer.produce(*record_batch);
    } catch (rb::State& state) {
      state.jump();
    }

    return Qnil;
  }

  VALUE
  table_each_raw_record(VALUE rb_table) {
    auto garrow_table = GARROW_TABLE(RVAL2GOBJ(rb_table));
    auto table = garrow_table_get_raw(garrow_table).get();
    RETURN_SIZED_ENUMERATOR(rb_table, 0, nullptr, table->num_rows());

    try {
      RawRecordsProducer producer;
      producer.produce(*table);
    } catch (rb::State& state) {
      state.jump();
    }

    return Qnil;
  }
}
