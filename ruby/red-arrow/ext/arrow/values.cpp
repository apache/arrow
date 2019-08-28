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
    class ValuesBuilder : private Converter, public arrow::ArrayVisitor {
    public:
      explicit ValuesBuilder(VALUE values)
        : Converter(),
          values_(values),
          row_offset_(0) {
      }

      void build(const arrow::Array& array, VALUE rb_array) {
        rb::protect([&] {
          check_status(array.Accept(this),
                       "[array][values]");
          return Qnil;
        });
      }

      void build(const arrow::ChunkedArray& chunked_array,
                 VALUE rb_chunked_array) {
        rb::protect([&] {
          for (const auto& array : chunked_array.chunks()) {
            check_status(array->Accept(this),
                         "[chunked-array][values]");
            row_offset_ += array->length();
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
      // TODO
      // VISIT(HalfFloat)
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
      // TODO
      // VISIT(Interval)
      VISIT(List)
      VISIT(Struct)
      VISIT(Union)
      VISIT(Dictionary)
      VISIT(Decimal128)
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
            rb_ary_store(values_, ii, value);
          }
        } else {
          for (int64_t i = 0, ii = row_offset_; i < n; ++i, ++ii) {
            rb_ary_store(values_, ii, convert_value(array, i));
          }
        }
      }

      // Destination for converted values.
      VALUE values_;

      // The current row offset.
      int64_t row_offset_;
    };
  }

  VALUE
  array_values(VALUE rb_array) {
    auto garrow_array = GARROW_ARRAY(RVAL2GOBJ(rb_array));
    auto array = garrow_array_get_raw(garrow_array).get();
    const auto n_rows = array->length();
    auto values = rb_ary_new_capa(n_rows);

    try {
      ValuesBuilder builder(values);
      builder.build(*array, rb_array);
    } catch (rb::State& state) {
      state.jump();
    }

    return values;
  }

  VALUE
  chunked_array_values(VALUE rb_chunked_array) {
    auto garrow_chunked_array =
      GARROW_CHUNKED_ARRAY(RVAL2GOBJ(rb_chunked_array));
    auto chunked_array =
      garrow_chunked_array_get_raw(garrow_chunked_array).get();
    const auto n_rows = chunked_array->length();
    auto values = rb_ary_new_capa(n_rows);

    try {
      ValuesBuilder builder(values);
      builder.build(*chunked_array, rb_chunked_array);
    } catch (rb::State& state) {
      state.jump();
    }

    return values;
  }
}
