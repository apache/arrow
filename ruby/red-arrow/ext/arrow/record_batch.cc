/*
 * Copyright 2018 Kenta Murata <mrkn@mrkn.jp>
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

#include "ruby.hpp"
#include "red_arrow.hpp"

#include <arrow-glib/decimal128.hpp>

namespace red_arrow {

namespace {

using Status = arrow::Status;

class RawRecordsBuilder : public arrow::ArrayVisitor {
 public:
  RawRecordsBuilder(VALUE records, int num_columns) : records_(records), num_columns_(num_columns) {}

  Status Add(const arrow::RecordBatch& record_batch) {
    const int num_columns = record_batch.num_columns();
    for (int i = 0; i < num_columns; ++i) {
      auto array = record_batch.column(i);
      column_index_ = i;
      array->Accept(this);
    }
    return Status::OK();
  }

 protected:
  Status NotImplemented(char const* message) {
    return Status::NotImplemented(message);
  }

  Status Visit(const arrow::NullArray& array) override {
    const int64_t nr = array.length();
    for (int64_t i = 0; i < nr; ++i) {
      // FIXME: Explicit storing Qnil can be cancelled
      RETURN_NOT_OK(VisitValue(i, Qnil));
    }
    return Status::OK();
  }

  template <typename ArrayType>
  Status VisitColumn(const ArrayType& array, std::function<VALUE(const int64_t)> fetch_value) {
    const int64_t nr = array.length();
    if (array.null_count() > 0) {
      for (int64_t i = 0; i < nr; ++i) {
        if (array.IsNull(i)) {
          RETURN_NOT_OK(VisitValue(i, Qnil));
        } else {
          RETURN_NOT_OK(VisitValue(i, fetch_value(i)));
        }
      }
    } else {
      for (int64_t i = 0; i < nr; ++i) {
        RETURN_NOT_OK(VisitValue(i, fetch_value(i)));
      }
    }
    return Status::OK();
  }

  Status Visit(const arrow::BooleanArray& array) override {
    return VisitColumn(array, [&](const int64_t i) {
      bool value = array.Value(i);
      return value ? Qtrue : Qfalse;
    });
  }

  template <typename ArrayType>
  Status VisitSignedInteger(const ArrayType& array) {
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{ return LL2NUM(array.Value(i)); });
      return value;
    });
  }

#define VISIT_SIGNED_INTEGER(TYPE) \
  Status Visit(const TYPE& array) override { return VisitSignedInteger<TYPE>(array); }

  VISIT_SIGNED_INTEGER(arrow::Int8Array)
  VISIT_SIGNED_INTEGER(arrow::Int16Array)
  VISIT_SIGNED_INTEGER(arrow::Int32Array)
  VISIT_SIGNED_INTEGER(arrow::Int64Array)

#undef VISIT_SIGNED_INTEGER

  template <typename ArrayType>
  Status VisitUnsignedInteger(const ArrayType& array) {
    return VisitColumn(array, [&](int i) {
      VALUE value = rb::protect([&]{ return ULL2NUM(array.Value(i)); });
      return value;
    });
  }

#define VISIT_UNSIGNED_INTEGER(TYPE) \
  Status Visit(const TYPE& array) override { return VisitUnsignedInteger<TYPE>(array); }

  VISIT_UNSIGNED_INTEGER(arrow::UInt8Array)
  VISIT_UNSIGNED_INTEGER(arrow::UInt16Array)
  VISIT_UNSIGNED_INTEGER(arrow::UInt32Array)
  VISIT_UNSIGNED_INTEGER(arrow::UInt64Array)

#undef VISIT_UNSIGNED_INTEGER

  Status Visit(const arrow::HalfFloatArray& array) override {
    // FIXME
    return NotImplemented("HalfFloatArray");
  }

  template <typename ArrayType>
  Status VisitFloat(const ArrayType& array) {
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{ return DBL2NUM(array.Value(i)); });
      return value;
    });
  }

#define VISIT_FLOAT(TYPE) \
  Status Visit(const TYPE& array) override { return VisitFloat<TYPE>(array); }

  VISIT_FLOAT(arrow::FloatArray)
  VISIT_FLOAT(arrow::DoubleArray)

#undef VISIT_FLOAT

  Status Visit(const arrow::Date32Array& array) override {
    // FIXME
    return NotImplemented("Date32Array");
  }

  Status Visit(const arrow::Date64Array& array) override {
    // FIXME
    return NotImplemented("Date64Array");
  }

  Status Visit(const arrow::TimestampArray& array) override {
    // FIXME
    return NotImplemented("TimestampArray");
  }

  Status Visit(const arrow::Time32Array& array) override {
    // FIXME
    return NotImplemented("Time32Array");
  }

  Status Visit(const arrow::Time64Array& array) override {
    // FIXME
    return NotImplemented("Time64Array");
  }

  Status Visit(const arrow::FixedSizeBinaryArray& array) override {
    const auto byte_width = array.byte_width();
    VALUE buffer = rb::protect([&]{
      long length = byte_width * array.length();
      return rb_str_new(reinterpret_cast<const char*>(array.raw_values()), length);
    });
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{
        return rb_str_substr(buffer, i*byte_width, byte_width);
      });
      return value;
    });
  }

  Status Visit(const arrow::Decimal128Array& array) override {
    // TODO: optionally conversion to BigDecimal
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{
        auto raw_value = std::make_shared<arrow::Decimal128>(array.GetValue(i));
        auto gobj_value = garrow_decimal128_new_raw(&raw_value);
        return GOBJ2RVAL(gobj_value);
      });
      return value;
    });
  }

  Status Visit(const arrow::BinaryArray& array) override {
    return VisitColumn(array, [&](const int64_t i) {
      int32_t length;
      const uint8_t* ptr = array.GetValue(i, &length);
      VALUE value = rb::protect([&]{
        return rb_str_new(reinterpret_cast<const char*>(ptr), length);
      });
      return value;
    });
  }

  Status Visit(const arrow::StringArray& array) override {
    return VisitColumn(array, [&](const int64_t i) {
      int32_t length;
      const uint8_t* ptr = array.GetValue(i, &length);
      // TODO: encoding support
      VALUE value = rb::protect([&]{
        return rb_str_new(reinterpret_cast<const char*>(ptr), length);
      });
      return value;
    });
  }

  Status Visit(const arrow::ListArray& array) override {
    // FIXME
    return NotImplemented("ListArray");
  }

  Status Visit(const arrow::StructArray& array) override {
    // FIXME
    return NotImplemented("StructArray");
  }

  Status Visit(const arrow::UnionArray& array) override {
    // FIXME
    return NotImplemented("UnionArray");
  }

  Status Visit(const arrow::DictionaryArray& array) override {
    // FIXME
    return NotImplemented("DictionaryArray");
  }

 private:
  inline Status VisitValue(const int64_t row_index, VALUE val) {
    (void)rb::protect([&] {
      VALUE record = rb_ary_entry(records_, row_index);
      if (NIL_P(record)) {
        record = rb_ary_new_capa(num_columns_);
        rb_ary_store(records_, row_index, record);
      }
      rb_ary_store(record, column_index_, val);
      return Qnil;
    });
    return Status::OK();
  }

  // Destination for converted records.
  VALUE records_;

  // The current column index.
  int column_index_;

  // The number of columns.
  const int num_columns_;
};

}  // namespace

VALUE
record_batch_raw_records(VALUE obj)
{
  try {
    const auto gobj_record_batch = GARROW_RECORD_BATCH(RVAL2GOBJ(obj));
    const auto record_batch = garrow_record_batch_get_raw(gobj_record_batch);
    const auto num_rows = record_batch->num_rows();
    const auto num_columns = record_batch->num_columns();
    const auto schema = record_batch->schema();

    VALUE records = rb::protect([&]{ return rb_ary_new2(num_rows); });
    RawRecordsBuilder builder(records, num_columns);
    builder.Add(*record_batch);
    return records;
  } catch (rb::error err) {
    rb_exc_raise(err.exception_object());
  }
}

}  // namespace red_arrow
