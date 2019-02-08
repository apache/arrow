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
  RawRecordsBuilder(VALUE records, int num_columns, bool convert_decimal=false) : records_(records), num_columns_(num_columns), convert_decimal_(convert_decimal) {}

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
    ID id_jd;
    VALUE cDate = rb::protect([&]{
      id_jd = rb_intern("jd");
      rb_require("date");
      return rb_const_get(rb_cObject, rb_intern("Date"));
    });
    return VisitColumn(array, [&](const int64_t i) {
      const static int32_t JD_UNIX_EPOCH = 2440588; // UNIX epoch in Julian date
      VALUE value = rb::protect([&]{
        auto raw_value = array.Value(i);
        auto days_in_julian = raw_value + JD_UNIX_EPOCH;
        return rb_funcall(cDate, id_jd, 1, LONG2NUM(days_in_julian));
      });
      return value;
    });
  }

  Status Visit(const arrow::Date64Array& array) override {
    ID id_to_datetime;
    rb::protect([&]{
      id_to_datetime = rb_intern("to_datetime");
      rb_require("date");
      return Qnil;
    });
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{
        auto raw_value = array.Value(i);
        VALUE msec = LL2NUM(raw_value);
        VALUE sec = rb_rational_new(msec, INT2NUM(1000));
        VALUE time_value = rb_time_num_new(sec, Qnil);
        return rb_funcall(time_value, id_to_datetime, 0, 0);
      });
      return value;
    });
    return Status::OK();
  }

  Status Visit(const arrow::TimestampArray& array) override {
    const auto& type = arrow::internal::checked_cast<arrow::TimestampType&>(*array.type());
    VALUE scale = Qnil;
    switch (type.unit()) {
      case arrow::TimeUnit::SECOND:
        scale = INT2FIX(1);
        break;
      case arrow::TimeUnit::MILLI:
        scale = INT2FIX(1000);
        break;
      case arrow::TimeUnit::MICRO:
        scale = INT2FIX(1000000);
        break;
      case arrow::TimeUnit::NANO:
        // Note that INT2FIX works for 1e+9 because:
        //     FIXNUM_MAX >= (1<<30) - 1 > 1e+9
        scale = INT2FIX(1000000000);
        break;
      default:
        return Status::Invalid("Invalid TimeUnit");
    }

    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{
        auto raw_value = array.Value(i);
        VALUE sec = rb_rational_new(LL2NUM(raw_value), scale);
        return rb_time_num_new(sec, Qnil);
      });
      return value;
    });
  }

  Status Visit(const arrow::Time32Array& array) override {
    // TODO: must test this function
    // TODO: unit treatment
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{
        auto raw_value = array.Value(i);
        return LONG2NUM(raw_value);
      });
      return value;
    });
  }

  Status Visit(const arrow::Time64Array& array) override {
    // TODO: must test this function
    // TODO: unit treatment
    return VisitColumn(array, [&](const int64_t i) {
      VALUE value = rb::protect([&]{
        auto raw_value = array.Value(i);
        return LL2NUM(raw_value);
      });
      return value;
    });
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
    if (convert_decimal_) {
      ID id_BigDecimal = rb_intern("BigDecimal");
      return VisitColumn(array, [&](const int64_t i) {
        auto decimal_str = array.FormatValue(i);
        VALUE value = rb::protect([&]{
          return rb_funcall(rb_cObject, id_BigDecimal, 1, rb_str_new_cstr(decimal_str.c_str()));
        });
        return value;
      });
    } else {
      return VisitColumn(array, [&](const int64_t i) {
        VALUE value = rb::protect([&]{
          auto raw_value = std::make_shared<arrow::Decimal128>(array.GetValue(i));
          auto gobj_value = garrow_decimal128_new_raw(&raw_value);
          return GOBJ2RVAL(gobj_value);
        });
        return value;
      });
    }
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

  // Convert Decimal to BigDecimal.
  const bool convert_decimal_;
};

}  // namespace

VALUE
record_batch_raw_records(int argc, VALUE* argv, VALUE obj)
{
  VALUE kwargs;
  bool convert_decimal = false;

  rb_scan_args(argc, argv, ":", &kwargs);
  if (!NIL_P(kwargs)) {
    enum { k_convert_decimal, num_kwargs };
    static ID kwarg_keys[num_kwargs];
    VALUE kwarg_vals[num_kwargs];
    kwarg_keys[k_convert_decimal] = rb_intern("convert_decimal");

    rb_get_kwargs(kwargs, kwarg_keys, 0, num_kwargs, kwarg_vals);

    if (kwarg_vals[k_convert_decimal] != Qundef && !NIL_P(kwarg_vals[k_convert_decimal])) {
      convert_decimal = RTEST(kwarg_vals[k_convert_decimal]);
    }
  }

  if (convert_decimal) {
    rb_require("bigdecimal");
  }

  try {
    const auto gobj_record_batch = GARROW_RECORD_BATCH(RVAL2GOBJ(obj));
    const auto record_batch = garrow_record_batch_get_raw(gobj_record_batch);
    const auto num_rows = record_batch->num_rows();
    const auto num_columns = record_batch->num_columns();
    const auto schema = record_batch->schema();

    VALUE records = rb::protect([&]{ return rb_ary_new2(num_rows); });
    RawRecordsBuilder builder(records, num_columns, convert_decimal);
    builder.Add(*record_batch);
    return records;
  } catch (rb::error err) {
    err.raise();
  }

  return Qnil; // unreachable
}

}  // namespace red_arrow
