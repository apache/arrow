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

namespace internal {

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
  Status Visit(const TYPE& array) override { return VisitUnsignedInteger<TYPE>(array); }

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
      if (!NIL_P(val)) {
        rb_ary_store(record, column_index_, val);
      }
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

class ColumnConverter {
 public:
  ColumnConverter(VALUE rows, int column_index, int num_columns)
      : rows_(rows),
        column_index_(column_index),
        num_columns_(num_columns),
        row_index_(0) {}

  Status Convert(const std::shared_ptr<arrow::Array> arr) {
    using Type = arrow::Type;
    switch (arr->type_id()) {
      default:
        throw rb::error(rb_eRuntimeError,
                        std::string("Unsupported data type: ") + arr->type()->ToString());

#define CASE(type_id, type_name, TypeName) \
      case type_id: \
        { \
          auto type_name ## _array = std::static_pointer_cast<arrow :: TypeName ## Array>(arr); \
          return Visit(type_name ## _array); \
        }

      CASE(Type::BOOL,    bool,    Boolean);
      CASE(Type::UINT8,   uint8,   UInt8);
      CASE(Type::INT8,    int8,    Int8);
      CASE(Type::UINT16,  uint16,  UInt16);
      CASE(Type::INT16,   int16,   Int16);
      CASE(Type::UINT32,  uint32,  UInt32);
      CASE(Type::INT32,   int32,   Int32);
      CASE(Type::UINT64,  uint64,  UInt64);
      CASE(Type::INT64,   int64,   Int64);
      CASE(Type::FLOAT,   float,   Float);
      CASE(Type::DOUBLE,  double,  Double);
      CASE(Type::DECIMAL, decimal, Decimal128);
      CASE(Type::STRING,  str,     Binary);
      CASE(Type::BINARY,  bin,     Binary);

#undef CASE
    }
  }

  Status Visit(const std::shared_ptr<arrow::BinaryArray>& arr) {
    const int64_t nr = arr->length();
    if (arr->null_count() > 0) {
      for (int64_t i = 0; i < nr; ++i) {
        if (arr->IsNull(i)) {
          RETURN_NOT_OK(VisitNull());
        } else {
          int32_t length;
          const uint8_t* ptr = arr->GetValue(i, &length);
          RETURN_NOT_OK(VisitValue(ptr, length));
        }
      }
    } else {
      for (int64_t i = 0; i < nr; ++i) {
        int32_t length;
        const uint8_t* ptr = arr->GetValue(i, &length);
        RETURN_NOT_OK(VisitValue(ptr, length));
      }
    }
    return Status::OK();
  }

  template <typename ArrayType>
  Status Visit(const std::shared_ptr<ArrayType>& arr) {
    const int64_t nr = arr->length();
    if (arr->null_count() > 0) {
      for (int64_t i = 0; i < nr; ++i) {
        if (arr->IsNull(i)) {
          RETURN_NOT_OK(VisitNull());
        } else {
          RETURN_NOT_OK(VisitValue(arr->Value(i)));
        }
      }
    } else {
      for (int64_t i = 0; i < nr; ++i) {
        RETURN_NOT_OK(VisitValue(arr->Value(i)));
      }
    }
    return Status::OK();
  }

  Status VisitNull() {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, Qnil); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(bool val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, val ? Qtrue : Qfalse); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(int8_t val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, INT2NUM(val)); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(int16_t val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, INT2NUM(val)); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(int32_t val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, LONG2NUM(val)); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(int64_t val) {
    VALUE cols = next_row();
#if SIZEOF_LONG == 8
    rb_ary_store(cols, column_index_, LONG2NUM(val)); // TODO: protect
#else
    rb_ary_store(cols, column_index_, LL2NUM(val)); // TODO: protect
#endif
    return Status::OK();
  }

  Status VisitValue(uint8_t val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, UINT2NUM(val)); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(uint16_t val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, UINT2NUM(val)); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(uint32_t val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, ULONG2NUM(val)); // TODO: protect
    return Status::OK();
  }

  Status VisitValue(uint64_t val) {
    VALUE cols = next_row();
#if SIEOF_LONG == 8
    rb_ary_store(cols, column_index_, ULONG2NUM(val)); // TODO: protect
#else
    rb_ary_store(cols, column_index_, ULL2NUM(val)); // TODO: protect
#endif
    return Status::OK();
  }

  Status VisitValue(double val) {
    VALUE cols = next_row();
    rb_ary_store(cols, column_index_, DBL2NUM(val)); // TODO: protect
    return Status::OK();
  }

  // TODO: Support DECIMAL, too.
  Status VisitValue(const uint8_t* ptr, const int32_t length) {
    VALUE cols = next_row();
    VALUE val = rb_str_new(reinterpret_cast<const char*>(ptr), length); // TODO: protect
    rb_ary_store(cols, column_index_, val); // TODO: protect
    return Status::OK();
  }

 protected:
  virtual void expand_rows(int64_t row_index) {}

  VALUE next_row() {
    int64_t row_index = row_index_++;
    expand_rows(row_index);
    return RARRAY_AREF(rows_, row_index);
  }

  const VALUE rows_;
  const int column_index_;
  const int num_columns_;
  int64_t row_index_;
};

class FirstColumnConverter : public ColumnConverter {
 public:
  using ColumnConverter::ColumnConverter;

 protected:
  virtual void expand_rows(int64_t row_index) {
    if (RARRAY_LEN(rows_) <= row_index) {
      VALUE cols = rb_ary_new2(num_columns_); // TODO: protect
      rb_ary_push(rows_, cols); // TODO: protect
    }
  }
};


VALUE
record_batch_raw_records(VALUE obj) throw(rb::error) {
  const auto gobj_record_batch = GARROW_RECORD_BATCH(RVAL2GOBJ(obj));
  const auto record_batch = garrow_record_batch_get_raw(gobj_record_batch);
  const auto num_rows = record_batch->num_rows();
  const auto num_columns = record_batch->num_columns();
  const auto schema = record_batch->schema();

  VALUE rows = rb_ary_new2(num_rows); // TODO: protect

  /* first column */
  FirstColumnConverter converter0(rows, 0, num_columns);
  converter0.Convert(record_batch->column(0));

  if (num_columns > 1) {
    for (int j = 1; j < num_columns; ++j) {
      ColumnConverter converter(rows, j, num_columns);
      converter.Convert(record_batch->column(j));
    }
  }

  return rows;
}

}  // namespace internal

VALUE
record_batch_raw_records(VALUE obj)
{
  try {
    return internal::record_batch_raw_records(obj);
  } catch (rb::error err) {
    rb_exc_raise(err.exception_object());
  }
}


}  // namespace red_arrow
