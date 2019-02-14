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

#include <cassert>

namespace red_arrow {

namespace {

using Status = arrow::Status;

class ArrayConverter : public arrow::ArrayVisitor {
 public:
  ArrayConverter(bool convert_decimal) : convert_decimal_(convert_decimal) {}

 protected:
  Status NotImplemented(char const* message) {
    return Status::NotImplemented(message);
  }

  inline VALUE ConvertValue(const arrow::Array& array, const int64_t i) {
#define ARRAY_CONVERT_VALUE_INLINE(TYPE_CLASS) \
  case arrow::TYPE_CLASS::type_id: \
    { \
      using ArrayType = typename arrow::TypeTraits<arrow::TYPE_CLASS>::ArrayType; \
      return ConvertValue(arrow::internal::checked_cast<const ArrayType&>(array), i); \
    }

    switch (array.type_id()) {
      ARRAY_CONVERT_VALUE_INLINE(NullType);
      ARRAY_CONVERT_VALUE_INLINE(BooleanType);
      ARRAY_CONVERT_VALUE_INLINE(Int8Type);
      ARRAY_CONVERT_VALUE_INLINE(UInt8Type);
      ARRAY_CONVERT_VALUE_INLINE(Int16Type);
      ARRAY_CONVERT_VALUE_INLINE(UInt16Type);
      ARRAY_CONVERT_VALUE_INLINE(Int32Type);
      ARRAY_CONVERT_VALUE_INLINE(UInt32Type);
      ARRAY_CONVERT_VALUE_INLINE(Int64Type);
      ARRAY_CONVERT_VALUE_INLINE(UInt64Type);
      ARRAY_CONVERT_VALUE_INLINE(HalfFloatType); // TODO: test
      ARRAY_CONVERT_VALUE_INLINE(FloatType);
      ARRAY_CONVERT_VALUE_INLINE(DoubleType);
      ARRAY_CONVERT_VALUE_INLINE(Decimal128Type);
      ARRAY_CONVERT_VALUE_INLINE(FixedSizeBinaryType); // TODO: test
      ARRAY_CONVERT_VALUE_INLINE(StringType);
      ARRAY_CONVERT_VALUE_INLINE(BinaryType);
      ARRAY_CONVERT_VALUE_INLINE(Date32Type);
      ARRAY_CONVERT_VALUE_INLINE(Date64Type);
      ARRAY_CONVERT_VALUE_INLINE(TimestampType);
      ARRAY_CONVERT_VALUE_INLINE(Time32Type); // TODO: test
      ARRAY_CONVERT_VALUE_INLINE(Time64Type); // TODO: test
      ARRAY_CONVERT_VALUE_INLINE(ListType);
      ARRAY_CONVERT_VALUE_INLINE(StructType);
      ARRAY_CONVERT_VALUE_INLINE(DictionaryType);
      ARRAY_CONVERT_VALUE_INLINE(UnionType);
      default:
        break;
    }
#undef ARRAY_CONVERT_VALUE_INLINE

    // TODO: NotImplemented("Unsupported array in ConvertValue");
    throw rb::error(rb_eNotImpError, "Unsupported array in ConvertValue");
  }

  inline VALUE ConvertValue(const arrow::NullArray& array, const int64_t i) {
    return Qnil;
  }

  inline VALUE ConvertValue(const arrow::BooleanArray& array, const int64_t i) {
    bool value = array.Value(i);
    return value ? Qtrue : Qfalse;
  }

  template <typename ArrayType>
  inline VALUE ConvertSignedIntegerValue(const ArrayType& array, const int64_t i) {
    return rb::protect([&]{ return LL2NUM(array.Value(i)); });
  }

#define CONVERT_SIGNED_INTEGER(TYPE) \
  VALUE ConvertValue(const TYPE& array, const int64_t i) { \
    return ConvertSignedIntegerValue<TYPE>(array, i); \
  }

  CONVERT_SIGNED_INTEGER(arrow::Int8Array)
  CONVERT_SIGNED_INTEGER(arrow::Int16Array)
  CONVERT_SIGNED_INTEGER(arrow::Int32Array)
  CONVERT_SIGNED_INTEGER(arrow::Int64Array)

#undef CONVERT_SIGNED_INTEGER

  template <typename ArrayType>
  inline VALUE ConvertUnsignedIntegerValue(const ArrayType& array, const int64_t i) {
    return rb::protect([&]{ return ULL2NUM(array.Value(i)); });
  }

#define CONVERT_UNSIGNED_INTEGER(TYPE) \
  VALUE ConvertValue(const TYPE& array, const int64_t i) { \
    return ConvertUnsignedIntegerValue<TYPE>(array, i); \
  }

  CONVERT_UNSIGNED_INTEGER(arrow::UInt8Array)
  CONVERT_UNSIGNED_INTEGER(arrow::UInt16Array)
  CONVERT_UNSIGNED_INTEGER(arrow::UInt32Array)
  CONVERT_UNSIGNED_INTEGER(arrow::UInt64Array)

#undef CONVERT_UNSIGNED_INTEGER

  template <typename ArrayType>
  inline VALUE ConvertFloatValue(const ArrayType& array, const int64_t i) {
    return rb::protect([&]{ return DBL2NUM(array.Value(i)); });
  }

#define CONVERT_FLOAT(TYPE) \
  VALUE ConvertValue(const TYPE& array, const int64_t i) { \
    return ConvertFloatValue<TYPE>(array, i); \
  }

  CONVERT_FLOAT(arrow::FloatArray)
  CONVERT_FLOAT(arrow::DoubleArray)

#undef CONVERT_FLOAT

  inline VALUE ConvertValue(const arrow::HalfFloatArray& array, const int64_t i) {
    // FIXME: should convert to Float
    return rb::protect([&]{
      return LONG2FIX(static_cast<long>(array.Value(i)));
    });
  }

  inline VALUE ConvertDecimal128(const arrow::Decimal128Array& array, const int64_t i) {
    return rb::protect([&]{
      auto raw_value = std::make_shared<arrow::Decimal128>(array.GetValue(i));
      auto gobj_value = garrow_decimal128_new_raw(&raw_value);
      return GOBJ2RVAL(gobj_value);
    });
  }

  inline VALUE ConvertDecimal128ToBigDecimal(const arrow::Decimal128Array& array, const int64_t i) {
    auto decimal_str = array.FormatValue(i);
    return rb::protect([&]{
      return rb_funcall(rb_cObject, id_BigDecimal, 1, rb_str_new_cstr(decimal_str.c_str()));
    });
  }

  inline VALUE ConvertValue(const arrow::Decimal128Array& array, const int64_t i) {
    if (convert_decimal_) {
      return ConvertDecimal128ToBigDecimal(array, i);
    } else {
      return ConvertDecimal128(array, i);
    }
  }

  inline VALUE ConvertValue(const arrow::FixedSizeBinaryArray& array, const int64_t i, VALUE buffer) {
    const auto byte_width = array.byte_width();
    return rb::protect([&]{ return rb_str_substr(buffer, i*byte_width, byte_width); });
  }

  inline VALUE ConvertValue(const arrow::FixedSizeBinaryArray& array, const int64_t i) {
    const auto byte_width = array.byte_width();
    const auto offset = i * byte_width;
    const auto ptr = reinterpret_cast<const char*>(array.raw_values());
    return rb::protect([&]{ return rb_str_new(ptr + offset, byte_width); });
  }

  inline VALUE ConvertValue(const arrow::BinaryArray& array, const int64_t i) {
    int32_t length;
    const uint8_t* ptr = array.GetValue(i, &length);
    return rb::protect([&]{
      return rb_str_new(reinterpret_cast<const char*>(ptr), length);
    });
  }

  inline VALUE ConvertValue(const arrow::StringArray& array, const int64_t i) {
    int32_t length;
    const uint8_t* ptr = array.GetValue(i, &length);
    // TODO: encoding support
    return rb::protect([&]{
      return rb_utf8_str_new(reinterpret_cast<const char*>(ptr), length);
    });
  }

  inline VALUE ConvertValue(const arrow::Date32Array& array, const int64_t i) {
    const static int32_t JD_UNIX_EPOCH = 2440588; // UNIX epoch in Julian date
    return rb::protect([&]{
      auto raw_value = array.Value(i);
      auto days_in_julian = raw_value + JD_UNIX_EPOCH;
      return rb_funcall(rb_cDate, id_jd, 1, LONG2NUM(days_in_julian));
    });
  }

  inline VALUE ConvertValue(const arrow::Date64Array& array, const int64_t i) {
    return rb::protect([&]{
      auto raw_value = array.Value(i);
      VALUE msec = LL2NUM(raw_value);
      VALUE sec = rb_rational_new(msec, INT2NUM(1000));
      VALUE time_value = rb_time_num_new(sec, Qnil);
      return rb_funcall(time_value, id_to_datetime, 0, 0);
    });
  }

  inline VALUE ConvertValue(const arrow::Time32Array& array, const int64_t i) {
    // TODO: must test this function
    // TODO: unit treatment
    return rb::protect([&]{
      auto raw_value = array.Value(i);
      return LONG2NUM(raw_value);
    });
  }

  inline VALUE ConvertValue(const arrow::Time64Array& array, const int64_t i) {
    // TODO: must test this function
    // TODO: unit treatment
    return rb::protect([&]{
      auto raw_value = array.Value(i);
      return LL2NUM(raw_value);
    });
  }

  inline VALUE ConvertValue(const arrow::TimestampArray& array, const int64_t i, VALUE scale) {
    return rb::protect([&]{
      auto raw_value = array.Value(i);
      VALUE sec = rb_rational_new(LL2NUM(raw_value), scale);
      return rb_time_num_new(sec, Qnil);
    });
  }

  inline VALUE ConvertValue(const arrow::TimestampArray& array, const int64_t i) {
    const auto& type = arrow::internal::checked_cast<const arrow::TimestampType&>(*array.type());
    VALUE scale = time_unit_to_scale(type.unit());
    if (NIL_P(scale)) {
      throw rb::error(rb_eArgError, "Invalid TimeUnit");
    }
    return ConvertValue(array, i, scale);
  }

  inline VALUE ConvertValue(const arrow::ListArray& array, const int64_t i) {
    return rb::protect([&]{
      auto list = array.values()->Slice(array.value_offset(i), array.value_length(i));
      auto gobj = garrow_array_new_raw(&list);
      return GOBJ2RVAL(gobj);
    });
  }

  inline VALUE ConvertValue(const arrow::StructArray& array, const int64_t i) {
    const auto* struct_type = array.struct_type();
    const auto nf = struct_type->num_children();
    return rb::protect([&] {
      VALUE record = rb_hash_new();
      for (int k = 0; k < nf; ++k) {
        auto field_type = struct_type->child(k);
        auto& field_name = field_type->name();
        VALUE key = rb_str_new_cstr(field_name.c_str());
        const auto& field_array = *array.field(k);
        VALUE val = ConvertValue(field_array, i);
        rb_hash_aset(record, key, val);
      }
      return record;
    });
  }

  inline VALUE ConvertSparseUnionArray(const arrow::UnionArray& array, const int64_t i) {
    assert(array.mode() == arrow::UnionMode::SPARSE);
    const auto* type_ids = array.raw_type_ids();
    const auto& child_array = *array.UnsafeChild(type_ids[i]);
    return ConvertValue(child_array, i);
  }

  inline VALUE ConvertDenseUnionArray(const arrow::UnionArray& array, const int64_t i) {
    assert(array.mode() == arrow::UnionMode::DENSE);
    const auto* type_ids = array.raw_type_ids();
    const auto* value_offsets = array.raw_value_offsets();
    const auto offset = value_offsets[i];
    const auto& child_array = *array.UnsafeChild(type_ids[i]);
    return ConvertValue(child_array, offset);
  }

  inline VALUE ConvertValue(const arrow::UnionArray& array, const int64_t i) {
    switch (array.mode()) {
      case arrow::UnionMode::SPARSE:
        return ConvertSparseUnionArray(array, i);

      case arrow::UnionMode::DENSE:
        return ConvertDenseUnionArray(array, i);
    }

    // TODO: return Status::Invalid("Invalid union mode");
    throw rb::error(rb_eRuntimeError, "Invalid union mode");
  }

  inline VALUE ConvertValue(const arrow::DictionaryArray& array, const int64_t i) {
    auto indices = array.indices();
    return ConvertValue(*indices, i);
  }

  Status Visit(const arrow::NullArray& array) override {
    const int64_t nr = array.length();
    for (int64_t i = 0; i < nr; ++i) {
      // FIXME: Explicit storing Qnil can be cancelled
      RETURN_NOT_OK(VisitValue(i, Qnil));
    }
    return Status::OK();
  }

  template <typename TYPE>
  inline Status VisitConvertValue(const TYPE& array) {
    const int64_t nr = array.length();
    if (array.null_count() > 0) {
      for (int64_t i = 0; i < nr; ++i) {
        if (array.IsNull(i)) {
          RETURN_NOT_OK(VisitValue(i, Qnil));
        } else {
          RETURN_NOT_OK(VisitValue(i, ConvertValue(array, i)));
        }
      }
    } else {
      for (int64_t i = 0; i < nr; ++i) {
        RETURN_NOT_OK(VisitValue(i, ConvertValue(array, i)));
      }
    }
    return Status::OK();
  }

#define VISIT_CONVERT_VALUE(TYPE) \
  Status Visit(const arrow::TYPE ## Array& array) override { \
    return VisitConvertValue<arrow::TYPE ## Array>(array); \
  }

  VISIT_CONVERT_VALUE(Boolean)
  VISIT_CONVERT_VALUE(Int8)
  VISIT_CONVERT_VALUE(Int16)
  VISIT_CONVERT_VALUE(Int32)
  VISIT_CONVERT_VALUE(Int64)
  VISIT_CONVERT_VALUE(UInt8)
  VISIT_CONVERT_VALUE(UInt16)
  VISIT_CONVERT_VALUE(UInt32)
  VISIT_CONVERT_VALUE(UInt64)
  VISIT_CONVERT_VALUE(Float)
  VISIT_CONVERT_VALUE(Double)
  VISIT_CONVERT_VALUE(HalfFloat)
  VISIT_CONVERT_VALUE(String)
  VISIT_CONVERT_VALUE(Date32)
  VISIT_CONVERT_VALUE(Date64)
  VISIT_CONVERT_VALUE(Time32)
  VISIT_CONVERT_VALUE(Time64)
  VISIT_CONVERT_VALUE(List)
  VISIT_CONVERT_VALUE(Union)

#undef VISIT_CONVERT_VALUE

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

  Status Visit(const arrow::TimestampArray& array) override {
    const auto& type = arrow::internal::checked_cast<const arrow::TimestampType&>(*array.type());
    VALUE scale = time_unit_to_scale(type.unit());
    if (NIL_P(scale)) {
      return Status::Invalid("Invalid TimeUnit");
    }
    return VisitColumn(array, [&](const int64_t i) {
      return ConvertValue(array, i, scale);
    });
  }

  Status Visit(const arrow::FixedSizeBinaryArray& array) override {
    const auto byte_width = array.byte_width();
    VALUE buffer = rb::protect([&]{
      long length = byte_width * array.length();
      return rb_str_new(reinterpret_cast<const char*>(array.raw_values()), length);
    });
    return VisitColumn(array, [&](const int64_t i) {
      return ConvertValue(array, i, buffer);
    });
  }

  Status Visit(const arrow::Decimal128Array& array) override {
    if (convert_decimal_) {
      return VisitColumn(array, [&](const int64_t i) {
        return ConvertDecimal128ToBigDecimal(array, i);
      });
    } else {
      return VisitColumn(array, [&](const int64_t i) {
        return ConvertDecimal128(array, i);
      });
    }
  }

  Status Visit(const arrow::DictionaryArray& array) override {
    auto indices = array.indices();
    return indices->Accept(this);
  }

  virtual Status VisitValue(const int64_t row_index, VALUE value) = 0;

  // Convert Decimal to BigDecimal.
  const bool convert_decimal_;
};

class StructArrayConverter : public ArrayConverter {
 public:
  using VisitValueFunc = std::function<Status(const int64_t, VALUE, VALUE)>;

  explicit StructArrayConverter(bool convert_decimal, VisitValueFunc visit_value_func)
      : ArrayConverter(convert_decimal), visit_value_func_(visit_value_func) {}

  Status Convert(const arrow::StructArray& array) {
    const auto* struct_type = array.struct_type();
    const auto nf = struct_type->num_children();

    RETURN_NOT_OK(collect_field_names(struct_type));

    for (int k = 0; k < nf; ++k) {
      field_index_ = k;

      auto field_array = array.field(k);
      RETURN_NOT_OK(field_array->Accept(this));
    }

    return Status::OK();
  }

 protected:
  using ArrayConverter::Visit;

  Status Visit(const arrow::StructArray& array) override {
    // FIXME
    return NotImplemented("Struct in Struct is not supported");
  }

  Status VisitValue(const int64_t row_index, VALUE value) override {
    VALUE field_name = field_names_[field_index_];
    return visit_value_func_(row_index, field_name, value);
  }

 private:
  Status collect_field_names(const arrow::StructType* type) {
    const auto nf = type->num_children();
    field_names_.reserve(nf);
    (void)rb::protect([&]{
      for (int i = 0; i < nf; ++i) {
        auto field = type->child(i);
        auto& name = field->name();
        field_names_.push_back(rb_str_new_cstr(name.c_str()));
      }
      return Qnil;
    });
    return Status::OK();
  }

  VisitValueFunc visit_value_func_;
  std::vector<VALUE> field_names_;
  int field_index_;
};

class RawRecordsBuilder : public ArrayConverter {
 public:
  RawRecordsBuilder(VALUE records, int num_columns, bool convert_decimal=false)
      : ArrayConverter(convert_decimal), records_(records), num_columns_(num_columns) {}

  Status Add(const arrow::RecordBatch& record_batch) {
    const int num_columns = record_batch.num_columns();
    for (int i = 0; i < num_columns; ++i) {
      auto array = record_batch.column(i);
      column_index_ = i;
      RETURN_NOT_OK(array->Accept(this));
    }
    return Status::OK();
  }

 protected:
  using ArrayConverter::Visit;

  Status Visit(const arrow::StructArray& array) override {
    StructArrayConverter converter(
        convert_decimal_,
        [&](const int64_t row_index, VALUE key, VALUE val) {
          VALUE hash = GetValue(row_index);
          (void)rb::protect([&]{
            if (NIL_P(hash)) {
              hash = rb_hash_new();
              VisitValue(row_index, hash);
            }
            return rb_hash_aset(hash, key, val);
          });
          return Status::OK();
        });
    return converter.Convert(array);
  }

 private:
  inline VALUE GetValue(const int64_t row_index) {
    return rb::protect([&] {
      VALUE record = rb_ary_entry(records_, row_index);
      if (NIL_P(record)) return Qnil;
      return rb_ary_entry(record, column_index_);
    });
  }

  Status VisitValue(const int64_t row_index, VALUE val) override {
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
