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

#include "red-arrow.hpp"

#include <ruby.hpp>
#include <ruby/encoding.h>

#include <arrow-glib/error.hpp>

#include <arrow/util/logging.h>

namespace red_arrow {
  namespace {
    using Status = arrow::Status;

    void check_status(const Status&& status, const char* context) {
      GError* error = nullptr;
      if (!garrow_error_check(&error, status, context)) {
        RG_RAISE_ERROR(error);
      }
    }

    class ListArrayValueConverter;
    class StructArrayValueConverter;
    class UnionArrayValueConverter;
    class DictionaryArrayValueConverter;

    class ArrayValueConverter {
    public:
      ArrayValueConverter()
        : decimal_buffer_(),
          list_array_value_converter_(nullptr),
          struct_array_value_converter_(nullptr),
          union_array_value_converter_(nullptr),
          dictionary_array_value_converter_(nullptr) {
      }

      void set_sub_value_converters(ListArrayValueConverter* list_array_value_converter,
                                    StructArrayValueConverter* struct_array_value_converter,
                                    UnionArrayValueConverter* union_array_value_converter,
                                    DictionaryArrayValueConverter* dictionary_array_value_converter) {
        list_array_value_converter_ = list_array_value_converter;
        struct_array_value_converter_ = struct_array_value_converter;
        union_array_value_converter_ = union_array_value_converter;
        dictionary_array_value_converter_ = dictionary_array_value_converter;
      }

      inline VALUE convert(const arrow::NullArray& array,
                           const int64_t i) {
        return Qnil;
      }

      inline VALUE convert(const arrow::BooleanArray& array,
                           const int64_t i) {
        return array.Value(i) ? Qtrue : Qfalse;
      }

      inline VALUE convert(const arrow::Int8Array& array,
                           const int64_t i) {
        return INT2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::Int16Array& array,
                           const int64_t i) {
        return INT2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::Int32Array& array,
                           const int64_t i) {
        return INT2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::Int64Array& array,
                           const int64_t i) {
        return LL2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::UInt8Array& array,
                           const int64_t i) {
        return UINT2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::UInt16Array& array,
                           const int64_t i) {
        return UINT2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::UInt32Array& array,
                           const int64_t i) {
        return UINT2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::UInt64Array& array,
                           const int64_t i) {
        return ULL2NUM(array.Value(i));
      }

      // TODO
      // inline VALUE convert(const arrow::HalfFloatArray& array,
      //                      const int64_t i) {
      // }

      inline VALUE convert(const arrow::FloatArray& array,
                           const int64_t i) {
        return DBL2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::DoubleArray& array,
                           const int64_t i) {
        return DBL2NUM(array.Value(i));
      }

      inline VALUE convert(const arrow::BinaryArray& array,
                           const int64_t i) {
        int32_t length;
        const auto value = array.GetValue(i, &length);
        // TODO: encoding support
        return rb_enc_str_new(reinterpret_cast<const char*>(value),
                              length,
                              rb_ascii8bit_encoding());
      }

      inline VALUE convert(const arrow::StringArray& array,
                           const int64_t i) {
        int32_t length;
        const auto value = array.GetValue(i, &length);
        return rb_utf8_str_new(reinterpret_cast<const char*>(value),
                               length);
      }

      inline VALUE convert(const arrow::FixedSizeBinaryArray& array,
                           const int64_t i) {
        return rb_enc_str_new(reinterpret_cast<const char*>(array.Value(i)),
                              array.byte_width(),
                              rb_ascii8bit_encoding());
      }

      constexpr static int32_t JULIAN_DATE_UNIX_EPOCH = 2440588;
      inline VALUE convert(const arrow::Date32Array& array,
                           const int64_t i) {
        const auto value = array.Value(i);
        const auto days_in_julian = value + JULIAN_DATE_UNIX_EPOCH;
        return rb_funcall(cDate, id_jd, 1, LONG2NUM(days_in_julian));
      }

      inline VALUE convert(const arrow::Date64Array& array,
                           const int64_t i) {
        const auto value = array.Value(i);
        auto msec = LL2NUM(value);
        auto sec = rb_rational_new(msec, INT2NUM(1000));
        auto time_value = rb_time_num_new(sec, Qnil);
        return rb_funcall(time_value, id_to_datetime, 0, 0);
      }

      inline VALUE convert(const arrow::Time32Array& array,
                           const int64_t i) {
        // TODO: unit treatment
        const auto value = array.Value(i);
        return INT2NUM(value);
      }

      inline VALUE convert(const arrow::Time64Array& array,
                           const int64_t i) {
        // TODO: unit treatment
        const auto value = array.Value(i);
        return LL2NUM(value);
      }

      inline VALUE convert(const arrow::TimestampArray& array,
                           const int64_t i) {
        const auto type =
          arrow::internal::checked_cast<const arrow::TimestampType*>(array.type().get());
        auto scale = time_unit_to_scale(type->unit());
        if (NIL_P(scale)) {
          rb_raise(rb_eArgError, "Invalid TimeUnit");
        }
        auto value = array.Value(i);
        auto sec = rb_rational_new(LL2NUM(value), scale);
        return rb_time_num_new(sec, Qnil);
      }

      // TODO
      // inline VALUE convert(const arrow::IntervalArray& array,
      //                      const int64_t i) {
      // };

      VALUE convert(const arrow::ListArray& array,
                    const int64_t i);

      VALUE convert(const arrow::StructArray& array,
                    const int64_t i);

      VALUE convert(const arrow::UnionArray& array,
                    const int64_t i);

      VALUE convert(const arrow::DictionaryArray& array,
                    const int64_t i);

      inline VALUE convert(const arrow::Decimal128Array& array,
                           const int64_t i) {
        decimal_buffer_ = array.FormatValue(i);
        return rb_funcall(rb_cObject,
                          id_BigDecimal,
                          1,
                          rb_enc_str_new(decimal_buffer_.data(),
                                         decimal_buffer_.length(),
                                         rb_ascii8bit_encoding()));
      }

    private:
      std::string decimal_buffer_;
      ListArrayValueConverter* list_array_value_converter_;
      StructArrayValueConverter* struct_array_value_converter_;
      UnionArrayValueConverter* union_array_value_converter_;
      DictionaryArrayValueConverter* dictionary_array_value_converter_;
    };

    class ListArrayValueConverter : public arrow::ArrayVisitor {
    public:
      explicit ListArrayValueConverter(ArrayValueConverter* converter)
        : array_value_converter_(converter),
          offset_(0),
          length_(0),
          result_(Qnil) {}

      VALUE convert(const arrow::ListArray& array, const int64_t index) {
        auto values = array.values().get();
        auto offset_keep = offset_;
        auto length_keep = length_;
        offset_ = array.value_offset(index);
        length_ = array.value_length(index);
        auto result_keep = result_;
        result_ = rb_ary_new_capa(length_);
        check_status(values->Accept(this),
                     "[raw-records][list-array]");
        offset_ = offset_keep;
        length_ = length_keep;
        auto result_return = result_;
        result_ = result_keep;
        return result_return;
      }

#define VISIT(TYPE)                                                     \
      Status Visit(const arrow::TYPE ## Array& array) override {        \
        return visit_value(array);                                      \
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
      inline VALUE convert_value(const ArrayType& array,
                                 const int64_t i) {
        return array_value_converter_->convert(array, i);
      }

      template <typename ArrayType>
      Status visit_value(const ArrayType& array) {
        if (array.null_count() > 0) {
          for (int64_t i = 0; i < length_; ++i) {
            auto value = Qnil;
            if (!array.IsNull(i + offset_)) {
              value = convert_value(array, i + offset_);
            }
            rb_ary_push(result_, value);
          }
        } else {
          for (int64_t i = 0; i < length_; ++i) {
            rb_ary_push(result_, convert_value(array, i + offset_));
          }
        }
        return Status::OK();
      }

      ArrayValueConverter* array_value_converter_;
      int32_t offset_;
      int32_t length_;
      VALUE result_;
    };

    class StructArrayValueConverter : public arrow::ArrayVisitor {
    public:
      explicit StructArrayValueConverter(ArrayValueConverter* converter)
        : array_value_converter_(converter),
          key_(Qnil),
          index_(0),
          result_(Qnil) {}

      VALUE convert(const arrow::StructArray& array,
                    const int64_t index) {
        auto index_keep = index_;
        auto result_keep = result_;
        index_ = index;
        result_ = rb_hash_new();
        const auto struct_type = array.struct_type();
        const auto n = struct_type->num_children();
        for (int i = 0; i < n; ++i) {
          const auto field_type = struct_type->child(i).get();
          const auto& field_name = field_type->name();
          auto key_keep = key_;
          key_ = rb_utf8_str_new(field_name.data(), field_name.length());
          const auto field_array = array.field(i).get();
          check_status(field_array->Accept(this),
                       "[raw-records][struct-array]");
          key_ = key_keep;
        }
        auto result_return = result_;
        result_ = result_keep;
        index_ = index_keep;
        return result_return;
      }

#define VISIT(TYPE)                                                     \
      Status Visit(const arrow::TYPE ## Array& array) override {        \
        fill_field(array);                                              \
        return Status::OK();                                            \
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
      inline VALUE convert_value(const ArrayType& array,
                                 const int64_t i) {
        return array_value_converter_->convert(array, i);
      }

      template <typename ArrayType>
      void fill_field(const ArrayType& array) {
        if (array.IsNull(index_)) {
          rb_hash_aset(result_, key_, Qnil);
        } else {
          rb_hash_aset(result_, key_, convert_value(array, index_));
        }
      }

      ArrayValueConverter* array_value_converter_;
      VALUE key_;
      int64_t index_;
      VALUE result_;
    };

    class UnionArrayValueConverter : public arrow::ArrayVisitor {
    public:
      explicit UnionArrayValueConverter(ArrayValueConverter* converter)
        : array_value_converter_(converter),
          index_(0),
          result_(Qnil) {}

      VALUE convert(const arrow::UnionArray& array,
                    const int64_t index) {
        index_ = index;
        switch (array.mode()) {
        case arrow::UnionMode::SPARSE:
          convert_sparse(array);
          break;
        case arrow::UnionMode::DENSE:
          convert_dense(array);
          break;
        default:
          rb_raise(rb_eArgError, "Invalid union mode");
          break;
        }
        return result_;
      }

#define VISIT(TYPE)                                                     \
      Status Visit(const arrow::TYPE ## Array& array) override {        \
        result_ = convert_value(array, index_);                         \
        return Status::OK();                                            \
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
      inline VALUE convert_value(const ArrayType& array,
                                 const int64_t i) {
        return array_value_converter_->convert(array, i);
      }

      void convert_sparse(const arrow::UnionArray& array) {
        const auto type_ids = array.raw_type_ids();
        const auto child_array = array.UnsafeChild(type_ids[index_]);
        check_status(child_array->Accept(this),
                     "[raw-records][union-sparse-array]");
      }

      void convert_dense(const arrow::UnionArray& array) {
        const auto type_id = array.raw_type_ids()[index_];
        const auto offset = array.value_offset(index_);
        const auto child_array = array.UnsafeChild(type_id);
        const auto i = index_;
        index_ = offset;
        check_status(child_array->Accept(this),
                     "[raw-records][union-dense-array]");
        index_ = i;
      }

      ArrayValueConverter* array_value_converter_;
      int64_t index_;
      VALUE result_;
    };

    class DictionaryArrayValueConverter : public arrow::ArrayVisitor {
    public:
      explicit DictionaryArrayValueConverter(ArrayValueConverter* converter)
        : array_value_converter_(converter),
          index_(0),
          result_(Qnil) {
      }

      VALUE convert(const arrow::DictionaryArray& array,
                    const int64_t index) {
        index_ = index;
        auto indices = array.indices().get();
        check_status(indices->Accept(this),
                     "[raw-records][dictionary-array]");
        return result_;
      }

      // TODO: Convert to real value.
#define VISIT(TYPE)                                                     \
      Status Visit(const arrow::TYPE ## Array& array) override {        \
        result_ = convert_value(array, index_);                         \
        return Status::OK();                                            \
      }

      VISIT(Int8)
      VISIT(Int16)
      VISIT(Int32)
      VISIT(Int64)

#undef VISIT

    private:
      template <typename ArrayType>
      inline VALUE convert_value(const ArrayType& array,
                                 const int64_t i) {
        return array_value_converter_->convert(array, i);
      }

      ArrayValueConverter* array_value_converter_;
      int64_t index_;
      VALUE result_;
    };

    VALUE ArrayValueConverter::convert(const arrow::ListArray& array,
                                       const int64_t i) {
      return list_array_value_converter_->convert(array, i);
    }

    VALUE ArrayValueConverter::convert(const arrow::StructArray& array,
                                       const int64_t i) {
      return struct_array_value_converter_->convert(array, i);
    }

    VALUE ArrayValueConverter::convert(const arrow::UnionArray& array,
                                       const int64_t i) {
      return union_array_value_converter_->convert(array, i);
    }

    VALUE ArrayValueConverter::convert(const arrow::DictionaryArray& array,
                                       const int64_t i) {
      return dictionary_array_value_converter_->convert(array, i);
    }

    class RawRecordsBuilder : public arrow::ArrayVisitor {
    public:
      explicit RawRecordsBuilder(VALUE records, int n_columns)
        : array_value_converter_(),
          list_array_value_converter_(&array_value_converter_),
          struct_array_value_converter_(&array_value_converter_),
          union_array_value_converter_(&array_value_converter_),
          dictionary_array_value_converter_(&array_value_converter_),
          records_(records),
          n_columns_(n_columns) {
        array_value_converter_.
          set_sub_value_converters(&list_array_value_converter_,
                                   &struct_array_value_converter_,
                                   &union_array_value_converter_,
                                   &dictionary_array_value_converter_);
      }

      void build(const arrow::RecordBatch& record_batch) {
        rb::protect([&] {
          const auto n_rows = record_batch.num_rows();
          for (int64_t i = 0; i < n_rows; ++i) {
            auto record = rb_ary_new_capa(n_columns_);
            rb_ary_push(records_, record);
          }
          for (int i = 0; i < n_columns_; ++i) {
            const auto array = record_batch.column(i).get();
            column_index_ = i;
            check_status(array->Accept(this),
                         "[raw-records]");
          }
          return Qnil;
        });
      }

#define VISIT(TYPE)                                                     \
      Status Visit(const arrow::TYPE ## Array& array) override {        \
        convert(array);                                                 \
        return Status::OK();                                            \
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
      inline VALUE convert_value(const ArrayType& array,
                                 const int64_t i) {
        return array_value_converter_.convert(array, i);
      }

      template <typename ArrayType>
      void convert(const ArrayType& array) {
        const auto n = array.length();
        if (array.null_count() > 0) {
          for (int64_t i = 0; i < n; ++i) {
            auto value = Qnil;
            if (!array.IsNull(i)) {
              value = convert_value(array, i);
            }
            auto record = rb_ary_entry(records_, i);
            rb_ary_store(record, column_index_, value);
          }
        } else {
          for (int64_t i = 0; i < n; ++i) {
            auto record = rb_ary_entry(records_, i);
            rb_ary_store(record, column_index_, convert_value(array, i));
          }
        }
      }

      ArrayValueConverter array_value_converter_;
      ListArrayValueConverter list_array_value_converter_;
      StructArrayValueConverter struct_array_value_converter_;
      UnionArrayValueConverter union_array_value_converter_;
      DictionaryArrayValueConverter dictionary_array_value_converter_;

      // Destination for converted records.
      VALUE records_;

      // The current column index.
      int column_index_;

      // The number of columns.
      const int n_columns_;
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
}
