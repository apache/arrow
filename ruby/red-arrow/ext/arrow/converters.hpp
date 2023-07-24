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
  class ListArrayValueConverter;
  class StructArrayValueConverter;
  class MapArrayValueConverter;
  class UnionArrayValueConverter;
  class DictionaryArrayValueConverter;

  class ArrayValueConverter {
  public:
    ArrayValueConverter()
      : decimal_buffer_(),
        list_array_value_converter_(nullptr),
        struct_array_value_converter_(nullptr),
        map_array_value_converter_(nullptr),
        union_array_value_converter_(nullptr),
        dictionary_array_value_converter_(nullptr) {
    }

    inline void set_sub_value_converters(ListArrayValueConverter* list_array_value_converter,
                                         StructArrayValueConverter* struct_array_value_converter,
                                         MapArrayValueConverter* map_array_value_converter,
                                         UnionArrayValueConverter* union_array_value_converter,
                                         DictionaryArrayValueConverter* dictionary_array_value_converter) {
      list_array_value_converter_ = list_array_value_converter;
      struct_array_value_converter_ = struct_array_value_converter;
      map_array_value_converter_ = map_array_value_converter;
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

    inline VALUE convert(const arrow::HalfFloatArray& array,
                         const int64_t i) {
      const auto value = array.Value(i);
      // | sign (1 bit) | exponent (5 bit) | fraction (10 bit) |
      constexpr auto exponent_n_bits = 5;
      static const auto exponent_mask =
        static_cast<uint32_t>(std::pow(2.0, exponent_n_bits) - 1);
      constexpr auto exponent_bias = 15;
      constexpr auto fraction_n_bits = 10;
      static const auto fraction_mask =
        static_cast<uint32_t>(std::pow(2.0, fraction_n_bits)) - 1;
      static const auto fraction_denominator = std::pow(2.0, fraction_n_bits);
      const auto sign = value >> (exponent_n_bits + fraction_n_bits);
      const auto exponent = (value >> fraction_n_bits) & exponent_mask;
      const auto fraction = value & fraction_mask;
      if (exponent == exponent_mask) {
        if (sign == 0) {
          return DBL2NUM(HUGE_VAL);
        } else {
          return DBL2NUM(-HUGE_VAL);
        }
      } else {
        const auto implicit_fraction = (exponent == 0) ? 0 : 1;
        return DBL2NUM(((sign == 0) ? 1 : -1) *
                       std::pow(2.0, exponent - exponent_bias) *
                       (implicit_fraction + fraction / fraction_denominator));
      }
    }

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
      const auto type =
        arrow::internal::checked_cast<const arrow::Time32Type*>(array.type().get());
      const auto value = array.Value(i);
      return rb_funcall(red_arrow::cArrowTime,
                        id_new,
                        2,
                        time_unit_to_enum(type->unit()),
                        INT2NUM(value));
    }

    inline VALUE convert(const arrow::Time64Array& array,
                         const int64_t i) {
      const auto type =
        arrow::internal::checked_cast<const arrow::Time64Type*>(array.type().get());
      const auto value = array.Value(i);
      return rb_funcall(red_arrow::cArrowTime,
                        id_new,
                        2,
                        time_unit_to_enum(type->unit()),
                        LL2NUM(value));
    }

    inline VALUE convert(const arrow::TimestampArray& array,
                         const int64_t i) {
      const auto type =
        arrow::internal::checked_cast<const arrow::TimestampType*>(array.type().get());
      auto scale = time_unit_to_scale(type->unit());
      auto value = array.Value(i);
      auto sec = rb_rational_new(LL2NUM(value), scale);
      return rb_time_num_new(sec, Qnil);
    }

    // TODO
    // inline VALUE convert(const arrow::IntervalArray& array,
    //                      const int64_t i) {
    // };

    inline VALUE convert(const arrow::MonthIntervalArray& array,
                         const int64_t i) {
      return INT2NUM(array.Value(i));
    }

    inline VALUE convert(const arrow::DayTimeIntervalArray& array,
                         const int64_t i) {
      auto value = rb_hash_new();
      auto arrow_value = array.Value(i);
      rb_hash_aset(value,
                   red_arrow::symbols::day,
                   INT2NUM(arrow_value.days));
      rb_hash_aset(value,
                   red_arrow::symbols::millisecond,
                   INT2NUM(arrow_value.milliseconds));
      return value;
    }

    inline VALUE convert(const arrow::MonthDayNanoIntervalArray& array,
                         const int64_t i) {
      auto value = rb_hash_new();
      auto arrow_value = array.Value(i);
      rb_hash_aset(value,
                   red_arrow::symbols::month,
                   INT2NUM(arrow_value.months));
      rb_hash_aset(value,
                   red_arrow::symbols::day,
                   INT2NUM(arrow_value.days));
      rb_hash_aset(value,
                   red_arrow::symbols::nanosecond,
                   INT2NUM(arrow_value.nanoseconds));
      return value;
    }

    VALUE convert(const arrow::ListArray& array,
                  const int64_t i);

    VALUE convert(const arrow::StructArray& array,
                  const int64_t i);

    VALUE convert(const arrow::MapArray& array,
                  const int64_t i);

    VALUE convert(const arrow::UnionArray& array,
                  const int64_t i);

    VALUE convert(const arrow::DictionaryArray& array,
                  const int64_t i);

    inline VALUE convert(const arrow::Decimal128Array& array,
                         const int64_t i) {
      return convert_decimal(std::move(array.FormatValue(i)));
    }

    inline VALUE convert(const arrow::Decimal256Array& array,
                         const int64_t i) {
      return convert_decimal(std::move(array.FormatValue(i)));
    }

  private:
    inline VALUE convert_decimal(std::string&& value) {
      decimal_buffer_ = value;
      return rb_funcall(rb_cObject,
                        id_BigDecimal,
                        1,
                        rb_enc_str_new(decimal_buffer_.data(),
                                       decimal_buffer_.length(),
                                       rb_ascii8bit_encoding()));
    }

    std::string decimal_buffer_;
    ListArrayValueConverter* list_array_value_converter_;
    StructArrayValueConverter* struct_array_value_converter_;
    MapArrayValueConverter* map_array_value_converter_;
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
    arrow::Status Visit(const arrow::TYPE ## Array& array) override {   \
      return visit_value(array);                                        \
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
    inline VALUE convert_value(const ArrayType& array,
                               const int64_t i) {
      return array_value_converter_->convert(array, i);
    }

    template <typename ArrayType>
    arrow::Status visit_value(const ArrayType& array) {
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
      return arrow::Status::OK();
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
      const auto n = struct_type->num_fields();
      for (int i = 0; i < n; ++i) {
        const auto field_type = struct_type->field(i).get();
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
    arrow::Status Visit(const arrow::TYPE ## Array& array) override {   \
      fill_field(array);                                                \
      return arrow::Status::OK();                                       \
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

  class MapArrayValueConverter : public arrow::ArrayVisitor {
  public:
    explicit MapArrayValueConverter(ArrayValueConverter* converter)
      : array_value_converter_(converter),
        offset_(0),
        length_(0),
        values_(Qnil) {}

    VALUE convert(const arrow::MapArray& array,
                  const int64_t index) {
      auto key_array = array.keys().get();
      auto item_array = array.items().get();
      auto offset_keep = offset_;
      auto length_keep = length_;
      auto values_keep = values_;
      offset_ = array.value_offset(index);
      length_ = array.value_length(index);
      auto keys = rb_ary_new_capa(length_);
      values_ = keys;
      check_status(key_array->Accept(this),
                   "[raw-records][map-array][keys]");
      auto items = rb_ary_new_capa(length_);
      values_ = items;
      check_status(item_array->Accept(this),
                   "[raw-records][map-array][items]");
      auto map = rb_hash_new();
      auto n = RARRAY_LEN(keys);
      auto raw_keys = RARRAY_CONST_PTR(keys);
      auto raw_items = RARRAY_CONST_PTR(items);
      for (long i = 0; i < n; ++i) {
        rb_hash_aset(map, raw_keys[i], raw_items[i]);
      }
      offset_ = offset_keep;
      length_ = length_keep;
      values_ = values_keep;
      return map;
    }

#define VISIT(TYPE)                                                     \
    arrow::Status Visit(const arrow::TYPE ## Array& array) override {   \
      return visit_value(array);                                        \
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
    inline VALUE convert_value(const ArrayType& array,
                               const int64_t i) {
      return array_value_converter_->convert(array, i);
    }

    template <typename ArrayType>
    arrow::Status visit_value(const ArrayType& array) {
      if (array.null_count() > 0) {
        for (int64_t i = 0; i < length_; ++i) {
          auto value = Qnil;
          if (!array.IsNull(i + offset_)) {
            value = convert_value(array, i + offset_);
          }
          rb_ary_push(values_, value);
        }
      } else {
        for (int64_t i = 0; i < length_; ++i) {
          rb_ary_push(values_, convert_value(array, i + offset_));
        }
      }
      return arrow::Status::OK();
    }

    ArrayValueConverter* array_value_converter_;
    int32_t offset_;
    int32_t length_;
    VALUE values_;
  };

  class UnionArrayValueConverter : public arrow::ArrayVisitor {
  public:
    explicit UnionArrayValueConverter(ArrayValueConverter* converter)
      : array_value_converter_(converter),
        index_(0),
        result_(Qnil) {}

    VALUE convert(const arrow::UnionArray& array,
                  const int64_t index) {
      const auto index_keep = index_;
      const auto result_keep = result_;
      index_ = index;
      switch (array.mode()) {
      case arrow::UnionMode::SPARSE:
        convert_sparse(static_cast<const arrow::SparseUnionArray&>(array));
        break;
      case arrow::UnionMode::DENSE:
        convert_dense(static_cast<const arrow::DenseUnionArray&>(array));
        break;
      default:
        rb_raise(rb_eArgError, "Invalid union mode");
        break;
      }
      auto result_return = result_;
      index_ = index_keep;
      result_ = result_keep;
      return result_return;
    }

#define VISIT(TYPE)                                                     \
    arrow::Status Visit(const arrow::TYPE ## Array& array) override {   \
      convert_value(array);                                             \
      return arrow::Status::OK();                                       \
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
    inline void convert_value(const ArrayType& array) {
      if (array.IsNull(index_)) {
        result_ = RUBY_Qnil;
      } else {
        result_ = array_value_converter_->convert(array, index_);
      }
    }

    int8_t compute_child_id(const arrow::UnionArray& array,
                            arrow::UnionType* type,
                            const char* tag) {
      const auto type_code = array.raw_type_codes()[index_];
      if (type_code >= 0 && type_code <= arrow::UnionType::kMaxTypeCode) {
        const auto child_id = type->child_ids()[type_code];
        if (child_id >= 0) {
          return child_id;
        }
      }
      check_status(arrow::Status::Invalid("Unknown type ID: ", type_code),
                   tag);
      return 0;
    }

    void convert_sparse(const arrow::SparseUnionArray& array) {
      const auto type =
        std::static_pointer_cast<arrow::UnionType>(array.type()).get();
      const auto tag = "[raw-records][union-sparse-array]";
      const auto child_id = compute_child_id(array, type, tag);
      const auto field_array = array.field(child_id).get();
      check_status(field_array->Accept(this), tag);
    }

    void convert_dense(const arrow::DenseUnionArray& array) {
      const auto type =
        std::static_pointer_cast<arrow::UnionType>(array.type()).get();
      const auto tag = "[raw-records][union-dense-array]";
      const auto child_id = compute_child_id(array, type, tag);
      const auto field_array = array.field(child_id);
      const auto index_keep = index_;
      index_ = array.value_offset(index_);
      check_status(field_array->Accept(this), tag);
      index_ = index_keep;
    }

    ArrayValueConverter* array_value_converter_;
    int64_t index_;
    VALUE result_;
  };

  class DictionaryArrayValueConverter : public arrow::ArrayVisitor {
  public:
    explicit DictionaryArrayValueConverter(ArrayValueConverter* converter)
      : array_value_converter_(converter),
        value_index_(0),
        result_(Qnil) {
    }

    VALUE convert(const arrow::DictionaryArray& array,
                  const int64_t index) {
      value_index_ = array.GetValueIndex(index);
      auto dictionary = array.dictionary().get();
      check_status(dictionary->Accept(this),
                   "[raw-records][dictionary-array]");
      return result_;
    }

#define VISIT(TYPE)                                                     \
    arrow::Status Visit(const arrow::TYPE ## Array& array) override {   \
      result_ = convert_value(array, value_index_);                     \
      return arrow::Status::OK();                                       \
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
    inline VALUE convert_value(const ArrayType& array,
                               const int64_t i) {
      return array_value_converter_->convert(array, i);
    }

    ArrayValueConverter* array_value_converter_;
    int64_t value_index_;
    VALUE result_;
  };

  class Converter {
  public:
    explicit Converter()
      : array_value_converter_(),
        list_array_value_converter_(&array_value_converter_),
        struct_array_value_converter_(&array_value_converter_),
        map_array_value_converter_(&array_value_converter_),
        union_array_value_converter_(&array_value_converter_),
        dictionary_array_value_converter_(&array_value_converter_) {
      array_value_converter_.
        set_sub_value_converters(&list_array_value_converter_,
                                 &struct_array_value_converter_,
                                 &map_array_value_converter_,
                                 &union_array_value_converter_,
                                 &dictionary_array_value_converter_);
    }

    template <typename ArrayType>
    inline VALUE convert_value(const ArrayType& array,
                               const int64_t i) {
      return array_value_converter_.convert(array, i);
    }

    ArrayValueConverter array_value_converter_;
    ListArrayValueConverter list_array_value_converter_;
    StructArrayValueConverter struct_array_value_converter_;
    MapArrayValueConverter map_array_value_converter_;
    UnionArrayValueConverter union_array_value_converter_;
    DictionaryArrayValueConverter dictionary_array_value_converter_;
  };
}
