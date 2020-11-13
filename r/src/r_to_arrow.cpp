// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "./arrow_types.h"
#include "./arrow_vctrs.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/converter.h>

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using internal::Converter;
using internal::DictionaryConverter;
using internal::ListConverter;
using internal::PrimitiveConverter;
using internal::StructConverter;

using internal::MakeChunker;
using internal::MakeConverter;

namespace r {

struct RConversionOptions {
  RConversionOptions() = default;

  std::shared_ptr<arrow::DataType> type;
  bool strict;
  int64_t size;
};

struct RObject {};

using RConverter = Converter<RObject*, RConversionOptions>;

// TODO: this needs various versions as what python does:
// class PyPrimitiveConverter<T, enable_if_null<T>>
//
// class PyPrimitiveConverter<
// T, enable_if_t<is_boolean_type<T>::value || is_number_type<T>::value ||
//   is_decimal_type<T>::value || is_date_type<T>::value ||
//   is_time_type<T>::value>> : public PrimitiveConverter<T, PyConverter> {
//
// class PyPrimitiveConverter<
//   T, enable_if_t<is_boolean_type<T>::value || is_number_type<T>::value ||
//   is_decimal_type<T>::value || is_date_type<T>::value ||
//   is_time_type<T>::value>> : public PrimitiveConverter<T, PyConverter> {
//
// class PyPrimitiveConverter<T, enable_if_binary<T>>
//   : public PrimitiveConverter<T, PyConverter> {
//
// class PyPrimitiveConverter<T, enable_if_t<std::is_same<T, FixedSizeBinaryType>::value>>
//   :  public PrimitiveConverter<T, PyConverter> {
//
// class PyPrimitiveConverter<T, enable_if_string_like<T>>
//   : public PrimitiveConverter<T, PyConverter> {

template <typename T, typename Enable = void>
class RPrimitiveConverter : public PrimitiveConverter<T, RConverter> {
 public:
  Status Append(RObject* value) {
    Rprintf("T = %s\n", arrow::util::nameof<T>().c_str());
    return Status::OK();
  }
};

template <typename T>
class RListConverter;

// TODO: replace by various versions. The python code has 2 versions:
//
// template <typename U>
// class PyDictionaryConverter<U, enable_if_has_c_type<U>>
//    : public DictionaryConverter<U, PyConverter> {
//
// template <typename U>
// class PyDictionaryConverter<U, enable_if_has_string_view<U>>
//    : public DictionaryConverter<U, PyConverter> {
//
template <typename U, typename Enable = void>
class RDictionaryConverter : public DictionaryConverter<U, RConverter> {
 public:
  Status Append(RObject* value) { return Status::OK(); }
};

class RStructConverter;

template <typename T, typename Enable = void>
struct RConverterTrait;

template <typename T>
struct RConverterTrait<
    T, enable_if_t<!is_nested_type<T>::value && !is_interval_type<T>::value &&
                   !is_extension_type<T>::value>> {
  using type = RPrimitiveConverter<T>;
};

template <typename T>
struct RConverterTrait<T, enable_if_list_like<T>> {
  using type = RListConverter<T>;
};

template <typename T>
class RListConverter : public ListConverter<T, RConverter, RConverterTrait> {
 public:
  Status Append(RObject* value) { return Status::OK(); }
};

template <>
struct RConverterTrait<StructType> {
  using type = RStructConverter;
};

class RStructConverter : public StructConverter<RConverter, RConverterTrait> {
public:
  Status Append(RObject* value) override {
    return Status::OK();
  }

protected:
Status Init(MemoryPool* pool) override {
  RETURN_NOT_OK((StructConverter<RConverter, RConverterTrait>::Init(pool)));
  return Status::OK();
}
};

template <>
struct RConverterTrait<DictionaryType> {
  template <typename T>
  using dictionary_type = RDictionaryConverter<T>;
};

std::shared_ptr<arrow::Array> vec_to_arrow(SEXP x, SEXP s_type) {
  RConversionOptions options;
  options.strict = !Rf_isNull(s_type);

  std::shared_ptr<arrow::DataType> type;
  if (options.strict) {
    options.type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(s_type);
  } else {
    options.type = arrow::r::InferArrowType(x);
  }

  options.size = vctrs::short_vec_size(x);

  auto converter = ValueOrStop(MakeConverter<RConverter, RConverterTrait>(
      options.type, options, gc_memory_pool()));

  StopIfNotOk(converter->Reserve(options.size));
  // TODO: iterate and call Append on each value
  // StopIfNotOk(converter->Append(x));
  return ValueOrStop(converter->ToArray());
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
SEXP vec_to_arrow(SEXP x, SEXP s_type) {
  if (Rf_inherits(x, "Array")) return x;
  return cpp11::to_r6(arrow::r::vec_to_arrow(x, s_type));
}

#endif
