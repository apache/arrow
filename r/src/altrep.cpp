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

#include <arrow/array.h>
#include <arrow/chunk_resolver.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/api.h>
#include <arrow/util/bitmap_reader.h>
#include <arrow/visit_data_inline.h>

#include <cpp11/altrep.hpp>
#include <cpp11/declarations.hpp>
#if defined(HAS_ALTREP)

#if R_VERSION < R_Version(3, 6, 0)

// workaround because R's <R_ext/Altrep.h> not so conveniently uses `class`
// as a variable name, and C++ is not happy about that
//
// SEXP R_new_altrep(R_altrep_class_t class, SEXP data1, SEXP data2);
//
#define class klass

// Because functions declared in <R_ext/Altrep.h> have C linkage
extern "C" {
#include <R_ext/Altrep.h>
}

// undo the workaround
#undef class

#else
#include <R_ext/Altrep.h>
#endif

#include "./r_task_group.h"

// defined in array_to_vector.cpp
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array);

namespace arrow {
namespace r {
namespace altrep {

namespace {
template <typename c_type>
R_xlen_t Standard_Get_region(SEXP data2, R_xlen_t i, R_xlen_t n, c_type* buf);

template <>
R_xlen_t Standard_Get_region<double>(SEXP data2, R_xlen_t i, R_xlen_t n, double* buf) {
  return REAL_GET_REGION(data2, i, n, buf);
}

template <>
R_xlen_t Standard_Get_region<int>(SEXP data2, R_xlen_t i, R_xlen_t n, int* buf) {
  return INTEGER_GET_REGION(data2, i, n, buf);
}

template <typename T>
void DeletePointer(std::shared_ptr<T>* ptr) {
  delete ptr;
}

template <typename T>
using Pointer = cpp11::external_pointer<std::shared_ptr<T>, DeletePointer<T>>;

class ArrowAltrepData {
 public:
  explicit ArrowAltrepData(const std::shared_ptr<ChunkedArray>& chunked_array)
      : chunked_array_(chunked_array), resolver_(chunked_array->chunks()) {}

  const std::shared_ptr<ChunkedArray>& chunked_array() { return chunked_array_; }

  arrow::internal::ChunkLocation locate(int64_t index) {
    return resolver_.Resolve(index);
  }

 private:
  std::shared_ptr<ChunkedArray> chunked_array_;
  arrow::internal::ChunkResolver resolver_;
};

// the ChunkedArray that is being wrapped by the altrep object
const std::shared_ptr<ChunkedArray>& GetChunkedArray(SEXP alt) {
  auto array_data =
      reinterpret_cast<ArrowAltrepData*>(R_ExternalPtrAddr(R_altrep_data1(alt)));
  return array_data->chunked_array();
}

// base class for all altrep vectors
//
// data1: the Array as an external pointer; becomes NULL when
//        materialization is needed.
// data2: starts as NULL, and becomes a standard R vector with the same
//        data if necessary: if materialization is needed, e.g. if we need
//        to access its data pointer, with DATAPTR().
template <typename Impl>
struct AltrepVectorBase {
  // store the Array as an external pointer in data1, mark as immutable
  static SEXP Make(const std::shared_ptr<ChunkedArray>& chunked_array) {
    SEXP alt = R_new_altrep(
        Impl::class_t,
        external_pointer<ArrowAltrepData>(new ArrowAltrepData(chunked_array)),
        R_NilValue);
    MARK_NOT_MUTABLE(alt);

    return alt;
  }

  // Is the vector materialized, i.e. does the data2 slot contain a
  // standard R vector with the same data as the array.
  static bool IsMaterialized(SEXP alt) { return !Rf_isNull(Impl::Representation(alt)); }

  static R_xlen_t Length(SEXP alt) {
    if (IsMaterialized(alt)) {
      return Rf_xlength(Representation(alt));
    } else {
      return GetChunkedArray(alt)->length();
    }
  }

  static int No_NA(SEXP alt) {
    if (IsMaterialized(alt)) {
      return false;
    }

    return GetChunkedArray(alt)->null_count() == 0;
  }

  static int Is_sorted(SEXP alt) { return UNKNOWN_SORTEDNESS; }

  // What gets printed on .Internal(inspect(<the altrep object>))
  static Rboolean Inspect(SEXP alt, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    SEXP data_class_sym = CAR(ATTRIB(ALTREP_CLASS(alt)));
    const char* class_name = CHAR(PRINTNAME(data_class_sym));

    if (IsMaterialized(alt)) {
      Rprintf("materialized %s len=%d\n", class_name, Rf_xlength(Representation(alt)));
    } else {
      const auto& chunked_array = GetChunkedArray(alt);
      Rprintf("%s<%p, %s, %d chunks, %d nulls> len=%d\n", class_name, chunked_array.get(),
              chunked_array->type()->ToString().c_str(), chunked_array->num_chunks(),
              chunked_array->null_count(), chunked_array->length());
    }

    return TRUE;
  }

  // Materialize and then duplicate data2
  static SEXP Duplicate(SEXP alt, Rboolean /* deep */) {
    return Rf_duplicate(Impl::Materialize(alt));
  }

  static SEXP Coerce(SEXP alt, int type) {
    return Rf_coerceVector(Impl::Materialize(alt), type);
  }

  static SEXP Serialized_state(SEXP alt) { return Impl::Materialize(alt); }

  static SEXP Unserialize(SEXP /* class_ */, SEXP state) { return state; }

  // default methods used when data2 is the representation
  // this is overridden when data2 needs to be richer (e.g. for factors)
  static SEXP Representation(SEXP alt) { return R_altrep_data2(alt); }

  static void SetRepresentation(SEXP alt, SEXP x) { R_set_altrep_data2(alt, x); }
};

// altrep R vector shadowing an primitive (int or double) Array.
//
// This tries as much as possible to directly use the data
// from the Array and minimize data copies.
template <int sexp_type>
struct AltrepVectorPrimitive : public AltrepVectorBase<AltrepVectorPrimitive<sexp_type>> {
  using Base = AltrepVectorBase<AltrepVectorPrimitive<sexp_type>>;

  // singleton altrep class description
  static R_altrep_class_t class_t;

  using c_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
  using Base::IsMaterialized;
  using Base::Representation;
  using Base::SetRepresentation;

  // Force materialization. After calling this, the data2 slot of the altrep
  // object contains a standard R vector with the same data, with
  // R sentinels where the Array has nulls. This method also releases the
  // reference to the original ChunkedArray.
  static SEXP Materialize(SEXP alt) {
    if (!IsMaterialized(alt)) {
      auto size = Base::Length(alt);

      // create a standard R vector
      SEXP copy = PROTECT(Rf_allocVector(sexp_type, size));

      // copy the data from the array, through Get_region
      Get_region(alt, 0, size, reinterpret_cast<c_type*>(DATAPTR(copy)));

      // store as data2, this is now considered materialized
      SetRepresentation(alt, copy);

      // we no longer need the original ChunkedArray (keeping it alive uses more
      // memory than is required, since our methods can now use the
      // materialized array)
      R_set_altrep_data1(alt, R_NilValue);

      UNPROTECT(1);
    }
    return Representation(alt);
  }

  // R calls this to get a pointer to the start of the vector data
  // but only if this is possible without allocating (in the R sense).
  static const void* Dataptr_or_null(SEXP alt) {
    // data2 has been created, and so the R sentinels are in place where the array has
    // nulls
    if (IsMaterialized(alt)) {
      return DATAPTR_RO(Representation(alt));
    }

    // there is only one chunk with no nulls, we can directly return the start of its data
    auto chunked_array = GetChunkedArray(alt);
    if (chunked_array->num_chunks() == 1 && chunked_array->null_count() == 0) {
      return reinterpret_cast<const void*>(
          chunked_array->chunk(0)->data()->template GetValues<c_type>(1));
    }

    // Otherwise: if the array has nulls and data2 has not been generated: give up
    return nullptr;
  }

  // R calls this to get a pointer to the start of the data, R allocations are allowed.
  static void* Dataptr(SEXP alt, Rboolean writeable) {
    // If the object hasn't been materialized, and the array has no
    // nulls we can directly point to the array data.
    if (!IsMaterialized(alt)) {
      const auto& chunked_array = GetChunkedArray(alt);

      if (chunked_array->num_chunks() == 1 && chunked_array->null_count() == 0) {
        return reinterpret_cast<void*>(const_cast<c_type*>(
            chunked_array->chunk(0)->data()->template GetValues<c_type>(1)));
      }
    }

    // Otherwise we have to materialize and hand the pointer to data2
    return DATAPTR(Materialize(alt));
  }

  // The value at position i
  static c_type Elt(SEXP alt, R_xlen_t i) {
    if (IsMaterialized(alt)) {
      return reinterpret_cast<c_type*>(DATAPTR(Representation(alt)))[i];
    }

    auto altrep_data =
        reinterpret_cast<ArrowAltrepData*>(R_ExternalPtrAddr(R_altrep_data1(alt)));
    auto resolve = altrep_data->locate(i);
    const auto& array = altrep_data->chunked_array()->chunk(resolve.chunk_index);
    auto j = resolve.index_in_chunk;

    return array->IsNull(j) ? cpp11::na<c_type>()
                            : array->data()->template GetValues<c_type>(1)[j];
  }

  // R calls this when it wants data from position `i` to `i + n` copied into `buf`
  // The returned value is the number of values that were really copied
  // (this can be lower than n)
  static R_xlen_t Get_region(SEXP alt, R_xlen_t i, R_xlen_t n, c_type* buf) {
    // If we have data2, we can just copy the region into buf
    // using the standard Get_region for this R type
    if (IsMaterialized(alt)) {
      return Standard_Get_region<c_type>(Representation(alt), i, n, buf);
    }

    // The vector was not materialized, aka we don't have data2
    //
    // In that case, we copy the data from the Array, and then
    // do a second pass to force the R sentinels for where the
    // array has nulls
    //
    // This only materializes the region into buf (not the entire vector).
    auto slice = GetChunkedArray(alt)->Slice(i, n);
    R_xlen_t ncopy = slice->length();

    c_type* out = buf;
    for (const auto& array : slice->chunks()) {
      auto n_i = array->length();

      // first copy the data buffer
      memcpy(out, array->data()->template GetValues<c_type>(1), n_i * sizeof(c_type));

      // then set the R NA sentinels if needed
      if (array->null_count() > 0) {
        internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                             array->offset(), n_i);

        for (R_xlen_t j = 0; j < n_i; j++, bitmap_reader.Next()) {
          if (bitmap_reader.IsNotSet()) {
            out[j] = cpp11::na<c_type>();
          }
        }
      }

      out += n_i;
    }

    return ncopy;
  }

  static std::shared_ptr<arrow::compute::ScalarAggregateOptions> NaRmOptions(bool na_rm) {
    auto options = std::make_shared<arrow::compute::ScalarAggregateOptions>(
        arrow::compute::ScalarAggregateOptions::Defaults());
    options->min_count = 0;
    options->skip_nulls = na_rm;
    return options;
  }

  template <bool Min>
  static SEXP MinMax(SEXP alt, Rboolean narm) {
    if (IsMaterialized(alt)) {
      return nullptr;
    }

    using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
    using scalar_type =
        typename std::conditional<sexp_type == INTSXP, Int32Scalar, DoubleScalar>::type;

    const auto& chunked_array = GetChunkedArray(alt);
    bool na_rm = narm == TRUE;
    auto n = chunked_array->length();
    auto null_count = chunked_array->null_count();
    if ((na_rm || n == 0) && null_count == n) {
      if (Min) {
        Rf_warning("no non-missing arguments to min; returning Inf");
        return Rf_ScalarReal(R_PosInf);
      } else {
        Rf_warning("no non-missing arguments to max; returning -Inf");
        return Rf_ScalarReal(R_NegInf);
      }
    }
    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }

    auto options = NaRmOptions(na_rm);

    const auto& minmax = ValueOrStop(
        arrow::compute::CallFunction("min_max", {chunked_array}, options.get()));
    const auto& minmax_scalar =
        internal::checked_cast<const StructScalar&>(*minmax.scalar());

    const auto& result_scalar = internal::checked_cast<const scalar_type&>(
        *ValueOrStop(minmax_scalar.field(Min ? "min" : "max")));
    return cpp11::as_sexp(result_scalar.value);
  }

  static SEXP Min(SEXP alt, Rboolean narm) { return MinMax<true>(alt, narm); }

  static SEXP Max(SEXP alt, Rboolean narm) { return MinMax<false>(alt, narm); }

  static SEXP Sum(SEXP alt, Rboolean narm) {
    if (IsMaterialized(alt)) {
      return nullptr;
    }

    using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

    const auto& chunked_array = GetChunkedArray(alt);
    bool na_rm = narm == TRUE;
    auto null_count = chunked_array->null_count();

    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }
    auto options = NaRmOptions(na_rm);

    const auto& sum =
        ValueOrStop(arrow::compute::CallFunction("sum", {chunked_array}, options.get()));

    if (sexp_type == INTSXP) {
      // When calling the "sum" function on an int32 array, we get an Int64 scalar
      // in case of overflow, make it a double like R
      int64_t value = internal::checked_cast<const Int64Scalar&>(*sum.scalar()).value;
      if (value <= INT32_MIN || value > INT32_MAX) {
        return Rf_ScalarReal(static_cast<double>(value));
      } else {
        return Rf_ScalarInteger(static_cast<int>(value));
      }
    } else {
      return Rf_ScalarReal(
          internal::checked_cast<const DoubleScalar&>(*sum.scalar()).value);
    }
  }
};
template <int sexp_type>
R_altrep_class_t AltrepVectorPrimitive<sexp_type>::class_t;

struct AltrepFactor : public AltrepVectorBase<AltrepFactor> {
  // singleton altrep class description
  static R_altrep_class_t class_t;

  using Base = AltrepVectorBase<AltrepFactor>;
  using Base::IsMaterialized;

  static R_xlen_t Length(SEXP alt) {
    if (IsMaterialized(alt)) {
      return Rf_xlength(Representation(alt));
    } else {
      return GetChunkedArray(alt)->length();
    }
  }

  // redefining because data2 is a paired list with the representation as the
  // first node: the CAR
  static SEXP Representation(SEXP alt) { return CAR(R_altrep_data2(alt)); }

  static void SetRepresentation(SEXP alt, SEXP x) { SETCAR(R_altrep_data2(alt), x); }

  // The CADR(data2) is used to store the transposed arrays when unification is needed
  // In that case we store a vector of Buffers
  using BufferVector = std::vector<std::shared_ptr<Buffer>>;

  static bool WasUnified(SEXP alt) { return !Rf_isNull(CADR(R_altrep_data2(alt))); }

  static const std::shared_ptr<Buffer>& GetArrayTransposed(SEXP alt, int i) {
    const auto& arrays = *Pointer<BufferVector>(CADR(R_altrep_data2(alt)));
    return (*arrays)[i];
  }

  static SEXP Make(const std::shared_ptr<ChunkedArray>& chunked_array) {
    // only dealing with dictionaries of strings
    if (internal::checked_cast<const DictionaryArray&>(*chunked_array->chunk(0))
            .dictionary()
            ->type_id() != Type::STRING) {
      return R_NilValue;
    }

    bool need_unification = DictionaryChunkArrayNeedUnification(chunked_array);

    std::shared_ptr<Array> dictionary;
    SEXP pointer_arrays_transpose;

    if (need_unification) {
      const auto& arr_type =
          internal::checked_cast<const DictionaryType&>(*chunked_array->type());
      std::unique_ptr<arrow::DictionaryUnifier> unifier_ =
          ValueOrStop(DictionaryUnifier::Make(arr_type.value_type()));

      size_t n_arrays = chunked_array->num_chunks();
      BufferVector arrays_transpose(n_arrays);

      for (size_t i = 0; i < n_arrays; i++) {
        const auto& dict_i =
            *internal::checked_cast<const DictionaryArray&>(*chunked_array->chunk(i))
                 .dictionary();
        StopIfNotOk(unifier_->Unify(dict_i, &arrays_transpose[i]));
      }

      std::shared_ptr<DataType> out_type;
      StopIfNotOk(unifier_->GetResult(&out_type, &dictionary));

      Pointer<BufferVector> ptr(
          new std::shared_ptr<BufferVector>(new BufferVector(arrays_transpose)));
      pointer_arrays_transpose = PROTECT(ptr);
    } else {
      // just use the first one
      const auto& dict_array =
          internal::checked_cast<const DictionaryArray&>(*chunked_array->chunk(0));
      dictionary = dict_array.dictionary();

      pointer_arrays_transpose = PROTECT(R_NilValue);
    }

    // the chunked array as data1
    SEXP data1 =
        PROTECT(external_pointer<ArrowAltrepData>(new ArrowAltrepData(chunked_array)));

    // a pairlist with the representation in the first node
    SEXP data2 = PROTECT(Rf_list2(R_NilValue,  // representation, empty at first
                                  pointer_arrays_transpose));

    SEXP alt = PROTECT(R_new_altrep(class_t, data1, data2));
    MARK_NOT_MUTABLE(alt);

    // set factor attributes
    Rf_setAttrib(alt, R_LevelsSymbol, Array__as_vector(dictionary));

    if (internal::checked_cast<const DictionaryType&>(*chunked_array->type()).ordered()) {
      Rf_classgets(alt, arrow::r::data::classes_ordered);
    } else {
      Rf_classgets(alt, arrow::r::data::classes_factor);
    }

    UNPROTECT(4);
    return alt;
  }

  // TODO: this is similar to the primitive Materialize
  static SEXP Materialize(SEXP alt) {
    if (!IsMaterialized(alt)) {
      auto size = Base::Length(alt);

      // create a standard R vector
      SEXP copy = PROTECT(Rf_allocVector(INTSXP, size));

      // copy the data from the array, through Get_region
      Get_region(alt, 0, size, reinterpret_cast<int*>(DATAPTR(copy)));

      // store as data2, this is now considered materialized
      SetRepresentation(alt, copy);

      // remove the ChunkedArray reference
      R_set_altrep_data1(alt, R_NilValue);

      UNPROTECT(1);
    }
    return Representation(alt);
  }

  static const void* Dataptr_or_null(SEXP alt) {
    if (IsMaterialized(alt)) {
      return DATAPTR_RO(Representation(alt));
    }

    return nullptr;
  }

  static void* Dataptr(SEXP alt, Rboolean writeable) { return DATAPTR(Materialize(alt)); }

  static SEXP Duplicate(SEXP alt, Rboolean /* deep */) {
    // the representation integer vector
    SEXP dup = PROTECT(Rf_lazy_duplicate(Materialize(alt)));

    // additional attributes from the altrep
    SEXP atts = PROTECT(Rf_duplicate(ATTRIB(alt)));
    SET_ATTRIB(dup, atts);

    UNPROTECT(2);
    return dup;
  }

  // The value at position i
  static int Elt(SEXP alt, R_xlen_t i) {
    if (Base::IsMaterialized(alt)) {
      return INTEGER_ELT(Representation(alt), i);
    }

    auto altrep_data =
        reinterpret_cast<ArrowAltrepData*>(R_ExternalPtrAddr(R_altrep_data1(alt)));
    auto resolve = altrep_data->locate(i);

    const auto& array = altrep_data->chunked_array()->chunk(resolve.chunk_index);
    auto j = resolve.index_in_chunk;

    if (!array->IsNull(j)) {
      const auto& indices =
          internal::checked_cast<const DictionaryArray&>(*array).indices();

      if (WasUnified(alt)) {
        const auto* transpose_data = reinterpret_cast<const int32_t*>(
            GetArrayTransposed(alt, resolve.chunk_index)->data());

        switch (indices->type_id()) {
          case Type::UINT8:
            return transpose_data[indices->data()->GetValues<uint8_t>(1)[j]] + 1;
          case Type::INT8:
            return transpose_data[indices->data()->GetValues<int8_t>(1)[j]] + 1;
          case Type::UINT16:
            return transpose_data[indices->data()->GetValues<uint16_t>(1)[j]] + 1;
          case Type::INT16:
            return transpose_data[indices->data()->GetValues<int16_t>(1)[j]] + 1;
          case Type::INT32:
            return transpose_data[indices->data()->GetValues<int32_t>(1)[j]] + 1;
          case Type::UINT32:
            return transpose_data[indices->data()->GetValues<uint32_t>(1)[j]] + 1;
          case Type::INT64:
            return transpose_data[indices->data()->GetValues<int64_t>(1)[j]] + 1;
          case Type::UINT64:
            return transpose_data[indices->data()->GetValues<uint64_t>(1)[j]] + 1;
          default:
            break;
        }
      } else {
        switch (indices->type_id()) {
          case Type::UINT8:
            return indices->data()->GetValues<uint8_t>(1)[j] + 1;
          case Type::INT8:
            return indices->data()->GetValues<int8_t>(1)[j] + 1;
          case Type::UINT16:
            return indices->data()->GetValues<uint16_t>(1)[j] + 1;
          case Type::INT16:
            return indices->data()->GetValues<int16_t>(1)[j] + 1;
          case Type::INT32:
            return indices->data()->GetValues<int32_t>(1)[j] + 1;
          case Type::UINT32:
            return indices->data()->GetValues<uint32_t>(1)[j] + 1;
          case Type::INT64:
            return indices->data()->GetValues<int64_t>(1)[j] + 1;
          case Type::UINT64:
            return indices->data()->GetValues<uint64_t>(1)[j] + 1;
          default:
            break;
        }
      }
    }

    // not reached
    return NA_INTEGER;
  }

  static R_xlen_t Get_region(SEXP alt, R_xlen_t start, R_xlen_t n, int* buf) {
    // If we have data2, we can just copy the region into buf
    // using the standard Get_region for this R type
    if (Base::IsMaterialized(alt)) {
      return Standard_Get_region<int>(Representation(alt), start, n, buf);
    }

    auto chunked_array = GetChunkedArray(alt);

    // get out if there is nothing to do
    auto chunked_array_size = chunked_array->length();
    if (start >= chunked_array_size) return 0;

    auto slice = GetChunkedArray(alt)->Slice(start, n);

    if (WasUnified(alt)) {
      int j = 0;

      // find out which is the first chunk of the chunk array
      // that is present in the slice, because the main loop
      // needs to refer to the correct transpose buffers
      int64_t k = 0;
      for (; j < chunked_array->num_chunks(); j++) {
        auto nj = chunked_array->chunk(j)->length();
        if (k + nj > start) {
          break;
        }

        k += nj;
      }

      int* out = buf;
      for (const auto& array : slice->chunks()) {
        const auto& indices =
            internal::checked_cast<const DictionaryArray&>(*array).indices();

        // using the transpose data for this chunk
        const auto* transpose_data =
            reinterpret_cast<const int32_t*>(GetArrayTransposed(alt, j)->data());
        auto transpose = [transpose_data](int x) { return transpose_data[x]; };

        GetRegionDispatch(array, indices, transpose, out);

        out += array->length();
        j++;
      }

    } else {
      // simpler case, identity transpose
      auto transpose = [](int x) { return x; };

      int* out = buf;
      for (const auto& array : slice->chunks()) {
        const auto& indices =
            internal::checked_cast<const DictionaryArray&>(*array).indices();

        GetRegionDispatch(array, indices, transpose, out);

        out += array->length();
      }
    }

    return slice->length();
  }

#define CALL_GET_REGION_TRANSPOSE(TYPE_CLASS)                                      \
  case TYPE_CLASS##Type::type_id:                                                  \
    GetRegionTranspose<TYPE_CLASS##Type>(array, indices,                           \
                                         std::forward<Transpose>(transpose), out); \
    break

  template <typename Transpose>
  static void GetRegionDispatch(const std::shared_ptr<Array>& array,
                                const std::shared_ptr<Array>& indices,
                                Transpose&& transpose, int* out) {
    switch (indices->type_id()) {
      ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(CALL_GET_REGION_TRANSPOSE);
      default:
        break;
    }
  }

  template <typename Type, typename Transpose>
  static void GetRegionTranspose(const std::shared_ptr<Array>& array,
                                 const std::shared_ptr<Array>& indices,
                                 Transpose transpose, int* out) {
    using index_type = typename Type::c_type;

    VisitArraySpanInline<Type>(
        *array->data(),
        /*valid_func=*/[&](index_type index) { *out++ = transpose(index) + 1; },
        /*null_func=*/[&]() { *out++ = cpp11::na<int>(); });
  }

  static SEXP Min(SEXP alt, Rboolean narm) { return nullptr; }
  static SEXP Max(SEXP alt, Rboolean narm) { return nullptr; }
  static SEXP Sum(SEXP alt, Rboolean narm) { return nullptr; }
};
R_altrep_class_t AltrepFactor::class_t;

// Implementation for string arrays
template <typename Type>
struct AltrepVectorString : public AltrepVectorBase<AltrepVectorString<Type>> {
  using Base = AltrepVectorBase<AltrepVectorString<Type>>;

  static R_altrep_class_t class_t;
  using StringArrayType = typename TypeTraits<Type>::ArrayType;

  using Base::Representation;
  using Base::SetRepresentation;

  static SEXP Make(const std::shared_ptr<ChunkedArray>& chunked_array) {
    string_viewer().set_strip_out_nuls(GetBoolOption("arrow.skip_nul", false));
    return Base::Make(chunked_array);
  }

  // Helper class to convert to R strings. We declare one of these for the
  // class to avoid having to stack-allocate one for every STRING_ELT call.
  struct RStringViewer {
    RStringViewer() : strip_out_nuls_(false), nul_was_stripped_(false) {}

    void reset_null_was_stripped() { nul_was_stripped_ = false; }

    void set_strip_out_nuls(bool strip_out_nuls) { strip_out_nuls_ = strip_out_nuls; }

    // convert the i'th string of the Array to an R string (CHARSXP)
    SEXP Convert(size_t i) {
      if (array_->IsNull(i)) {
        return NA_STRING;
      }

      view_ = string_array_->GetView(i);
      bool no_nul = std::find(view_.begin(), view_.end(), '\0') == view_.end();

      if (no_nul) {
        return Rf_mkCharLenCE(view_.data(), view_.size(), CE_UTF8);
      } else if (strip_out_nuls_) {
        return ConvertStripNul();
      } else {
        Error();

        // not reached
        return R_NilValue;
      }
    }

    // strip the nuls and then convert to R string
    SEXP ConvertStripNul() {
      const char* old_string = view_.data();

      size_t stripped_len = 0, nul_count = 0;

      for (size_t i = 0; i < view_.size(); i++) {
        if (old_string[i] == '\0') {
          ++nul_count;

          if (nul_count == 1) {
            // first nul spotted: allocate stripped string storage
            stripped_string_.assign(view_.begin(), view_.end());
            stripped_len = i;
          }

          // don't copy old_string[i] (which is \0) into stripped_string
          continue;
        }

        if (nul_count > 0) {
          stripped_string_[stripped_len++] = old_string[i];
        }
      }

      nul_was_stripped_ = true;
      return Rf_mkCharLenCE(stripped_string_.data(), stripped_len, CE_UTF8);
    }

    bool nul_was_stripped() const { return nul_was_stripped_; }

    // throw R error about embedded nul
    void Error() {
      stripped_string_ = "embedded nul in string: '";
      for (char c : view_) {
        if (c) {
          stripped_string_ += c;
        } else {
          stripped_string_ += "\\0";
        }
      }

      stripped_string_ +=
          "'; to strip nuls when converting from Arrow to R, set options(arrow.skip_nul "
          "= TRUE)";

      Rf_error(stripped_string_.c_str());
    }

    void SetArray(const std::shared_ptr<Array>& array) {
      array_ = array;
      string_array_ = internal::checked_cast<const StringArrayType*>(array.get());
    }

    std::shared_ptr<Array> array_;
    const StringArrayType* string_array_;
    std::string stripped_string_;
    bool strip_out_nuls_;
    bool nul_was_stripped_;
    std::string_view view_;
  };

  // Get a single string as a CHARSXP SEXP
  static SEXP Elt(SEXP alt, R_xlen_t i) {
    if (Base::IsMaterialized(alt)) {
      return STRING_ELT(Representation(alt), i);
    }

    auto altrep_data =
        reinterpret_cast<ArrowAltrepData*>(R_ExternalPtrAddr(R_altrep_data1(alt)));
    auto resolve = altrep_data->locate(i);
    const auto& array = altrep_data->chunked_array()->chunk(resolve.chunk_index);
    auto j = resolve.index_in_chunk;

    SEXP s = NA_STRING;
    RStringViewer& r_string_viewer = string_viewer();
    r_string_viewer.SetArray(array);
    // Note: we don't check GetBoolOption("arrow.skip_nul", false) here
    // because it is too expensive to do so. We do set this value whenever
    // an altrep string; however, there is a chance that this value could
    // be out of date by the time a value in the vector is accessed.
    r_string_viewer.reset_null_was_stripped();
    s = r_string_viewer.Convert(j);
    if (r_string_viewer.nul_was_stripped()) {
      Rf_warning("Stripping '\\0' (nul) from character vector");
    }

    return s;
  }

  static void* Dataptr(SEXP alt, Rboolean writeable) { return DATAPTR(Materialize(alt)); }

  static SEXP Materialize(SEXP alt) {
    if (Base::IsMaterialized(alt)) {
      return Representation(alt);
    }

    const auto& chunked_array = GetChunkedArray(alt);
    SEXP data2 = PROTECT(Rf_allocVector(STRSXP, chunked_array->length()));
    MARK_NOT_MUTABLE(data2);

    R_xlen_t i = 0;
    RStringViewer& r_string_viewer = string_viewer();
    r_string_viewer.reset_null_was_stripped();
    r_string_viewer.set_strip_out_nuls(GetBoolOption("arrow.skip_nul", false));
    for (const auto& array : chunked_array->chunks()) {
      r_string_viewer.SetArray(array);

      auto ni = array->length();
      for (R_xlen_t j = 0; j < ni; j++, i++) {
        SET_STRING_ELT(data2, i, r_string_viewer.Convert(j));
      }
    }

    if (r_string_viewer.nul_was_stripped()) {
      Rf_warning("Stripping '\\0' (nul) from character vector");
    }

    // only set to data2 if all the values have been converted
    SetRepresentation(alt, data2);
    UNPROTECT(1);  // data2

    // remove reference to chunked array
    R_set_altrep_data1(alt, R_NilValue);

    return data2;
  }

  static const void* Dataptr_or_null(SEXP alt) {
    if (Base::IsMaterialized(alt)) return DATAPTR(Representation(alt));

    // otherwise give up
    return nullptr;
  }

  static void Set_elt(SEXP alt, R_xlen_t i, SEXP v) {
    Rf_error("ALTSTRING objects of type <arrow::array_string_vector> are immutable");
  }

  static RStringViewer& string_viewer() {
    static RStringViewer string_viewer;
    return string_viewer;
  }
};

template <typename Type>
R_altrep_class_t AltrepVectorString<Type>::class_t;

// initialize altrep, altvec, altreal, and altinteger methods
template <typename AltrepClass>
void InitAltrepMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altrep_Length_method(class_t, AltrepClass::Length);
  R_set_altrep_Inspect_method(class_t, AltrepClass::Inspect);
  R_set_altrep_Duplicate_method(class_t, AltrepClass::Duplicate);
  R_set_altrep_Serialized_state_method(class_t, AltrepClass::Serialized_state);
  R_set_altrep_Unserialize_method(class_t, AltrepClass::Unserialize);
  R_set_altrep_Coerce_method(class_t, AltrepClass::Coerce);
}

template <typename AltrepClass>
void InitAltvecMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altvec_Dataptr_method(class_t, AltrepClass::Dataptr);
  R_set_altvec_Dataptr_or_null_method(class_t, AltrepClass::Dataptr_or_null);
}

template <typename AltrepClass>
void InitAltRealMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altreal_No_NA_method(class_t, AltrepClass::No_NA);
  R_set_altreal_Is_sorted_method(class_t, AltrepClass::Is_sorted);

  R_set_altreal_Sum_method(class_t, AltrepClass::Sum);
  R_set_altreal_Min_method(class_t, AltrepClass::Min);
  R_set_altreal_Max_method(class_t, AltrepClass::Max);

  R_set_altreal_Elt_method(class_t, AltrepClass::Elt);
  R_set_altreal_Get_region_method(class_t, AltrepClass::Get_region);
}

template <typename AltrepClass>
void InitAltIntegerMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altinteger_No_NA_method(class_t, AltrepClass::No_NA);
  R_set_altinteger_Is_sorted_method(class_t, AltrepClass::Is_sorted);

  R_set_altinteger_Sum_method(class_t, AltrepClass::Sum);
  R_set_altinteger_Min_method(class_t, AltrepClass::Min);
  R_set_altinteger_Max_method(class_t, AltrepClass::Max);

  R_set_altinteger_Elt_method(class_t, AltrepClass::Elt);
  R_set_altinteger_Get_region_method(class_t, AltrepClass::Get_region);
}

template <typename AltrepClass>
void InitAltRealClass(DllInfo* dll, const char* name) {
  AltrepClass::class_t = R_make_altreal_class(name, "arrow", dll);
  InitAltrepMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltvecMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltRealMethods<AltrepClass>(AltrepClass::class_t, dll);
}

template <typename AltrepClass>
void InitAltIntegerClass(DllInfo* dll, const char* name) {
  AltrepClass::class_t = R_make_altinteger_class(name, "arrow", dll);
  InitAltrepMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltvecMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltIntegerMethods<AltrepClass>(AltrepClass::class_t, dll);
}

template <typename AltrepClass>
void InitAltStringClass(DllInfo* dll, const char* name) {
  AltrepClass::class_t = R_make_altstring_class(name, "arrow", dll);
  R_set_altrep_Length_method(AltrepClass::class_t, AltrepClass::Length);
  R_set_altrep_Inspect_method(AltrepClass::class_t, AltrepClass::Inspect);
  R_set_altrep_Duplicate_method(AltrepClass::class_t, AltrepClass::Duplicate);
  R_set_altrep_Serialized_state_method(AltrepClass::class_t,
                                       AltrepClass::Serialized_state);
  R_set_altrep_Unserialize_method(AltrepClass::class_t, AltrepClass::Unserialize);
  R_set_altrep_Coerce_method(AltrepClass::class_t, AltrepClass::Coerce);

  R_set_altvec_Dataptr_method(AltrepClass::class_t, AltrepClass::Dataptr);
  R_set_altvec_Dataptr_or_null_method(AltrepClass::class_t, AltrepClass::Dataptr_or_null);

  R_set_altstring_Elt_method(AltrepClass::class_t, AltrepClass::Elt);
  R_set_altstring_Set_elt_method(AltrepClass::class_t, AltrepClass::Set_elt);
  R_set_altstring_No_NA_method(AltrepClass::class_t, AltrepClass::No_NA);
  R_set_altstring_Is_sorted_method(AltrepClass::class_t, AltrepClass::Is_sorted);
}

}  // namespace

// initialize the altrep classes
void Init_Altrep_classes(DllInfo* dll) {
  InitAltRealClass<AltrepVectorPrimitive<REALSXP>>(dll, "arrow::array_dbl_vector");
  InitAltIntegerClass<AltrepVectorPrimitive<INTSXP>>(dll, "arrow::array_int_vector");
  InitAltIntegerClass<AltrepFactor>(dll, "arrow::array_factor");

  InitAltStringClass<AltrepVectorString<StringType>>(dll, "arrow::array_string_vector");
  InitAltStringClass<AltrepVectorString<LargeStringType>>(
      dll, "arrow::array_large_string_vector");
}

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepVector(const std::shared_ptr<ChunkedArray>& chunked_array) {
  // using altrep if
  // - the arrow.use_altrep is set to TRUE or unset (implicit TRUE)
  // - the chunked array has at least one element
  if (arrow::r::GetBoolOption("arrow.use_altrep", true) && chunked_array->length() > 0) {
    switch (chunked_array->type()->id()) {
      case arrow::Type::DOUBLE:
        return altrep::AltrepVectorPrimitive<REALSXP>::Make(chunked_array);

      case arrow::Type::INT32:
        return altrep::AltrepVectorPrimitive<INTSXP>::Make(chunked_array);

      case arrow::Type::STRING:
        return altrep::AltrepVectorString<StringType>::Make(chunked_array);

      case arrow::Type::LARGE_STRING:
        return altrep::AltrepVectorString<LargeStringType>::Make(chunked_array);

      case arrow::Type::DICTIONARY:
        return altrep::AltrepFactor::Make(chunked_array);

      default:
        break;
    }
  }

  return R_NilValue;
}

bool is_arrow_altrep(SEXP x) {
  if (ALTREP(x)) {
    SEXP info = ALTREP_CLASS_SERIALIZED_CLASS(ALTREP_CLASS(x));
    SEXP pkg = ALTREP_SERIALIZED_CLASS_PKGSYM(info);

    if (pkg == symbols::arrow) return true;
  }

  return false;
}

bool is_unmaterialized_arrow_altrep(SEXP x) {
  return is_arrow_altrep(x) && R_altrep_data1(x) != R_NilValue;
}

std::shared_ptr<ChunkedArray> vec_to_arrow_altrep_bypass(SEXP x) {
  if (is_unmaterialized_arrow_altrep(x)) {
    return GetChunkedArray(x);
  }

  return nullptr;
}

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#else  // HAS_ALTREP

namespace arrow {
namespace r {
namespace altrep {

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepVector(const std::shared_ptr<ChunkedArray>& chunked_array) {
  return R_NilValue;
}

bool is_arrow_altrep(SEXP) { return false; }

std::shared_ptr<ChunkedArray> vec_to_arrow_altrep_bypass(SEXP x) { return nullptr; }

bool is_unmaterialized_arrow_altrep(SEXP) { return false; }

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#endif

// [[arrow::export]]
bool is_arrow_altrep(cpp11::sexp x) { return arrow::r::altrep::is_arrow_altrep(x); }

// [[arrow::export]]
void test_arrow_altrep_set_string_elt(sexp x, int i, std::string value) {
  if (!is_arrow_altrep(x)) {
    stop("x is not arrow ALTREP");
  }

  SET_STRING_ELT(x, i, Rf_mkChar(value.c_str()));
}

// [[arrow::export]]
sexp test_arrow_altrep_is_materialized(sexp x) {
  if (!is_arrow_altrep(x)) {
    return Rf_ScalarLogical(NA_LOGICAL);
  }

  sexp data_class_sym = CAR(ATTRIB(ALTREP_CLASS(x)));
  std::string class_name(CHAR(PRINTNAME(data_class_sym)));

  int result = NA_LOGICAL;
  if (class_name == "arrow::array_dbl_vector") {
    result = arrow::r::altrep::AltrepVectorPrimitive<REALSXP>::IsMaterialized(x);
  } else if (class_name == "arrow::array_int_vector") {
    result = arrow::r::altrep::AltrepVectorPrimitive<INTSXP>::IsMaterialized(x);
  } else if (class_name == "arrow::array_string_vector") {
    result = arrow::r::altrep::AltrepVectorString<arrow::StringType>::IsMaterialized(x);
  } else if (class_name == "arrow::array_large_string_vector") {
    result =
        arrow::r::altrep::AltrepVectorString<arrow::LargeStringType>::IsMaterialized(x);
  } else if (class_name == "arrow::array_factor") {
    result = arrow::r::altrep::AltrepFactor::IsMaterialized(x);
  }

  return Rf_ScalarLogical(result);
}

// [[arrow::export]]
bool test_arrow_altrep_force_materialize(sexp x) {
  if (!is_arrow_altrep(x)) {
    stop("x is not arrow ALTREP");
  }

  bool already_materialized = as_cpp<bool>(test_arrow_altrep_is_materialized(x));
  if (already_materialized) {
    stop("x is already materialized");
  }

  sexp data_class_sym = CAR(ATTRIB(ALTREP_CLASS(x)));
  std::string class_name(CHAR(PRINTNAME(data_class_sym)));

  if (class_name == "arrow::array_dbl_vector") {
    arrow::r::altrep::AltrepVectorPrimitive<REALSXP>::Materialize(x);
  } else if (class_name == "arrow::array_int_vector") {
    arrow::r::altrep::AltrepVectorPrimitive<INTSXP>::Materialize(x);
  } else if (class_name == "arrow::array_string_vector") {
    arrow::r::altrep::AltrepVectorString<arrow::StringType>::Materialize(x);
  } else if (class_name == "arrow::array_large_string_vector") {
    arrow::r::altrep::AltrepVectorString<arrow::LargeStringType>::Materialize(x);
  } else if (class_name == "arrow::array_factor") {
    arrow::r::altrep::AltrepFactor::Materialize(x);
  } else {
    return false;
  }

  return true;
}

// [[arrow::export]]
sexp test_arrow_altrep_copy_by_element(sexp x) {
  if (!is_arrow_altrep(x)) {
    stop("x is not arrow ALTREP");
  }

  R_xlen_t n = Rf_xlength(x);

  if (TYPEOF(x) == INTSXP) {
    cpp11::writable::integers out(Rf_xlength(x));
    for (R_xlen_t i = 0; i < n; i++) {
      out[i] = INTEGER_ELT(x, i);
    }
    return out;
  } else if (TYPEOF(x) == REALSXP) {
    cpp11::writable::doubles out(Rf_xlength(x));
    for (R_xlen_t i = 0; i < n; i++) {
      out[i] = REAL_ELT(x, i);
    }
    return out;
  } else if (TYPEOF(x) == STRSXP) {
    cpp11::writable::strings out(Rf_xlength(x));
    for (R_xlen_t i = 0; i < n; i++) {
      out[i] = STRING_ELT(x, i);
    }
    return out;
  } else {
    return R_NilValue;
  }
}

// [[arrow::export]]
sexp test_arrow_altrep_copy_by_region(sexp x, R_xlen_t region_size) {
  if (!is_arrow_altrep(x)) {
    stop("x is not arrow ALTREP");
  }

  R_xlen_t n = Rf_xlength(x);

  if (TYPEOF(x) == INTSXP) {
    cpp11::writable::integers out(Rf_xlength(x));
    cpp11::writable::integers buf_shelter(region_size);
    int* buf = INTEGER(buf_shelter);
    for (R_xlen_t i = 0; i < n; i++) {
      if ((i % region_size) == 0) {
        INTEGER_GET_REGION(x, i, region_size, buf);
      }
      out[i] = buf[i % region_size];
    }
    return out;
  } else if (TYPEOF(x) == REALSXP) {
    cpp11::writable::doubles out(Rf_xlength(x));
    cpp11::writable::doubles buf_shelter(region_size);
    double* buf = REAL(buf_shelter);
    for (R_xlen_t i = 0; i < n; i++) {
      if ((i % region_size) == 0) {
        REAL_GET_REGION(x, i, region_size, buf);
      }
      out[i] = buf[i % region_size];
    }
    return out;
  } else {
    return R_NilValue;
  }
}

// [[arrow::export]]
sexp test_arrow_altrep_copy_by_dataptr(sexp x) {
  if (!is_arrow_altrep(x)) {
    stop("x is not arrow ALTREP");
  }

  R_xlen_t n = Rf_xlength(x);

  if (TYPEOF(x) == INTSXP) {
    cpp11::writable::integers out(Rf_xlength(x));
    int* ptr = reinterpret_cast<int*>(DATAPTR(x));
    for (R_xlen_t i = 0; i < n; i++) {
      out[i] = ptr[i];
    }
    return out;
  } else if (TYPEOF(x) == REALSXP) {
    cpp11::writable::doubles out(Rf_xlength(x));
    double* ptr = reinterpret_cast<double*>(DATAPTR(x));
    for (R_xlen_t i = 0; i < n; i++) {
      out[i] = ptr[i];
    }
    return out;
  } else if (TYPEOF(x) == STRSXP) {
    cpp11::writable::strings out(Rf_xlength(x));
    SEXP* ptr = reinterpret_cast<SEXP*>(DATAPTR(x));
    for (R_xlen_t i = 0; i < n; i++) {
      out[i] = ptr[i];
    }
    return out;
  } else {
    return R_NilValue;
  }
}
