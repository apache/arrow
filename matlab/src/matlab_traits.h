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

#ifndef MLARROW_MATLAB_TRAITS_H
#define MLARROW_MATLAB_TRAITS_H

#include <arrow/type.h>

#include <matrix.h>

namespace mlarrow {

/// \brief A type traits class mapping Arrow types to MATLAB types.
template <typename ArrowDataType>
struct MatlabTraits;

template <>
struct MatlabTraits<arrow::FloatType> {
  static constexpr mxClassID matlab_class_id = mxSINGLE_CLASS;
  typedef mxSingle MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetSingles(pa); }
};

template <>
struct MatlabTraits<arrow::DoubleType> {
  static constexpr mxClassID matlab_class_id = mxDOUBLE_CLASS;
  typedef mxDouble MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetDoubles(pa); }
};

template <>
struct MatlabTraits<arrow::UInt8Type> {
  static constexpr mxClassID matlab_class_id = mxUINT8_CLASS;
  typedef mxUint8 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint8s(pa); }
};

template <>
struct MatlabTraits<arrow::UInt16Type> {
  static constexpr mxClassID matlab_class_id = mxUINT16_CLASS;
  typedef mxUint16 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint16s(pa); }
};

template <>
struct MatlabTraits<arrow::UInt32Type> {
  static constexpr mxClassID matlab_class_id = mxUINT32_CLASS;
  typedef mxUint32 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint32s(pa); }
};

template <>
struct MatlabTraits<arrow::UInt64Type> {
  static constexpr mxClassID matlab_class_id = mxUINT64_CLASS;
  typedef mxUint64 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint64s(pa); }
};

template <>
struct MatlabTraits<arrow::Int8Type> {
  static constexpr mxClassID matlab_class_id = mxINT8_CLASS;
  typedef mxInt8 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt8s(pa); }
};

template <>
struct MatlabTraits<arrow::Int16Type> {
  static constexpr mxClassID matlab_class_id = mxINT16_CLASS;
  typedef mxInt16 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt16s(pa); }
};

template <>
struct MatlabTraits<arrow::Int32Type> {
  static constexpr mxClassID matlab_class_id = mxINT32_CLASS;
  typedef mxInt32 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt32s(pa); }
};

template <>
struct MatlabTraits<arrow::Int64Type> {
  static constexpr mxClassID matlab_class_id = mxINT64_CLASS;
  typedef mxInt64 MatlabType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt64s(pa); }
};

}  // namespace mlarrow

#endif  // MLARROW_MATLAB_TRAITS_H
