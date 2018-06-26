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

#ifndef MATLAB_ARROW_IPC_FEATHER_FEATHER_TYPE_H
#define MATLAB_ARROW_IPC_FEATHER_FEATHER_TYPE_H

#include <arrow/type.h>

#include <matrix.h>

namespace matlab {
namespace arrow {
namespace ipc {
namespace feather {

/// \brief A type traits class mapping Arrow types to MATLAB types.
template <::arrow::Type::type ArrowTypeID>
struct FeatherType;

template <>
struct FeatherType<::arrow::Type::FLOAT> {
  static constexpr mxClassID matlab_class_id = mxSINGLE_CLASS;
  typedef mxSingle MatlabType;
  typedef ::arrow::FloatArray ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetSingles(pa); }
};

template <>
struct FeatherType<::arrow::Type::DOUBLE> {
  static constexpr mxClassID matlab_class_id = mxDOUBLE_CLASS;
  typedef mxDouble MatlabType;
  typedef ::arrow::DoubleArray ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetDoubles(pa); }
};

template <>
struct FeatherType<::arrow::Type::UINT8> {
  static constexpr mxClassID matlab_class_id = mxUINT8_CLASS;
  typedef mxUint8 MatlabType;
  typedef ::arrow::UInt8Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint8s(pa); }
};

template <>
struct FeatherType<::arrow::Type::UINT16> {
  static constexpr mxClassID matlab_class_id = mxUINT16_CLASS;
  typedef mxUint16 MatlabType;
  typedef ::arrow::UInt16Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint16s(pa); }
};

template <>
struct FeatherType<::arrow::Type::UINT32> {
  static constexpr mxClassID matlab_class_id = mxUINT32_CLASS;
  typedef mxUint32 MatlabType;
  typedef ::arrow::UInt32Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint32s(pa); }
};

template <>
struct FeatherType<::arrow::Type::UINT64> {
  static constexpr mxClassID matlab_class_id = mxUINT64_CLASS;
  typedef mxUint64 MatlabType;
  typedef ::arrow::UInt64Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetUint64s(pa); }
};

template <>
struct FeatherType<::arrow::Type::INT8> {
  static constexpr mxClassID matlab_class_id = mxINT8_CLASS;
  typedef mxInt8 MatlabType;
  typedef ::arrow::Int8Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt8s(pa); }
};

template <>
struct FeatherType<::arrow::Type::INT16> {
  static constexpr mxClassID matlab_class_id = mxINT16_CLASS;
  typedef mxInt16 MatlabType;
  typedef ::arrow::Int16Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt16s(pa); }
};

template <>
struct FeatherType<::arrow::Type::INT32> {
  static constexpr mxClassID matlab_class_id = mxINT32_CLASS;
  typedef mxInt32 MatlabType;
  typedef ::arrow::Int32Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt32s(pa); }
};

template <>
struct FeatherType<::arrow::Type::INT64> {
  static constexpr mxClassID matlab_class_id = mxINT64_CLASS;
  typedef mxInt64 MatlabType;
  typedef ::arrow::Int64Array ArrowArrayType;
  static MatlabType* GetData(mxArray* pa) { return mxGetInt64s(pa); }
};

}  // namespace feather
}  // namespace ipc
}  // namespace arrow
}  // namespace matlab

#endif  // MATLAB_ARROW_IPC_FEATHER_FEATHER_TYPE_H
