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

#ifndef ARROW_TENSORFLOW_CONVERTER_H
#define ARROW_TENSORFLOW_CONVERTER_H

#include <memory>

#include "tensorflow/core/framework/op.h"

#include "arrow/type.h"

// These utilities are supposed to be included in TensorFlow operators
// that need to be compiled separately from Arrow because of ABI issues.
// They therefore need to be header-only.

namespace arrow {

namespace adapters {

namespace tensorflow {

Status GetArrowType(::tensorflow::DataType dtype, std::shared_ptr<DataType>* out) {
  switch (dtype) {
    case ::tensorflow::DT_BOOL:
      *out = arrow::boolean();
      break;
    case ::tensorflow::DT_FLOAT:
      *out = arrow::float32();
      break;
    case ::tensorflow::DT_DOUBLE:
      *out = arrow::float64();
      break;
    case ::tensorflow::DT_HALF:
      *out = arrow::float16();
      break;
    case ::tensorflow::DT_INT8:
      *out = arrow::int8();
      break;
    case ::tensorflow::DT_INT16:
      *out = arrow::int16();
      break;
    case ::tensorflow::DT_INT32:
      *out = arrow::int32();
      break;
    case ::tensorflow::DT_INT64:
      *out = arrow::int64();
      break;
    case ::tensorflow::DT_UINT8:
      *out = arrow::uint8();
      break;
    case ::tensorflow::DT_UINT16:
      *out = arrow::uint16();
      break;
    case ::tensorflow::DT_UINT32:
      *out = arrow::uint32();
      break;
    case ::tensorflow::DT_UINT64:
      *out = arrow::uint64();
      break;
    case ::tensorflow::DT_BFLOAT16:
    case ::tensorflow::DT_COMPLEX64:
    case ::tensorflow::DT_COMPLEX128:
    case ::tensorflow::DT_INVALID:
    case ::tensorflow::DT_QINT8:
    case ::tensorflow::DT_QINT16:
    case ::tensorflow::DT_QINT32:
    case ::tensorflow::DT_QUINT8:
    case ::tensorflow::DT_QUINT16:
    case ::tensorflow::DT_RESOURCE:
    case ::tensorflow::DT_STRING:
    case ::tensorflow::DT_VARIANT:
    default:
      return Status::TypeError("TensorFlow data type is not supported");
  }
  return Status::OK();
}

Status GetTensorFlowType(std::shared_ptr<DataType> dtype, ::tensorflow::DataType* out) {
  switch (dtype->id()) {
    case Type::BOOL:
      *out = ::tensorflow::DT_BOOL;
      break;
    case Type::UINT8:
      *out = ::tensorflow::DT_UINT8;
      break;
    case Type::INT8:
      *out = ::tensorflow::DT_INT8;
      break;
    case Type::UINT16:
      *out = ::tensorflow::DT_UINT16;
      break;
    case Type::INT16:
      *out = ::tensorflow::DT_INT16;
      break;
    case Type::UINT32:
      *out = ::tensorflow::DT_UINT32;
      break;
    case Type::INT32:
      *out = ::tensorflow::DT_INT32;
      break;
    case Type::UINT64:
      *out = ::tensorflow::DT_UINT64;
      break;
    case Type::INT64:
      *out = ::tensorflow::DT_INT64;
      break;
    case Type::HALF_FLOAT:
      *out = ::tensorflow::DT_HALF;
      break;
    case Type::FLOAT:
      *out = ::tensorflow::DT_FLOAT;
      break;
    case Type::DOUBLE:
      *out = ::tensorflow::DT_DOUBLE;
      break;
    case Type::STRING:
    case Type::BINARY:
    case Type::FIXED_SIZE_BINARY:
    case Type::DATE32:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME32:
    case Type::TIME64:
    case Type::INTERVAL:
    case Type::DECIMAL:
    case Type::LIST:
    case Type::STRUCT:
    case Type::UNION:
    case Type::DICTIONARY:
    case Type::MAP:
    default:
      return Status::TypeError("Arrow data type is not supported");
  }
  return arrow::Status::OK();
}

}  // namespace tensorflow

}  // namespace adapters

}  // namespace arrow

#endif  // ARROW_TENSORFLOW_CONVERTER_H
