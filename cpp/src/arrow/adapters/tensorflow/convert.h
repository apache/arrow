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

arrow::Status GetArrowType(::tensorflow::DataType dtype,
                           std::shared_ptr<arrow::DataType>* out) {
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
      return arrow::Status(arrow::StatusCode::TypeError,
                           "TensorFlow data type is not supported");
  }
  return arrow::Status::OK();
}

arrow::Status GetTensorFlowType(std::shared_ptr<arrow::DataType> dtype,
                                ::tensorflow::DataType* out) {
  switch (dtype->id()) {
    case arrow::Type::BOOL:
      *out = ::tensorflow::DT_BOOL;
      break;
    case arrow::Type::UINT8:
      *out = ::tensorflow::DT_UINT8;
      break;
    case arrow::Type::INT8:
      *out = ::tensorflow::DT_INT8;
      break;
    case arrow::Type::UINT16:
      *out = ::tensorflow::DT_UINT16;
      break;
    case arrow::Type::INT16:
      *out = ::tensorflow::DT_INT16;
      break;
    case arrow::Type::UINT32:
      *out = ::tensorflow::DT_UINT32;
      break;
    case arrow::Type::INT32:
      *out = ::tensorflow::DT_INT32;
      break;
    case arrow::Type::UINT64:
      *out = ::tensorflow::DT_UINT64;
      break;
    case arrow::Type::INT64:
      *out = ::tensorflow::DT_INT64;
      break;
    case arrow::Type::HALF_FLOAT:
      *out = ::tensorflow::DT_HALF;
      break;
    case arrow::Type::FLOAT:
      *out = ::tensorflow::DT_FLOAT;
      break;
    case arrow::Type::DOUBLE:
      *out = ::tensorflow::DT_DOUBLE;
      break;
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
    case arrow::Type::TIMESTAMP:
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
    case arrow::Type::INTERVAL:
    case arrow::Type::DECIMAL:
    case arrow::Type::LIST:
    case arrow::Type::STRUCT:
    case arrow::Type::UNION:
    case arrow::Type::DICTIONARY:
    case arrow::Type::MAP:
    default:
      return arrow::Status(arrow::StatusCode::TypeError,
                           "Arrow data type is not supported");
  }
  return arrow::Status::OK();
}

}  // namespace tensorflow

}  // namespace adapters

}  // namespace arrow

#endif  // ARROW_TENSORFLOW_CONVERTER_H
