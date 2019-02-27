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

#include "arrow/compute/expression.h"

#include <memory>
#include <utility>

#include "arrow/compute/logical_type.h"
#include "arrow/status.h"

namespace arrow {
namespace compute {

ValueExpr::ValueExpr(std::shared_ptr<Operation> op, std::shared_ptr<TypeClass>& type,
                     Rank rank)
    : Expr(std::move(op)), type_(std::move(type)), rank_(rank) {}

ScalarExpr::ScalarExpr(std::shared_ptr<Operation> op, std::shared_ptr<TypeClass> type)
    : ValueExpr(std::move(op), std::move(type), ValueExpr::SCALAR) {}

ArrayExpr::ArrayExpr(std::shared_ptr<Operation> op, std::shared_ptr<TypeClass> type)
    : ValueExpr(std::move(op), std::move(type), ValueExpr::ARRAY) {}

// ----------------------------------------------------------------------

const std::string& Int8Array::kind() const { return "array[int8]"; }

const std::string& Int8Scalar::kind() const { return "scalar[int8]"; }

const std::string& Int16Array::kind() const { return "array[int16]"; }

const std::string& Int16Scalar::kind() const { return "scalar[int16]"; }

const std::string& Int32Array::kind() const { return "array[int32]"; }

const std::string& Int32Scalar::kind() const { return "scalar[int32]"; }

const std::string& Int64Array::kind() const { return "array[int64]"; }

const std::string& Int64Scalar::kind() const { return "scalar[int64]"; }

const std::string& UInt8Array::kind() const { return "array[uint8]"; }

const std::string& UInt8Scalar::kind() const { return "scalar[uint8]"; }

const std::string& UInt16Array::kind() const { return "array[uint16]"; }

const std::string& UInt16Scalar::kind() const { return "scalar[uint16]"; }

const std::string& UInt32Array::kind() const { return "array[uint32]"; }

const std::string& UInt32Scalar::kind() const { return "scalar[uint32]"; }

const std::string& UInt64Array::kind() const { return "array[uint64]"; }

const std::string& UInt64Scalar::kind() const { return "scalar[uint64]"; }

const std::string& FloatArray::kind() const { return "array[float]"; }

const std::string& FloatScalar::kind() const { return "scalar[float]"; }

const std::string& DoubleArray::kind() const { return "array[double]"; }

const std::string& DoubleScalar::kind() const { return "scalar[double]"; }

}  // namespace compute
}  // namespace arrow
