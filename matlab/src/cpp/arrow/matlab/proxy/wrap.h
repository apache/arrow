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

#pragma once

#include "MatlabDataArray.hpp"
#include "arrow/array.h"
#include "arrow/matlab/array/proxy/array.h"
#include "arrow/matlab/type/proxy/type.h"
#include "arrow/result.h"

namespace arrow::matlab::proxy {

/// \brief Wraps an array within a proxy::Array.
///
/// \return arrow::result<std::shared_ptr<arrow::matlab::array::proxy::Array>>
arrow::Result<std::shared_ptr<arrow::matlab::array::proxy::Array>> wrap(
    const std::shared_ptr<arrow::Array>& array);

/// \brief Wraps an array within a proxy::Array and adds the Proxy to the ProxyManager.
///
/// \return arrow::Result<mda::StructArray>. The mda::StructArray has two fields: ProxyID
/// (uint64) and TypeID (int32).
arrow::Result<::matlab::data::StructArray> wrap_and_manage(
    const std::shared_ptr<arrow::Array>& array);

/// \brief Wraps a DataType within a proxy::DataType.
///
/// \return arrow::result<std::shared_ptr<arrow::matlab::type::proxy::Type>>
arrow::Result<std::shared_ptr<arrow::matlab::type::proxy::Type>> wrap(
    const std::shared_ptr<arrow::DataType>& datatype);

/// \brief Wraps a DataType within a proxy::DataType and adds the proxy to the
/// ProxyManager.
///
/// \return arrow::Result<mda::StructArray>. The mda::StructArray has two fields: ProxyID
/// (uint64) and TypeID (int32).
arrow::Result<::matlab::data::StructArray> wrap_and_manage(
    const std::shared_ptr<arrow::DataType>& datatype);
}  // namespace arrow::matlab::proxy
