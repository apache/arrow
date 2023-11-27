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

#include "arrow/array/array_base.h"
#include "arrow/c/dlpack_abi.h"

namespace arrow {

namespace dlpack {

/// \brief DLPack protocol for producing DLManagedTensor
///
/// DLMangedTensor is produced from an array as defined by
/// the DLPack protocol, see https://dmlc.github.io/dlpack/latest/.
///
/// Data types for which the protocol is supported are
/// primitive data types without NullType, BooleanType and
/// Decimal types.
///
/// DLPack protocol only supports arrays with one contiguous
/// memory region which means Arrow Arrays with validity buffers
/// are not supported.
///
/// \param[in] arr Arrow array
/// \param[out] out DLManagedTensor struct
/// \return Status
ARROW_EXPORT
Status ExportArray(const std::shared_ptr<Array>& arr, DLManagedTensor** out);

}  // namespace dlpack

}  // namespace arrow
