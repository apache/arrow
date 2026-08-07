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

#include "arrow/c/dlpack_abi.h"

#include <memory>

#include "arrow/array/array_base.h"

namespace arrow::dlpack {

/// \brief Export Arrow array as DLPack tensor.
///
/// DLMangedTensor is produced as defined by the DLPack protocol,
/// see https://dmlc.github.io/dlpack/latest/.
///
/// Data types for which the protocol is supported are
/// integer and floating-point data types.
///
/// DLPack protocol only supports arrays with one contiguous
/// memory region which means Arrow Arrays with validity buffers
/// are not supported.
///
/// \note Deprecated in DLPack 1.0. Use ExportArrayVersioned instead.
///
/// \param[in] arr Arrow array
/// \return DLManagedTensor struct
ARROW_EXPORT
Result<DLManagedTensor*> ExportArray(const std::shared_ptr<Array>& arr);

/// \brief Export Arrow array as a versioned DLPack tensor.
///
/// Same restrictions on data types as ExportArray, but produces the
/// DLManagedTensorVersioned structure introduced in DLPack 1.0.
///
/// The returned tensor is owned by the caller, who must release it by
/// calling its ``deleter``.
///
/// Arrow arrays are immutable, so the exported tensor is flagged with
/// DLPACK_FLAG_BITMASK_READ_ONLY unless a copy is made, in which case it is
/// flagged with DLPACK_FLAG_BITMASK_IS_COPIED and the consumer is free to
/// mutate it.
///
/// \param[in] arr Arrow array
/// \param[in] copy Whether to copy the data instead of sharing it with the array
/// \return DLManagedTensorVersioned struct
ARROW_EXPORT
Result<DLManagedTensorVersioned*> ExportArrayVersioned(const std::shared_ptr<Array>& arr,
                                                       bool copy);

/// \brief Export Arrow tensor as DLPack tensor.
///
/// \note Deprecated in DLPack 1.0. Use ExportTensorVersioned instead.
///
/// \param[in] t Arrow tensor
/// \return DLManagedTensor struct
ARROW_EXPORT
Result<DLManagedTensor*> ExportTensor(const std::shared_ptr<Tensor>& t);

/// \brief Export Arrow tensor as a versioned DLPack tensor.
///
/// Same as ExportTensor, but produces the DLManagedTensorVersioned structure
/// introduced in DLPack 1.0.
///
/// The returned tensor is owned by the caller, who must release it by
/// calling its ``deleter``.
///
/// When the data is shared with the Arrow tensor, the exported tensor is
/// flagged with DLPACK_FLAG_BITMASK_READ_ONLY if the Arrow tensor is not
/// mutable. When a copy is made, it is flagged with
/// DLPACK_FLAG_BITMASK_IS_COPIED and the consumer is free to mutate it.
///
/// \param[in] t Arrow tensor
/// \param[in] copy Whether to copy the data instead of sharing it with the tensor
/// \return DLManagedTensorVersioned struct
ARROW_EXPORT
Result<DLManagedTensorVersioned*> ExportTensorVersioned(const std::shared_ptr<Tensor>& t,
                                                        bool copy);

/// \brief Get DLDevice with enumerator specifying the
/// type of the device data is stored on and index of the
/// device which is 0 by default for CPU.
///
/// \param[in] arr Arrow array
/// \return DLDevice struct
ARROW_EXPORT
Result<DLDevice> ExportDevice(const std::shared_ptr<Array>& arr);

ARROW_EXPORT
Result<DLDevice> ExportDevice(const std::shared_ptr<Tensor>& t);

}  // namespace arrow::dlpack
