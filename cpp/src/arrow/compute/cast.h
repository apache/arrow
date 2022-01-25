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

#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class ExecContext;

/// \addtogroup compute-concrete-options
/// @{

class ARROW_EXPORT CastOptions : public FunctionOptions {
 public:
  explicit CastOptions(bool safe = true);

  static constexpr char const kTypeName[] = "CastOptions";
  static CastOptions Safe(std::shared_ptr<DataType> to_type = NULLPTR) {
    CastOptions safe(true);
    safe.to_type = std::move(to_type);
    return safe;
  }

  static CastOptions Unsafe(std::shared_ptr<DataType> to_type = NULLPTR) {
    CastOptions unsafe(false);
    unsafe.to_type = std::move(to_type);
    return unsafe;
  }

  // Type being casted to. May be passed separate to eager function
  // compute::Cast
  std::shared_ptr<DataType> to_type;

  bool allow_int_overflow;
  bool allow_time_truncate;
  bool allow_time_overflow;
  bool allow_decimal_truncate;
  bool allow_float_truncate;
  // Indicate if conversions from Binary/FixedSizeBinary to string must
  // validate the utf8 payload.
  bool allow_invalid_utf8;
};

/// @}

// Cast functions are _not_ registered in the FunctionRegistry, though they use
// the same execution machinery
class CastFunction : public ScalarFunction {
 public:
  CastFunction(std::string name, Type::type out_type_id);

  Type::type out_type_id() const { return out_type_id_; }
  const std::vector<Type::type>& in_type_ids() const { return in_type_ids_; }

  Status AddKernel(Type::type in_type_id, std::vector<InputType> in_types,
                   OutputType out_type, ArrayKernelExec exec,
                   NullHandling::type = NullHandling::INTERSECTION,
                   MemAllocation::type = MemAllocation::PREALLOCATE);

  // Note, this function toggles off memory allocation and sets the init
  // function to CastInit
  Status AddKernel(Type::type in_type_id, ScalarKernel kernel);

  Result<const Kernel*> DispatchExact(
      const std::vector<ValueDescr>& values) const override;

 private:
  std::vector<Type::type> in_type_ids_;
  const Type::type out_type_id_;
};

ARROW_EXPORT
Result<std::shared_ptr<CastFunction>> GetCastFunction(
    const std::shared_ptr<DataType>& to_type);

/// \brief Return true if a cast function is defined
ARROW_EXPORT
bool CanCast(const DataType& from_type, const DataType& to_type);

// ----------------------------------------------------------------------
// Convenience invocation APIs for a number of kernels

/// \brief Cast from one array type to another
/// \param[in] value array to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[in] ctx the function execution context, optional
/// \return the resulting array
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<std::shared_ptr<Array>> Cast(const Array& value, std::shared_ptr<DataType> to_type,
                                    const CastOptions& options = CastOptions::Safe(),
                                    ExecContext* ctx = NULLPTR);

/// \brief Cast from one array type to another
/// \param[in] value array to cast
/// \param[in] options casting options. The "to_type" field must be populated
/// \param[in] ctx the function execution context, optional
/// \return the resulting array
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Cast(const Datum& value, const CastOptions& options,
                   ExecContext* ctx = NULLPTR);

/// \brief Cast from one value to another
/// \param[in] value datum to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Cast(const Datum& value, std::shared_ptr<DataType> to_type,
                   const CastOptions& options = CastOptions::Safe(),
                   ExecContext* ctx = NULLPTR);

/// \brief Cast several values simultaneously. Safe cast options are used.
/// \param[in] values datums to cast
/// \param[in] descrs ValueDescrs to cast to
/// \param[in] ctx the function execution context, optional
/// \return the resulting datums
///
/// \since 4.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<std::vector<Datum>> Cast(std::vector<Datum> values, std::vector<ValueDescr> descrs,
                                ExecContext* ctx = NULLPTR);

}  // namespace compute
}  // namespace arrow
