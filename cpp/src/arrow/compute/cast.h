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

#ifndef ARROW_COMPUTE_CAST_H
#define ARROW_COMPUTE_CAST_H

#include <memory>

#include "arrow/array.h"
#include "arrow/util/visibility.h"

namespace arrow {

using internal::ArrayData;

namespace compute {

class FunctionContext;

struct CastOptions {
  bool allow_int_overflow;
};

typedef std::function<void(FunctionContext*, const CastOptions& options, const ArrayData&,
                           ArrayData*)>
    CastFunction;

class ARROW_EXPORT OpKernel {
 public:
  virtual ~OpKernel() = default;
};

class ARROW_EXPORT UnaryKernel : public OpKernel {
 public:
  virtual void operator()(FunctionContext* ctx, const ArrayData& input,
                          ArrayData* out) = 0;
};

class ARROW_EXPORT CastKernel : public UnaryKernel {
 public:
  CastKernel(const CastOptions& options, const CastFunction& func, bool is_zero_copy);
  void operator()(FunctionContext* ctx, const ArrayData& input, ArrayData* out) override;

 private:
  CastOptions options_;
  CastFunction func_;
  bool is_zero_copy_;
};

/// \since 0.7.0
/// \note API not yet finalized
ARROW_EXPORT
Status GetCastFunction(const DataType& in_type, const std::shared_ptr<DataType>& to_type,
                       const CastOptions& options, std::unique_ptr<CastKernel>* kernel);

/// \brief Cast from one array type to another
/// \param[in] context
/// \param[in] array
/// \param[in] to_type
/// \param[in] options
/// \param[out] out
///
/// \since 0.7.0
/// \note API not yet finalized
ARROW_EXPORT
Status Cast(FunctionContext* context, const Array& array,
            const std::shared_ptr<DataType>& to_type, const CastOptions& options,
            std::shared_ptr<Array>* out);

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_CAST_H
