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

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow::internal {

/// \brief Percent-point / quantile function (PPF) of the normal distribution.
///
/// Given p in [0, 1], return the corresponding quantile value in the normal
/// distribution. This is the reciprocal of the cumulative distribution function.
///
/// If p is not in [0, 1], behavior is undefined.
///
/// This function is sometimes also called the probit function.
ARROW_EXPORT
double NormalPPF(double p);

}  // namespace arrow::internal
