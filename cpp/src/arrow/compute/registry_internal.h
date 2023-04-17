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

namespace arrow {
namespace compute {

class FunctionRegistry;

namespace internal {

// Built-in scalar / elementwise functions
void RegisterScalarArithmetic(FunctionRegistry* registry);
void RegisterScalarBoolean(FunctionRegistry* registry);
void RegisterScalarCast(FunctionRegistry* registry);
void RegisterScalarComparison(FunctionRegistry* registry);
void RegisterScalarIfElse(FunctionRegistry* registry);
void RegisterScalarNested(FunctionRegistry* registry);
void RegisterScalarRandom(FunctionRegistry* registry);  // Nullary
void RegisterScalarRoundArithmetic(FunctionRegistry* registry);
void RegisterScalarSetLookup(FunctionRegistry* registry);
void RegisterScalarStringAscii(FunctionRegistry* registry);
void RegisterScalarStringUtf8(FunctionRegistry* registry);
void RegisterScalarTemporalBinary(FunctionRegistry* registry);
void RegisterScalarTemporalUnary(FunctionRegistry* registry);
void RegisterScalarValidity(FunctionRegistry* registry);

void RegisterScalarOptions(FunctionRegistry* registry);

// Vector functions
void RegisterVectorArraySort(FunctionRegistry* registry);
void RegisterVectorCumulativeSum(FunctionRegistry* registry);
void RegisterVectorHash(FunctionRegistry* registry);
void RegisterVectorNested(FunctionRegistry* registry);
void RegisterVectorRank(FunctionRegistry* registry);
void RegisterVectorReplace(FunctionRegistry* registry);
void RegisterVectorSelectK(FunctionRegistry* registry);
void RegisterVectorSelection(FunctionRegistry* registry);
void RegisterVectorSort(FunctionRegistry* registry);
void RegisterVectorRunEndEncode(FunctionRegistry* registry);
void RegisterVectorRunEndDecode(FunctionRegistry* registry);

void RegisterVectorOptions(FunctionRegistry* registry);

// Aggregate functions
void RegisterHashAggregateBasic(FunctionRegistry* registry);
void RegisterScalarAggregateBasic(FunctionRegistry* registry);
void RegisterScalarAggregateMode(FunctionRegistry* registry);
void RegisterScalarAggregateQuantile(FunctionRegistry* registry);
void RegisterScalarAggregateTDigest(FunctionRegistry* registry);
void RegisterScalarAggregateVariance(FunctionRegistry* registry);

void RegisterAggregateOptions(FunctionRegistry* registry);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
