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

#include "arrow/types/json.h"

#include <vector>

#include "arrow/type.h"
#include "arrow/types/union.h"

namespace arrow {

static const TypePtr Null(new NullType());
static const TypePtr Int32(new Int32Type());
static const TypePtr String(new StringType());
static const TypePtr Double(new DoubleType());
static const TypePtr Bool(new BooleanType());

static const std::vector<TypePtr> json_types = {Null, Int32, String,
                                                Double, Bool};
TypePtr JSONScalar::dense_type = TypePtr(new DenseUnionType(json_types));
TypePtr JSONScalar::sparse_type = TypePtr(new SparseUnionType(json_types));

} // namespace arrow
