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

#include "arrow/factory.h"

#include <memory>

#include "arrow/type.h"

namespace arrow {

#define TYPE_FACTORY(NAME, KLASS)                                       \
  std::shared_ptr<DataType> NAME() {                                    \
    static std::shared_ptr<DataType> result = std::make_shared<KLASS>(); \
    return result;                                                      \
  }

TYPE_FACTORY(int8, Int8Type);
TYPE_FACTORY(uint8, UInt8Type);
TYPE_FACTORY(int16, Int16Type);
TYPE_FACTORY(uint16, UInt16Type);
TYPE_FACTORY(int32, Int32Type);
TYPE_FACTORY(uint32, UInt32Type);
TYPE_FACTORY(int64, Int64Type);
TYPE_FACTORY(uint64, UInt64Type);
TYPE_FACTORY(float32, FloatType);
TYPE_FACTORY(float64, DoubleType);
TYPE_FACTORY(utf8, StringType);
TYPE_FACTORY(binary, BinaryType);

}  // namespace arrow
