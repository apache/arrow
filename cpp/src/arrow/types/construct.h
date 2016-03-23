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

#ifndef ARROW_TYPES_CONSTRUCT_H
#define ARROW_TYPES_CONSTRUCT_H

#include <cstdint>
#include <memory>

namespace arrow {

class Array;
class ArrayBuilder;
class Buffer;
struct DataType;
class MemoryPool;
class Status;

Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
    std::shared_ptr<ArrayBuilder>* out);

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type,
    int32_t length, const std::shared_ptr<Buffer>& data,
    int32_t null_count, const std::shared_ptr<Buffer>& nulls,
    std::shared_ptr<Array>* out);

} // namespace arrow

#endif // ARROW_BUILDER_H_
