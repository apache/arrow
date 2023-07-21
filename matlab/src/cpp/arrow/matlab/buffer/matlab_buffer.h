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

#include "arrow/buffer.h"

#include "MatlabDataArray.hpp"

namespace arrow::matlab::buffer {

    namespace mda = ::matlab::data;

    class MatlabBuffer : public arrow::Buffer {
    public:
    
        template<typename CType>
        MatlabBuffer(const mda::TypedArray<CType> typed_array)  
            : arrow::Buffer{nullptr, 0}
            , array{typed_array} {
                
                // Get raw pointer of mxArray
                auto it(typed_array.cbegin());
                auto dt = it.operator->();
            
                data_ = reinterpret_cast<const uint8_t*>(dt);
                size_ = sizeof(CType) * static_cast<int64_t>(typed_array.getNumberOfElements());
                capacity_ = size_;
                is_mutable_ = false;
            }
    private:
        const mda::Array array;
    };
}