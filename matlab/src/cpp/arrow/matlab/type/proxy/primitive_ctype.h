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

#include "arrow/matlab/type/proxy/fixed_width_type.h"
#include "arrow/type_traits.h"

#include <type_traits>


namespace arrow::matlab::type::proxy {

template <typename CType>
using arrow_type_t = typename arrow::CTypeTraits<CType>::ArrowType;

template <typename CType>
using is_primitive = arrow::is_primitive_ctype<arrow_type_t<CType>>;

template<typename CType>
using enable_if_primitive = std::enable_if_t<is_primitive<CType>::value, bool>; 

template<typename CType, enable_if_primitive<CType> = true>
class PrimitiveCType : public arrow::matlab::type::proxy::FixedWidthType {
    
    using ArrowDataType = arrow_type_t<CType>;
    
    public:
        PrimitiveCType(std::shared_ptr<ArrowDataType> primitive_type) : arrow::matlab::type::proxy::FixedWidthType(std::move(primitive_type)) {
        }

        ~PrimitiveCType() {}

        static libmexclass::proxy::MakeResult make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            auto data_type = arrow::CTypeTraits<CType>::type_singleton();
            return std::make_shared<PrimitiveCType>(std::static_pointer_cast<ArrowDataType>(std::move(data_type)));
        }
};

}

