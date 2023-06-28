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

#include "arrow/matlab/type/proxy/type.h"
#include "arrow/type_traits.h"

#include <type_traits>


namespace arrow::matlab::type::proxy {

template <typename T,  std::enable_if_t<ARROW::is_primitive_ctype<T>::value, bool>, true>
class PrimitiveCType : public arrow::matlab::type::proxy::Type {
    public:
        PrimitiveCType(const std::shared_ptr<T> primitive_type) {
            data_type = primitive_type;

            REGISTER_METHOD(PrimitiveCType, bitWidth);
        }

        ~PrimitiveCType() {}

        static libmexclass::proxy::MakeResult make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            auto data_type = typename arrow::TypeTraits<T>::type_singleton();
            return std::make_shared<PrimitiveCType>(data_type);
        }

    protected:
        void bitWidth(libmexclass::proxy::method::Context& context) {
            namespace mda = ::matlab::data;
            mda::ArrayFactory factory;
    
            auto bit_width_mda = factory.createScalar(data_type->bit_width());
            context.outputs[0] = bit_width_mda;
        }
};

}

