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


#include "arrow/matlab/type/proxy/fixed_width_type.h"

namespace arrow::matlab::type::proxy {

    FixedWidthType::FixedWidthType(std::shared_ptr<arrow::FixedWidthType> type) : Type(std::move(type)) {
        REGISTER_METHOD(FixedWidthType, getBitWidth);
    }

    void FixedWidthType::getBitWidth(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
         mda::ArrayFactory factory;
     
         auto bit_width_mda = factory.createScalar(data_type->bit_width());
         context.outputs[0] = bit_width_mda;
    }
}
