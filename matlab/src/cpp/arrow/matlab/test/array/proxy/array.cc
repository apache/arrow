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

#include "arrow/matlab/test/array/proxy/array.h"

#include "arrow/builder.h"
#include "arrow/type_traits.h"

namespace arrow::matlab::test::array::proxy {

template <typename CType>
std::shared_ptr<arrow::Array> createNumericArray(const ::matlab::data::TypedArray<CType> numeric_mda) {
    using BuilderType = typename arrow::CTypeTraits<CType>::BuilderType;

    BuilderType builder;

    // Get raw pointer of mxArray
    auto it(numeric_mda.cbegin());
    auto dt = it.operator->();

    // TODO: throw an error
    auto st = builder.AppendValues(dt, numeric_mda.getNumberOfElements());
    if (st.ok()) {
        return nullptr;
    }
    
    auto maybe_array = builder.Finish();
    
    // TODO: throw an error
    if (!maybe_array.ok()) {
        return nullptr;
    }

    std::shared_ptr<arrow::Array> array = *maybe_array;
    return array;
}

std::shared_ptr<arrow::Array> createArray(const ::matlab::data::Array mda_array) {
    switch (mda_array.getType()) {
        case ::matlab::data::ArrayType::SINGLE:
            return createNumericArray<float>(::matlab::data::TypedArray<float>(mda_array));
        case ::matlab::data::ArrayType::DOUBLE:
            return createNumericArray<double>(::matlab::data::TypedArray<double>(mda_array));
        case ::matlab::data::ArrayType::UINT8:
            return createNumericArray<uint8_t>(::matlab::data::TypedArray<uint8_t>(mda_array));
        case ::matlab::data::ArrayType::UINT16:
            return createNumericArray<uint16_t>(::matlab::data::TypedArray<uint16_t>(mda_array));
        case ::matlab::data::ArrayType::UINT32:
            return createNumericArray<uint32_t>(::matlab::data::TypedArray<uint32_t>(mda_array));
        case ::matlab::data::ArrayType::UINT64:
            return createNumericArray<uint64_t>(::matlab::data::TypedArray<uint64_t>(mda_array));
        case ::matlab::data::ArrayType::INT8:
            return createNumericArray<int8_t>(::matlab::data::TypedArray<int8_t>(mda_array));
        case ::matlab::data::ArrayType::INT16:
            return createNumericArray<int16_t>(::matlab::data::TypedArray<int16_t>(mda_array));
        case ::matlab::data::ArrayType::INT32:
            return createNumericArray<int32_t>(::matlab::data::TypedArray<int32_t>(mda_array));
        case ::matlab::data::ArrayType::INT64:
            return createNumericArray<int64_t>(::matlab::data::TypedArray<int64_t>(mda_array));
        default:
            return nullptr;
    }
}

Array::Array(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
    const ::matlab::data::Array mda_array = constructor_arguments[0];
    array = createArray(mda_array);
}

}
