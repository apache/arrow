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

#include "libmexclass/proxy/ProxyManager.h"

#include "arrow/matlab/array/proxy/array.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::tabular::proxy {

    RecordBatch::RecordBatch(std::shared_ptr<arrow::RecordBatch> record_batch) : record_batch{record_batch} {
        REGISTER_METHOD(RecordBatch, toString);
        REGISTER_METHOD(RecordBatch, numColumns);
        REGISTER_METHOD(RecordBatch, columnNames);
    }

    void RecordBatch::toString(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto utf16_string, arrow::util::UTF8StringToUTF16(record_batch->ToString()), context, error::UNICODE_CONVERSION_ERROR_ID);
        mda::ArrayFactory factory;
        auto str_mda = factory.createScalar(utf16_string);
        context.outputs[0] = str_mda;
    }

    libmexclass::proxy::MakeResult RecordBatch::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        mda::StructArray opts = constructor_arguments[0];
        const mda::TypedArray<uint64_t> arrow_array_proxy_ids = opts[0]["ArrayProxyIDs"];
        const mda::StringArray column_names = opts[0]["ColumnNames"];

        std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
        // Retrieve all of the Arrow Array Proxy instances from the libmexclass ProxyManager.
        for (const auto& arrow_array_proxy_id : arrow_array_proxy_ids) {
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(arrow_array_proxy_id);
            auto arrow_array_proxy = std::static_pointer_cast<arrow::matlab::array::proxy::Array>(proxy);
            auto arrow_array = arrow_array_proxy->getArray();
            arrow_arrays.push_back(arrow_array);
        }

        std::vector<std::shared_ptr<Field>> fields;
        for (size_t i = 0; i < arrow_arrays.size(); ++i) {
            const auto type = arrow_arrays[i]->type();
            const auto column_name_utf16 = std::u16string(column_names[i]);
            MATLAB_ASSIGN_OR_ERROR(const auto column_name_utf8, arrow::util::UTF16StringToUTF8(column_name_utf16), error::UNICODE_CONVERSION_ERROR_ID);
            fields.push_back(std::make_shared<arrow::Field>(column_name_utf8, type));
        }

        arrow::SchemaBuilder schema_builder;
        MATLAB_ERROR_IF_NOT_OK(schema_builder.AddFields(fields), error::SCHEMA_BUILDER_ADD_FIELDS_ERROR_ID);
        MATLAB_ASSIGN_OR_ERROR(const auto schema, schema_builder.Finish(), error::SCHEMA_BUILDER_FINISH_ERROR_ID);
        const auto num_rows = arrow_arrays.size() == 0 ? 0 : arrow_arrays[0]->length();
        const auto record_batch = arrow::RecordBatch::Make(schema, num_rows, arrow_arrays);
        auto record_batch_proxy = std::make_shared<arrow::matlab::tabular::proxy::RecordBatch>(record_batch);

        return record_batch_proxy;
    }

    void RecordBatch::numColumns(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const auto num_columns = record_batch->num_columns();
        auto num_columns_mda = factory.createScalar(num_columns);
        context.outputs[0] = num_columns_mda;
    }

    void RecordBatch::columnNames(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const int num_columns = record_batch->num_columns();

        std::vector<mda::MATLABString> column_names;
        for (int i = 0; i < num_columns; ++i) {
            const auto column_name_utf8 = record_batch->column_name(i);
            MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto column_name_utf16, arrow::util::UTF8StringToUTF16(column_name_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
            const mda::MATLABString matlab_string = mda::MATLABString(std::move(column_name_utf16));
            column_names.push_back(matlab_string);
        }
        auto column_names_mda = factory.createArray({size_t{1}, static_cast<size_t>(num_columns)}, column_names.begin(), column_names.end());
        context.outputs[0] = column_names_mda;
    }

}
