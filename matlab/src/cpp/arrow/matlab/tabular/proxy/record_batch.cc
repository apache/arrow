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
#include "arrow/matlab/array/proxy/wrap.h"

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/matlab/tabular/get_row_as_string.h"
#include "arrow/type.h"
#include "arrow/util/utf8.h"

#include "libmexclass/proxy/ProxyManager.h"
#include "libmexclass/error/Error.h"

#include <sstream>

namespace arrow::matlab::tabular::proxy {

    namespace {
        libmexclass::error::Error makeEmptyRecordBatchError() {
            const std::string error_msg =  "Numeric indexing using the column method is not supported for record batches with no columns.";
            return libmexclass::error::Error{error::RECORD_BATCH_NUMERIC_INDEX_WITH_EMPTY_RECORD_BATCH, error_msg};
        }

        libmexclass::error::Error makeInvalidNumericIndexError(const int32_t matlab_index, const int32_t num_columns) {
            std::stringstream error_message_stream;
            error_message_stream << "Invalid column index: ";
            error_message_stream << matlab_index;
            error_message_stream << ". Column index must be between 1 and the number of columns (";
            error_message_stream << num_columns;
            error_message_stream << ").";
            return libmexclass::error::Error{error::RECORD_BATCH_INVALID_NUMERIC_COLUMN_INDEX, error_message_stream.str()};
        }
    }

    RecordBatch::RecordBatch(std::shared_ptr<arrow::RecordBatch> record_batch) : record_batch{record_batch} {
        REGISTER_METHOD(RecordBatch, toString);
        REGISTER_METHOD(RecordBatch, getNumRows);
        REGISTER_METHOD(RecordBatch, getNumColumns);
        REGISTER_METHOD(RecordBatch, getColumnNames);
        REGISTER_METHOD(RecordBatch, getColumnByIndex);
        REGISTER_METHOD(RecordBatch, getColumnByName);
        REGISTER_METHOD(RecordBatch, getSchema);
        REGISTER_METHOD(RecordBatch, getRowAsString);
    }

    std::shared_ptr<arrow::RecordBatch> RecordBatch::unwrap() {
        return record_batch;
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
            auto arrow_array = arrow_array_proxy->unwrap();
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

    void RecordBatch::getNumRows(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const auto num_rows = record_batch->num_rows();
        auto num_rows_mda = factory.createScalar(num_rows);
        context.outputs[0] = num_rows_mda;
    }

    void RecordBatch::getNumColumns(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const auto num_columns = record_batch->num_columns();
        auto num_columns_mda = factory.createScalar(num_columns);
        context.outputs[0] = num_columns_mda;
    }

    void RecordBatch::getColumnNames(libmexclass::proxy::method::Context& context) {
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

    void RecordBatch::getColumnByIndex(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int32_t> index_mda = args[0]["Index"];
        const auto matlab_index = int32_t(index_mda[0]);
        
        // Note: MATLAB uses 1-based indexing, so subtract 1.
        // arrow::Schema::field does not do any bounds checking.
        const int32_t index = matlab_index - 1;
        const auto num_columns = record_batch->num_columns();
        
        if (num_columns == 0) {
            context.error = makeEmptyRecordBatchError();
            return;
        }
        
        if (matlab_index < 1 || matlab_index > num_columns) {
            context.error = makeInvalidNumericIndexError(matlab_index, num_columns);
            return;
        }

        const auto array = record_batch->column(index);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto array_proxy,
                                            arrow::matlab::array::proxy::wrap(array),
                                            context,
                                            error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);
        
        
        const auto array_proxy_id = ProxyManager::manageProxy(array_proxy);
        const auto array_proxy_id_mda = factory.createScalar(array_proxy_id);
        const auto array_type_id_mda = factory.createScalar(static_cast<int32_t>(array->type_id()));
        
        context.outputs[0] = array_proxy_id_mda;
        context.outputs[1] = array_type_id_mda;
    }

    void RecordBatch::getColumnByName(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::StringArray name_mda = args[0]["Name"];
        const auto name_utf16 = std::u16string(name_mda[0]);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto name, arrow::util::UTF16StringToUTF8(name_utf16), context, error::UNICODE_CONVERSION_ERROR_ID);

        const std::vector<std::string> names = {name};
        const auto& schema = record_batch->schema();
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(schema->CanReferenceFieldsByNames(names), context, error::ARROW_TABULAR_SCHEMA_AMBIGUOUS_FIELD_NAME);

        const auto array = record_batch->GetColumnByName(name);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto array_proxy,
                                            arrow::matlab::array::proxy::wrap(array),
                                            context,
                                            error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);

        const auto array_proxy_id = ProxyManager::manageProxy(array_proxy);
        const auto array_proxy_id_mda = factory.createScalar(array_proxy_id);
        const auto array_type_id_mda = factory.createScalar(static_cast<int32_t>(array->type_id()));

        context.outputs[0] = array_proxy_id_mda;
        context.outputs[1] = array_type_id_mda;
    }

    void RecordBatch::getSchema(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        using SchemaProxy = arrow::matlab::tabular::proxy::Schema;
        mda::ArrayFactory factory;

        const auto schema = record_batch->schema();
        const auto schema_proxy = std::make_shared<SchemaProxy>(std::move(schema));
        const auto schema_proxy_id = ProxyManager::manageProxy(schema_proxy);
        const auto schema_proxy_id_mda = factory.createScalar(schema_proxy_id);

        context.outputs[0] = schema_proxy_id_mda;
    }

    void RecordBatch::getRowAsString(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int64_t> index_mda = args[0]["Index"];
        const auto matlab_row_index = int64_t(index_mda[0]);

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto row_str_utf8, arrow::matlab::tabular::get_row_as_string(record_batch, matlab_row_index), 
                                            context, error::TABULAR_GET_ROW_AS_STRING_FAILED);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto row_str_utf16, arrow::util::UTF8StringToUTF16(row_str_utf8),
                                            context, error::UNICODE_CONVERSION_ERROR_ID);
        context.outputs[0] = factory.createScalar(row_str_utf16);
    }

}
