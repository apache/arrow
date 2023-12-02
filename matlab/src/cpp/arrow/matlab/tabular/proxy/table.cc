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
#include "arrow/matlab/array/proxy/chunked_array.h"
#include "arrow/matlab/array/proxy/wrap.h"

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/tabular/proxy/table.h"
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/matlab/tabular/get_row_as_string.h"

#include "arrow/type.h"
#include "arrow/util/utf8.h"

#include "libmexclass/proxy/ProxyManager.h"
#include "libmexclass/error/Error.h"

namespace arrow::matlab::tabular::proxy {

    namespace {
        libmexclass::error::Error makeEmptyTableError() {
            const std::string error_msg =  "Numeric indexing using the column method is not supported for tables with no columns.";
            return libmexclass::error::Error{error::TABLE_NUMERIC_INDEX_WITH_EMPTY_TABLE, error_msg};
        }

        libmexclass::error::Error makeInvalidNumericIndexError(const int32_t matlab_index, const int32_t num_columns) {
            std::stringstream error_message_stream;
            error_message_stream << "Invalid column index: ";
            error_message_stream << matlab_index;
            error_message_stream << ". Column index must be between 1 and the number of columns (";
            error_message_stream << num_columns;
            error_message_stream << ").";
            return libmexclass::error::Error{error::TABLE_INVALID_NUMERIC_COLUMN_INDEX, error_message_stream.str()};
        }
    }

    Table::Table(std::shared_ptr<arrow::Table> table) : table{table} {
        REGISTER_METHOD(Table, toString);
        REGISTER_METHOD(Table, getNumRows);
        REGISTER_METHOD(Table, getNumColumns);
        REGISTER_METHOD(Table, getColumnNames);
        REGISTER_METHOD(Table, getSchema);
        REGISTER_METHOD(Table, getColumnByIndex);
        REGISTER_METHOD(Table, getColumnByName);
        REGISTER_METHOD(Table, getRowAsString);
    }

    std::shared_ptr<arrow::Table> Table::unwrap() {
        return table;
    }

    void Table::toString(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto utf16_string, arrow::util::UTF8StringToUTF16(table->ToString()), context, error::UNICODE_CONVERSION_ERROR_ID);
        mda::ArrayFactory factory;
        auto str_mda = factory.createScalar(utf16_string);
        context.outputs[0] = str_mda;
    }

    libmexclass::proxy::MakeResult Table::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        using ArrayProxy = arrow::matlab::array::proxy::Array;
        using TableProxy = arrow::matlab::tabular::proxy::Table;
        namespace mda = ::matlab::data;
        mda::StructArray opts = constructor_arguments[0];
        const mda::TypedArray<uint64_t> arrow_array_proxy_ids = opts[0]["ArrayProxyIDs"];
        const mda::StringArray column_names = opts[0]["ColumnNames"];

        std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
        // Retrieve all of the Arrow Array Proxy instances from the libmexclass ProxyManager.
        for (const auto& arrow_array_proxy_id : arrow_array_proxy_ids) {
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(arrow_array_proxy_id);
            auto arrow_array_proxy = std::static_pointer_cast<ArrayProxy>(proxy);
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
        const auto table = arrow::Table::Make(schema, arrow_arrays, num_rows);
        auto table_proxy = std::make_shared<TableProxy>(table);

        return table_proxy;
    }

    void Table::getNumRows(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const auto num_rows = table->num_rows();
        auto num_rows_mda = factory.createScalar(num_rows);
        context.outputs[0] = num_rows_mda;
    }

    void Table::getNumColumns(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const auto num_columns = table->num_columns();
        auto num_columns_mda = factory.createScalar(num_columns);
        context.outputs[0] = num_columns_mda;
    }

    void Table::getColumnNames(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        const int num_columns = table->num_columns();

        std::vector<mda::MATLABString> column_names;
        const auto schema = table->schema();
        const auto field_names = schema->field_names();
        for (int i = 0; i < num_columns; ++i) {
            const auto column_name_utf8 = field_names[i];
            MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto column_name_utf16, arrow::util::UTF8StringToUTF16(column_name_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
            const mda::MATLABString matlab_string = mda::MATLABString(std::move(column_name_utf16));
            column_names.push_back(matlab_string);
        }
        auto column_names_mda = factory.createArray({size_t{1}, static_cast<size_t>(num_columns)}, column_names.begin(), column_names.end());
        context.outputs[0] = column_names_mda;
    }

    void Table::getSchema(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        using SchemaProxy = arrow::matlab::tabular::proxy::Schema;
        mda::ArrayFactory factory;

        const auto schema = table->schema();
        const auto schema_proxy = std::make_shared<SchemaProxy>(std::move(schema));
        const auto schema_proxy_id = ProxyManager::manageProxy(schema_proxy);
        const auto schema_proxy_id_mda = factory.createScalar(schema_proxy_id);

        context.outputs[0] = schema_proxy_id_mda;
    }

    void Table::getColumnByIndex(libmexclass::proxy::method::Context& context) {
        using ChunkedArrayProxy = arrow::matlab::array::proxy::ChunkedArray;
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int32_t> index_mda = args[0]["Index"];
        const auto matlab_index = int32_t(index_mda[0]);

        // Note: MATLAB uses 1-based indexing, so subtract 1.
        // arrow::Schema::field does not do any bounds checking.
        const int32_t index = matlab_index - 1;
        const auto num_columns = table->num_columns();

        if (num_columns == 0) {
            context.error = makeEmptyTableError();
            return;
        }

        if (matlab_index < 1 || matlab_index > num_columns) {
            context.error = makeInvalidNumericIndexError(matlab_index, num_columns);
            return;
        }

        const auto chunked_array = table->column(index);
        const auto chunked_array_proxy = std::make_shared<ChunkedArrayProxy>(chunked_array);

        const auto chunked_array_proxy_id = ProxyManager::manageProxy(chunked_array_proxy);
        const auto chunked_array_proxy_id_mda = factory.createScalar(chunked_array_proxy_id);

        context.outputs[0] = chunked_array_proxy_id_mda;
    }

    void Table::getColumnByName(libmexclass::proxy::method::Context& context) {
        using ChunkedArrayProxy = arrow::matlab::array::proxy::ChunkedArray;
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::StringArray name_mda = args[0]["Name"];
        const auto name_utf16 = std::u16string(name_mda[0]);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto name, arrow::util::UTF16StringToUTF8(name_utf16), context, error::UNICODE_CONVERSION_ERROR_ID);

        const std::vector<std::string> names = {name};
        const auto& schema = table->schema();
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(schema->CanReferenceFieldsByNames(names), context, error::ARROW_TABULAR_SCHEMA_AMBIGUOUS_FIELD_NAME);

        const auto chunked_array = table->GetColumnByName(name);
        const auto chunked_array_proxy = std::make_shared<ChunkedArrayProxy>(chunked_array);

        const auto chunked_array_proxy_id = ProxyManager::manageProxy(chunked_array_proxy);
        const auto chunked_array_proxy_id_mda = factory.createScalar(chunked_array_proxy_id);

        context.outputs[0] = chunked_array_proxy_id_mda;
    }

    void Table::getRowAsString(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int64_t> index_mda = args[0]["Index"];
        const auto matlab_row_index = int64_t(index_mda[0]);

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto row_str_utf8, arrow::matlab::tabular::get_row_as_string(table, matlab_row_index), 
                                            context, error::TABULAR_GET_ROW_AS_STRING_FAILED);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto row_str_utf16, arrow::util::UTF8StringToUTF16(row_str_utf8),
                                            context, error::UNICODE_CONVERSION_ERROR_ID);
        context.outputs[0] = factory.createScalar(row_str_utf16);
    }

}
