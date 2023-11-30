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

#include "arrow/matlab/io/csv/proxy/table_writer.h"
#include "arrow/matlab/tabular/proxy/table.h"
#include "arrow/matlab/error/error.h"

#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/utf8.h"

#include "arrow/io/file.h"
#include "arrow/csv/writer.h"
#include "arrow/csv/options.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::io::csv::proxy {

    TableWriter::TableWriter(const std::string& filename) : filename{filename} {
        REGISTER_METHOD(TableWriter, getFilename);
        REGISTER_METHOD(TableWriter, write);
    }

    libmexclass::proxy::MakeResult TableWriter::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        mda::StructArray opts = constructor_arguments[0];
        const mda::StringArray filename_mda = opts[0]["Filename"];
        using TableWriterProxy = ::arrow::matlab::io::csv::proxy::TableWriter;

        const auto filename_utf16 = std::u16string(filename_mda[0]);
        MATLAB_ASSIGN_OR_ERROR(const auto filename_utf8,
                               arrow::util::UTF16StringToUTF8(filename_utf16),
                               error::UNICODE_CONVERSION_ERROR_ID);

        return std::make_shared<TableWriterProxy>(filename_utf8);
    }

    void TableWriter::getFilename(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto utf16_filename,
                                            arrow::util::UTF8StringToUTF16(filename),
                                            context,
                                            error::UNICODE_CONVERSION_ERROR_ID);
        mda::ArrayFactory factory;
        auto str_mda = factory.createScalar(utf16_filename);
        context.outputs[0] = str_mda;
    }

    void TableWriter::write(libmexclass::proxy::method::Context& context) {
        namespace csv = ::arrow::csv;
        namespace mda = ::matlab::data;
        using TableProxy = ::arrow::matlab::tabular::proxy::Table;

        mda::StructArray opts = context.inputs[0];
        const mda::TypedArray<uint64_t> table_proxy_id_mda = opts[0]["TableProxyID"];
        const uint64_t table_proxy_id = table_proxy_id_mda[0];

        auto proxy = libmexclass::proxy::ProxyManager::getProxy(table_proxy_id);
        auto table_proxy = std::static_pointer_cast<TableProxy>(proxy);
        auto table = table_proxy->unwrap();

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto output_stream,
                                            arrow::io::FileOutputStream::Open(filename),
                                            context,
                                            error::FAILED_TO_OPEN_FILE_FOR_WRITE);
        const auto options = csv::WriteOptions::Defaults();
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(csv::WriteCSV(*table, options, output_stream.get()),
                                            context,
                                            error::CSV_FAILED_TO_WRITE_TABLE);
    }
}
