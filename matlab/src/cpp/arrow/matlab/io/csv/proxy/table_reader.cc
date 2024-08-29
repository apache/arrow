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

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/io/csv/proxy/table_reader.h"
#include "arrow/matlab/tabular/proxy/table.h"

#include "arrow/util/utf8.h"

#include "arrow/result.h"

#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/csv/reader.h"
#include "arrow/table.h"

namespace arrow::matlab::io::csv::proxy {

    TableReader::TableReader(const std::string& filename) : filename{filename} {
        REGISTER_METHOD(TableReader, read);
        REGISTER_METHOD(TableReader, getFilename);
    }

    libmexclass::proxy::MakeResult TableReader::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using TableReaderProxy = arrow::matlab::io::csv::proxy::TableReader;

        mda::StructArray args = constructor_arguments[0];
        const mda::StringArray filename_utf16_mda = args[0]["Filename"];
        const auto filename_utf16 = std::u16string(filename_utf16_mda[0]);
        MATLAB_ASSIGN_OR_ERROR(const auto filename, arrow::util::UTF16StringToUTF8(filename_utf16), error::UNICODE_CONVERSION_ERROR_ID);

        return std::make_shared<TableReaderProxy>(filename);
    }

    void TableReader::read(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        namespace csv = ::arrow::csv;
        using TableProxy = arrow::matlab::tabular::proxy::Table;

        mda::ArrayFactory factory;

        // Create a file input stream.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto source, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()), context, error::FAILED_TO_OPEN_FILE_FOR_READ);

        const ::arrow::io::IOContext io_context;
        const auto read_options = csv::ReadOptions::Defaults();
        const auto parse_options = csv::ParseOptions::Defaults();
        const auto convert_options = csv::ConvertOptions::Defaults();

        // Create a TableReader from the file input stream.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto table_reader,
                                            csv::TableReader::Make(io_context, source, read_options, parse_options, convert_options),
                                            context,
                                            error::CSV_FAILED_TO_CREATE_TABLE_READER);

        // Read a Table from the file.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto table, table_reader->Read(), context, error::CSV_FAILED_TO_READ_TABLE);

        auto table_proxy = std::make_shared<TableProxy>(table);
        const auto table_proxy_id = ProxyManager::manageProxy(table_proxy);

        const auto table_proxy_id_mda = factory.createScalar(table_proxy_id);

        context.outputs[0] = table_proxy_id_mda;
    }

    void TableReader::getFilename(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto filename_utf16, arrow::util::UTF8StringToUTF16(filename), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto filename_utf16_mda = factory.createScalar(filename_utf16);
        context.outputs[0] = filename_utf16_mda;
    }

}
