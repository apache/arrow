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

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/io/feather/proxy/reader.h"

#include "arrow/util/utf8.h"

#include "arrow/result.h"

#include <iostream>

namespace arrow::matlab::io::feather::proxy {

    Reader::Reader(const std::string& filename) : filename{filename} {
        REGISTER_METHOD(Reader, read);
        REGISTER_METHOD(Reader, getFilename);
    }

    libmexclass::proxy::MakeResult Reader::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using ReaderProxy = arrow::matlab::io::feather::proxy::Reader;

        mda::StructArray args = constructor_arguments[0];
        const mda::StringArray filename_utf16_mda = args[0]["Filename"];
        const auto filename_utf16 = std::u16string(filename_utf16_mda[0]);
        MATLAB_ASSIGN_OR_ERROR(const auto filename, arrow::util::UTF16StringToUTF8(filename_utf16), error::UNICODE_CONVERSION_ERROR_ID);

        return std::make_shared<ReaderProxy>(filename);
    }

    void Reader::read(libmexclass::proxy::method::Context& context) {
        std::cout << "Test" << std::endl;
    }

    void Reader::getFilename(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

            MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto filename_utf16, arrow::util::UTF8StringToUTF16(filename), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto filename_utf16_mda = factory.createScalar(filename_utf16);
        context.outputs[0] = filename_utf16_mda;
    }

}
