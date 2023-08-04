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

#include "arrow/matlab/io/feather/proxy/feather_writer.h"
#include "arrow/matlab/error/error.h"

#include "arrow/result.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::io::feather::proxy {

    FeatherWriter::FeatherWriter(const std::string& filename) : filename{filename} {}

    libmexclass::proxy::MakeResult FeatherWriter::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        mda::StructArray opts = constructor_arguments[0];
        const mda::StringArray filename_mda = opts[0]["Filename"];

        const auto filename_utf16 = std::u16string(filename_mda[0]);
        MATLAB_ASSIGN_OR_ERROR(const auto column_name_utf8,
                               arrow::util::UTF16StringToUTF8(filename_utf16),
                               error::UNICODE_CONVERSION_ERROR_ID);
        
        return libmexclass::error::Error{"arrow:NotImplemented", "Not implemented"};
    }
}
