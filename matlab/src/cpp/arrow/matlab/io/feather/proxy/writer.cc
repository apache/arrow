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

#include "arrow/matlab/io/feather/proxy/writer.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/matlab/error/error.h"

#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/utf8.h"

#include "arrow/io/file.h"
#include "arrow/ipc/feather.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::io::feather::proxy {

    Writer::Writer(const std::string& filename) : filename{filename} {
        REGISTER_METHOD(Writer, getFilename);
        REGISTER_METHOD(Writer, write);
    }

    libmexclass::proxy::MakeResult Writer::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        mda::StructArray opts = constructor_arguments[0];
        const mda::StringArray filename_mda = opts[0]["Filename"];

        const auto filename_utf16 = std::u16string(filename_mda[0]);
        MATLAB_ASSIGN_OR_ERROR(const auto filename_utf8,
                               arrow::util::UTF16StringToUTF8(filename_utf16),
                               error::UNICODE_CONVERSION_ERROR_ID);
        
        return std::make_shared<Writer>(filename_utf8);
    }

    void Writer::getFilename(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto utf16_filename,
                                            arrow::util::UTF8StringToUTF16(filename), 
                                            context,
                                            error::UNICODE_CONVERSION_ERROR_ID);
        mda::ArrayFactory factory;
        auto str_mda = factory.createScalar(utf16_filename);
        context.outputs[0] = str_mda;
    }

    void Writer::write(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::StructArray opts = context.inputs[0];
        const mda::TypedArray<uint64_t> record_batch_proxy_id_mda = opts[0]["RecordBatchProxyID"];
        const uint64_t record_batch_proxy_id = record_batch_proxy_id_mda[0]; 

        auto proxy = libmexclass::proxy::ProxyManager::getProxy(record_batch_proxy_id);
        auto record_batch_proxy = std::static_pointer_cast<arrow::matlab::tabular::proxy::RecordBatch>(proxy);
        auto record_batch = record_batch_proxy->unwrap();
        
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto table, 
                                            arrow::Table::FromRecordBatches({record_batch}),
                                            context,
                                            error::TABLE_FROM_RECORD_BATCH);

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(std::shared_ptr<arrow::io::OutputStream> output_stream,
                                            arrow::io::FileOutputStream::Open(filename),
                                            context,
                                            error::FAILED_TO_OPEN_FILE_FOR_WRITE);

         // Specify the feather file format version as V1
        arrow::ipc::feather::WriteProperties write_props;
        write_props.version = arrow::ipc::feather::kFeatherV1Version;

        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(ipc::feather::WriteTable(*table, output_stream.get(), write_props),
                                            context,
                                            error::FEATHER_FAILED_TO_WRITE_TABLE);
    }
}
