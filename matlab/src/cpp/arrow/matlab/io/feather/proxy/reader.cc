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
#include "arrow/matlab/io/feather/proxy/reader.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"

#include "arrow/util/utf8.h"

#include "arrow/result.h"

#include "arrow/io/file.h"
#include "arrow/ipc/feather.h"
#include "arrow/table.h"

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
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        using RecordBatchProxy = arrow::matlab::tabular::proxy::RecordBatch;

        mda::ArrayFactory factory;

        // Create a file input stream.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto source, arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()), context, error::FAILED_TO_OPEN_FILE_FOR_READ);

        // Create a Reader from the file input stream.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto reader, arrow::ipc::feather::Reader::Open(source), context, error::FEATHER_FAILED_TO_CREATE_READER);

        // Error if not Feather V1.
        const auto version = reader->version();
        if (version == ipc::feather::kFeatherV2Version) {
            MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(Status::NotImplemented("Support for Feather V2 has not been implemented."), context, error::FEATHER_VERSION_2);
        } else if (version != ipc::feather::kFeatherV1Version) {
            MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(Status::Invalid("Unknown Feather format version."), context, error::FEATHER_VERSION_UNKNOWN);
        }

        // Read a Table from the file.
        std::shared_ptr<arrow::Table> table = nullptr;
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(reader->Read(&table), context, error::FEATHER_FAILED_TO_READ_TABLE);

        // Combine all the chunks of the Table into a single RecordBatch.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto record_batch, table->CombineChunksToBatch(arrow::default_memory_pool()), context, error::FEATHER_FAILED_TO_READ_RECORD_BATCH);

        // Create a Proxy from the first RecordBatch.
        auto record_batch_proxy = std::make_shared<RecordBatchProxy>(record_batch);
        const auto record_batch_proxy_id = ProxyManager::manageProxy(record_batch_proxy);

        const auto record_batch_proxy_id_mda = factory.createScalar(record_batch_proxy_id);

        context.outputs[0] = record_batch_proxy_id_mda;
    }

    void Reader::getFilename(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto filename_utf16, arrow::util::UTF8StringToUTF16(filename), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto filename_utf16_mda = factory.createScalar(filename_utf16);
        context.outputs[0] = filename_utf16_mda;
    }

}
