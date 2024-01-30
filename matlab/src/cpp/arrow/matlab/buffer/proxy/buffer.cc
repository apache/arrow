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


#include "arrow/matlab/buffer/proxy/buffer.h"
#include "arrow/matlab/buffer/matlab_buffer.h"
#include "arrow/matlab/error/error.h"
#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::buffer::proxy {

    Buffer::Buffer(std::shared_ptr<arrow::Buffer> buffer) : buffer{std::move(buffer)} {
        REGISTER_METHOD(Buffer, getNumBytes);
        REGISTER_METHOD(Buffer, isEqual);
        REGISTER_METHOD(Buffer, toMATLAB);
    }

    libmexclass::proxy::MakeResult Buffer::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using BufferProxy = proxy::Buffer;
        using MatlabBuffer = arrow::matlab::buffer::MatlabBuffer;

        mda::StructArray opts = constructor_arguments[0];
        const mda::TypedArray<uint8_t> values_mda = opts[0]["Values"];
        auto buffer = std::make_shared<MatlabBuffer>(values_mda);
        return std::make_shared<BufferProxy>(std::move(buffer));
    }

    std::shared_ptr<arrow::Buffer> Buffer::unwrap() {
        return buffer;
    }

    void Buffer::getNumBytes(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;        
        auto num_bytes_mda = factory.createScalar(buffer->size());
        context.outputs[0] = num_bytes_mda; 
    }

    void Buffer::isEqual(libmexclass::proxy::method::Context& context) { 
        namespace mda = ::matlab::data;

        bool is_equal = true;
        const mda::TypedArray<uint64_t> buffer_proxy_ids = context.inputs[0];
        for (const auto& buffer_proxy_id : buffer_proxy_ids) {
            // Retrieve the Buffer proxy from the ProxyManager
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(buffer_proxy_id);
            auto buffer_proxy = std::static_pointer_cast<proxy::Buffer>(proxy);
            auto buffer_to_compare = buffer_proxy->unwrap();

            if (!buffer->Equals(*buffer_to_compare)) {
                is_equal = false;
                break;
            }
        }
        mda::ArrayFactory factory;
        context.outputs[0] = factory.createScalar(is_equal);
    }

    void Buffer::toMATLAB(libmexclass::proxy::method::Context& context) { 
        namespace mda = ::matlab::data;

        // If buffer->is_cpu() returns false, invoking buffer->data() may cause a crash.
        // Avoid this potential crash by first invoking ViewOrCopy(buffer, memory_manager_device).
        // This function tries to create a no-copy view of the buffer on the given memory
        // manager device. If not possible, then ViewOrCopy copies the buffer's contents.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(
            auto cpu_buffer, 
            arrow::Buffer::ViewOrCopy(buffer, arrow::default_cpu_memory_manager()),
            context, error::BUFFER_VIEW_OR_COPY_FAILED
        );

        const auto* data_begin = cpu_buffer->data();
        const auto num_bytes = cpu_buffer->size();
        // data_begin is a uint8_t*, so num_bytes is equal to the number of elements
        const auto* data_end = data_begin + num_bytes;

        mda::ArrayFactory factory;
        context.outputs[0] = factory.createArray<uint8_t>({static_cast<size_t>(num_bytes), 1}, data_begin, data_end);
    }

}