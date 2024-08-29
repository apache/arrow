/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow/gpu/cuda_api.h>

#include <arrow-cuda-glib/cuda.h>

GArrowCUDAContext *
garrow_cuda_context_new_raw(std::shared_ptr<arrow::cuda::CudaContext> *arrow_context);
std::shared_ptr<arrow::cuda::CudaContext>
garrow_cuda_context_get_raw(GArrowCUDAContext *context);

GArrowCUDAIPCMemoryHandle *
garrow_cuda_ipc_memory_handle_new_raw(std::shared_ptr<arrow::cuda::CudaIpcMemHandle> *arrow_handle);
std::shared_ptr<arrow::cuda::CudaIpcMemHandle>
garrow_cuda_ipc_memory_handle_get_raw(GArrowCUDAIPCMemoryHandle *handle);

GArrowCUDABuffer *
garrow_cuda_buffer_new_raw(std::shared_ptr<arrow::cuda::CudaBuffer> *arrow_buffer);
std::shared_ptr<arrow::cuda::CudaBuffer>
garrow_cuda_buffer_get_raw(GArrowCUDABuffer *buffer);

GArrowCUDAHostBuffer *
garrow_cuda_host_buffer_new_raw(std::shared_ptr<arrow::cuda::CudaHostBuffer> *arrow_buffer);
std::shared_ptr<arrow::cuda::CudaHostBuffer>
garrow_cuda_host_buffer_get_raw(GArrowCUDAHostBuffer *buffer);

GArrowCUDABufferInputStream *
garrow_cuda_buffer_input_stream_new_raw(std::shared_ptr<arrow::cuda::CudaBufferReader> *arrow_reader);
std::shared_ptr<arrow::cuda::CudaBufferReader>
garrow_cuda_buffer_input_stream_get_raw(GArrowCUDABufferInputStream *input_stream);

GArrowCUDABufferOutputStream *
garrow_cuda_buffer_output_stream_new_raw(std::shared_ptr<arrow::cuda::CudaBufferWriter> *arrow_writer);
std::shared_ptr<arrow::cuda::CudaBufferWriter>
garrow_cuda_buffer_output_stream_get_raw(GArrowCUDABufferOutputStream *output_stream);
