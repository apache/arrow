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

#include <arrow-gpu-glib/cuda.h>

GArrowGPUCUDAContext *
garrow_gpu_cuda_context_new_raw(std::shared_ptr<arrow::gpu::CudaContext> *arrow_context);
std::shared_ptr<arrow::gpu::CudaContext>
garrow_gpu_cuda_context_get_raw(GArrowGPUCUDAContext *context);

GArrowGPUCUDAIPCMemoryHandle *
garrow_gpu_cuda_ipc_memory_handle_new_raw(arrow::gpu::CudaIpcMemHandle *arrow_handle);
arrow::gpu::CudaIpcMemHandle *
garrow_gpu_cuda_ipc_memory_handle_get_raw(GArrowGPUCUDAIPCMemoryHandle *handle);

GArrowGPUCUDABuffer *
garrow_gpu_cuda_buffer_new_raw(std::shared_ptr<arrow::gpu::CudaBuffer> *arrow_buffer);
std::shared_ptr<arrow::gpu::CudaBuffer>
garrow_gpu_cuda_buffer_get_raw(GArrowGPUCUDABuffer *buffer);

GArrowGPUCUDAHostBuffer *
garrow_gpu_cuda_host_buffer_new_raw(std::shared_ptr<arrow::gpu::CudaHostBuffer> *arrow_buffer);
std::shared_ptr<arrow::gpu::CudaHostBuffer>
garrow_gpu_cuda_host_buffer_get_raw(GArrowGPUCUDAHostBuffer *buffer);

GArrowGPUCUDABufferInputStream *
garrow_gpu_cuda_buffer_input_stream_new_raw(std::shared_ptr<arrow::gpu::CudaBufferReader> *arrow_reader);
std::shared_ptr<arrow::gpu::CudaBufferReader>
garrow_gpu_cuda_buffer_input_stream_get_raw(GArrowGPUCUDABufferInputStream *input_stream);

GArrowGPUCUDABufferOutputStream *
garrow_gpu_cuda_buffer_output_stream_new_raw(std::shared_ptr<arrow::gpu::CudaBufferWriter> *arrow_writer);
std::shared_ptr<arrow::gpu::CudaBufferWriter>
garrow_gpu_cuda_buffer_output_stream_get_raw(GArrowGPUCUDABufferOutputStream *output_stream);
