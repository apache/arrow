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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS

#define GARROW_GPU_TYPE_CUDA_DEVICE_MANAGER     \
  (garrow_gpu_cuda_device_manager_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDADeviceManager,
                         garrow_gpu_cuda_device_manager,
                         GARROW_GPU,
                         CUDA_DEVICE_MANAGER,
                         GObject)
struct _GArrowGPUCUDADeviceManagerClass
{
  GObjectClass parent_class;
};

#define GARROW_GPU_TYPE_CUDA_CONTEXT (garrow_gpu_cuda_context_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDAContext,
                         garrow_gpu_cuda_context,
                         GARROW_GPU,
                         CUDA_CONTEXT,
                         GObject)
struct _GArrowGPUCUDAContextClass
{
  GObjectClass parent_class;
};

#define GARROW_GPU_TYPE_CUDA_BUFFER (garrow_gpu_cuda_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDABuffer,
                         garrow_gpu_cuda_buffer,
                         GARROW_GPU,
                         CUDA_BUFFER,
                         GArrowBuffer)
struct _GArrowGPUCUDABufferClass
{
  GArrowBufferClass parent_class;
};

#define GARROW_GPU_TYPE_CUDA_HOST_BUFFER (garrow_gpu_cuda_host_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDAHostBuffer,
                         garrow_gpu_cuda_host_buffer,
                         GARROW_GPU,
                         CUDA_HOST_BUFFER,
                         GArrowMutableBuffer)
struct _GArrowGPUCUDAHostBufferClass
{
  GArrowMutableBufferClass parent_class;
};

#define GARROW_GPU_TYPE_CUDA_IPC_MEMORY_HANDLE          \
  (garrow_gpu_cuda_ipc_memory_handle_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDAIPCMemoryHandle,
                         garrow_gpu_cuda_ipc_memory_handle,
                         GARROW_GPU,
                         CUDA_IPC_MEMORY_HANDLE,
                         GObject)
struct _GArrowGPUCUDAIPCMemoryHandleClass
{
  GObjectClass parent_class;
};

#define GARROW_GPU_TYPE_CUDA_BUFFER_INPUT_STREAM        \
  (garrow_gpu_cuda_buffer_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDABufferInputStream,
                         garrow_gpu_cuda_buffer_input_stream,
                         GARROW_GPU,
                         CUDA_BUFFER_INPUT_STREAM,
                         GArrowBufferInputStream)
struct _GArrowGPUCUDABufferInputStreamClass
{
  GArrowBufferInputStreamClass parent_class;
};

#define GARROW_GPU_TYPE_CUDA_BUFFER_OUTPUT_STREAM               \
  (garrow_gpu_cuda_buffer_output_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGPUCUDABufferOutputStream,
                         garrow_gpu_cuda_buffer_output_stream,
                         GARROW_GPU,
                         CUDA_BUFFER_OUTPUT_STREAM,
                         GArrowOutputStream)
struct _GArrowGPUCUDABufferOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};

GArrowGPUCUDADeviceManager *
garrow_gpu_cuda_device_manager_new(GError **error);

GArrowGPUCUDAContext *
garrow_gpu_cuda_device_manager_get_context(GArrowGPUCUDADeviceManager *manager,
                                           gint gpu_number,
                                           GError **error);
gsize
garrow_gpu_cuda_device_manager_get_n_devices(GArrowGPUCUDADeviceManager *manager);

gint64
garrow_gpu_cuda_context_get_allocated_size(GArrowGPUCUDAContext *context);


GArrowGPUCUDABuffer *
garrow_gpu_cuda_buffer_new(GArrowGPUCUDAContext *context,
                           gint64 size,
                           GError **error);
GArrowGPUCUDABuffer *
garrow_gpu_cuda_buffer_new_ipc(GArrowGPUCUDAContext *context,
                               GArrowGPUCUDAIPCMemoryHandle *handle,
                               GError **error);
GArrowGPUCUDABuffer *
garrow_gpu_cuda_buffer_new_record_batch(GArrowGPUCUDAContext *context,
                                        GArrowRecordBatch *record_batch,
                                        GError **error);
GBytes *
garrow_gpu_cuda_buffer_copy_to_host(GArrowGPUCUDABuffer *buffer,
                                    gint64 position,
                                    gint64 size,
                                    GError **error);
gboolean
garrow_gpu_cuda_buffer_copy_from_host(GArrowGPUCUDABuffer *buffer,
                                      const guint8 *data,
                                      gint64 size,
                                      GError **error);
GArrowGPUCUDAIPCMemoryHandle *
garrow_gpu_cuda_buffer_export(GArrowGPUCUDABuffer *buffer,
                              GError **error);
GArrowGPUCUDAContext *
garrow_gpu_cuda_buffer_get_context(GArrowGPUCUDABuffer *buffer);
GArrowRecordBatch *
garrow_gpu_cuda_buffer_read_record_batch(GArrowGPUCUDABuffer *buffer,
                                         GArrowSchema *schema,
                                         GError **error);


GArrowGPUCUDAHostBuffer *
garrow_gpu_cuda_host_buffer_new(gint gpu_number,
                                gint64 size,
                                GError **error);

GArrowGPUCUDAIPCMemoryHandle *
garrow_gpu_cuda_ipc_memory_handle_new(const guint8 *data,
                                      gsize size,
                                      GError **error);

GArrowBuffer *
garrow_gpu_cuda_ipc_memory_handle_serialize(GArrowGPUCUDAIPCMemoryHandle *handle,
                                            GError **error);

GArrowGPUCUDABufferInputStream *
garrow_gpu_cuda_buffer_input_stream_new(GArrowGPUCUDABuffer *buffer);

GArrowGPUCUDABufferOutputStream *
garrow_gpu_cuda_buffer_output_stream_new(GArrowGPUCUDABuffer *buffer);

gboolean
garrow_gpu_cuda_buffer_output_stream_set_buffer_size(GArrowGPUCUDABufferOutputStream *stream,
                                                     gint64 size,
                                                     GError **error);
gint64
garrow_gpu_cuda_buffer_output_stream_get_buffer_size(GArrowGPUCUDABufferOutputStream *stream);
gint64
garrow_gpu_cuda_buffer_output_stream_get_buffered_size(GArrowGPUCUDABufferOutputStream *stream);

G_END_DECLS
