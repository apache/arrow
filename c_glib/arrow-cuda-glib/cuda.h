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

#define GARROW_CUDA_TYPE_DEVICE_MANAGER (garrow_cuda_device_manager_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDADeviceManager,
                         garrow_cuda_device_manager,
                         GARROW_CUDA,
                         DEVICE_MANAGER,
                         GObject)
struct _GArrowCUDADeviceManagerClass
{
  GObjectClass parent_class;
};

#define GARROW_CUDA_TYPE_CONTEXT (garrow_cuda_context_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDAContext,
                         garrow_cuda_context,
                         GARROW_CUDA,
                         CONTEXT,
                         GObject)
struct _GArrowCUDAContextClass
{
  GObjectClass parent_class;
};

#define GARROW_CUDA_TYPE_BUFFER (garrow_cuda_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDABuffer,
                         garrow_cuda_buffer,
                         GARROW_CUDA,
                         BUFFER,
                         GArrowBuffer)
struct _GArrowCUDABufferClass
{
  GArrowBufferClass parent_class;
};

#define GARROW_CUDA_TYPE_HOST_BUFFER (garrow_cuda_host_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDAHostBuffer,
                         garrow_cuda_host_buffer,
                         GARROW_CUDA,
                         HOST_BUFFER,
                         GArrowMutableBuffer)
struct _GArrowCUDAHostBufferClass
{
  GArrowMutableBufferClass parent_class;
};

#define GARROW_CUDA_TYPE_IPC_MEMORY_HANDLE      \
  (garrow_cuda_ipc_memory_handle_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDAIPCMemoryHandle,
                         garrow_cuda_ipc_memory_handle,
                         GARROW_CUDA,
                         IPC_MEMORY_HANDLE,
                         GObject)
struct _GArrowCUDAIPCMemoryHandleClass
{
  GObjectClass parent_class;
};

#define GARROW_CUDA_TYPE_BUFFER_INPUT_STREAM    \
  (garrow_cuda_buffer_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDABufferInputStream,
                         garrow_cuda_buffer_input_stream,
                         GARROW_CUDA,
                         BUFFER_INPUT_STREAM,
                         GArrowBufferInputStream)
struct _GArrowCUDABufferInputStreamClass
{
  GArrowBufferInputStreamClass parent_class;
};

#define GARROW_CUDA_TYPE_BUFFER_OUTPUT_STREAM   \
  (garrow_cuda_buffer_output_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCUDABufferOutputStream,
                         garrow_cuda_buffer_output_stream,
                         GARROW_CUDA,
                         BUFFER_OUTPUT_STREAM,
                         GArrowOutputStream)
struct _GArrowCUDABufferOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};

GArrowCUDADeviceManager *
garrow_cuda_device_manager_new(GError **error);

GArrowCUDAContext *
garrow_cuda_device_manager_get_context(GArrowCUDADeviceManager *manager,
                                       gint gpu_number,
                                       GError **error);
gsize
garrow_cuda_device_manager_get_n_devices(GArrowCUDADeviceManager *manager);

gint64
garrow_cuda_context_get_allocated_size(GArrowCUDAContext *context);


GArrowCUDABuffer *
garrow_cuda_buffer_new(GArrowCUDAContext *context,
                       gint64 size,
                       GError **error);
GArrowCUDABuffer *
garrow_cuda_buffer_new_ipc(GArrowCUDAContext *context,
                           GArrowCUDAIPCMemoryHandle *handle,
                           GError **error);
GArrowCUDABuffer *
garrow_cuda_buffer_new_record_batch(GArrowCUDAContext *context,
                                    GArrowRecordBatch *record_batch,
                                    GError **error);
GBytes *
garrow_cuda_buffer_copy_to_host(GArrowCUDABuffer *buffer,
                                gint64 position,
                                gint64 size,
                                GError **error);
gboolean
garrow_cuda_buffer_copy_from_host(GArrowCUDABuffer *buffer,
                                  const guint8 *data,
                                  gint64 size,
                                  GError **error);
GArrowCUDAIPCMemoryHandle *
garrow_cuda_buffer_export(GArrowCUDABuffer *buffer,
                          GError **error);
GArrowCUDAContext *
garrow_cuda_buffer_get_context(GArrowCUDABuffer *buffer);
GArrowRecordBatch *
garrow_cuda_buffer_read_record_batch(GArrowCUDABuffer *buffer,
                                     GArrowSchema *schema,
                                     GError **error);


GArrowCUDAHostBuffer *
garrow_cuda_host_buffer_new(gint gpu_number,
                            gint64 size,
                            GError **error);

GArrowCUDAIPCMemoryHandle *
garrow_cuda_ipc_memory_handle_new(const guint8 *data,
                                  gsize size,
                                  GError **error);

GArrowBuffer *
garrow_cuda_ipc_memory_handle_serialize(GArrowCUDAIPCMemoryHandle *handle,
                                        GError **error);

GArrowCUDABufferInputStream *
garrow_cuda_buffer_input_stream_new(GArrowCUDABuffer *buffer);

GArrowCUDABufferOutputStream *
garrow_cuda_buffer_output_stream_new(GArrowCUDABuffer *buffer);

gboolean
garrow_cuda_buffer_output_stream_set_buffer_size(GArrowCUDABufferOutputStream *stream,
                                                 gint64 size,
                                                 GError **error);
gint64
garrow_cuda_buffer_output_stream_get_buffer_size(GArrowCUDABufferOutputStream *stream);
gint64
garrow_cuda_buffer_output_stream_get_buffered_size(GArrowCUDABufferOutputStream *stream);

G_END_DECLS
