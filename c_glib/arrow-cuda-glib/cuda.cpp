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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/output-stream.hpp>
#include <arrow-glib/readable.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-cuda-glib/cuda.hpp>

G_BEGIN_DECLS

/**
 * SECTION: cuda
 * @section_id: cuda-classes
 * @title: CUDA related classes
 * @include: arrow-cuda-glib/arrow-cuda-glib.h
 *
 * The following classes provide CUDA support for Apache Arrow data.
 *
 * #GArrowCUDADeviceManager is the starting point. You need at
 * least one #GArrowCUDAContext to process Apache Arrow data on
 * NVIDIA GPU.
 *
 * #GArrowCUDAContext is a class to keep context for one GPU. You
 * need to create #GArrowCUDAContext for each GPU that you want to
 * use. You can create #GArrowCUDAContext by
 * garrow_cuda_device_manager_get_context().
 *
 * #GArrowCUDABuffer is a class for data on GPU. You can copy data
 * on GPU to/from CPU by garrow_cuda_buffer_copy_to_host() and
 * garrow_cuda_buffer_copy_from_host(). You can share data on GPU
 * with other processes by garrow_cuda_buffer_export() and
 * garrow_cuda_buffer_new_ipc().
 *
 * #GArrowCUDAHostBuffer is a class for data on CPU that is
 * directly accessible from GPU.
 *
 * #GArrowCUDAIPCMemoryHandle is a class to share data on GPU with
 * other processes. You can export your data on GPU to other processes
 * by garrow_cuda_buffer_export() and
 * garrow_cuda_ipc_memory_handle_new(). You can import other
 * process data on GPU by garrow_cuda_ipc_memory_handle_new() and
 * garrow_cuda_buffer_new_ipc().
 *
 * #GArrowCUDABufferInputStream is a class to read data in
 * #GArrowCUDABuffer.
 *
 * #GArrowCUDABufferOutputStream is a class to write data into
 * #GArrowCUDABuffer.
 */

G_DEFINE_TYPE(GArrowCUDADeviceManager,
              garrow_cuda_device_manager,
              G_TYPE_OBJECT)

static void
garrow_cuda_device_manager_init(GArrowCUDADeviceManager *object)
{
}

static void
garrow_cuda_device_manager_class_init(GArrowCUDADeviceManagerClass *klass)
{
}

/**
 * garrow_cuda_device_manager_new:
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GArrowCUDADeviceManager on success,
 *   %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowCUDADeviceManager *
garrow_cuda_device_manager_new(GError **error)
{
  arrow::cuda::CudaDeviceManager *manager;
  auto status = arrow::cuda::CudaDeviceManager::GetInstance(&manager);
  if (garrow_error_check(error, status, "[cuda][device-manager][new]")) {
    auto manager = g_object_new(GARROW_CUDA_TYPE_DEVICE_MANAGER,
                                NULL);
    return GARROW_CUDA_DEVICE_MANAGER(manager);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_device_manager_get_context:
 * @manager: A #GArrowCUDADeviceManager.
 * @gpu_number: A GPU device number for the target context.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowCUDAContext on
 *   success, %NULL on error. Contexts for the same GPU device number
 *   share the same data internally.
 *
 * Since: 0.8.0
 */
GArrowCUDAContext *
garrow_cuda_device_manager_get_context(GArrowCUDADeviceManager *manager,
                                       gint gpu_number,
                                       GError **error)
{
  arrow::cuda::CudaDeviceManager *arrow_manager;
  arrow::cuda::CudaDeviceManager::GetInstance(&arrow_manager);
  std::shared_ptr<arrow::cuda::CudaContext> context;
  auto status = arrow_manager->GetContext(gpu_number, &context);
  if (garrow_error_check(error, status,
                         "[cuda][device-manager][get-context]]")) {
    return garrow_cuda_context_new_raw(&context);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_device_manager_get_n_devices:
 * @manager: A #GArrowCUDADeviceManager.
 *
 * Returns: The number of GPU devices.
 *
 * Since: 0.8.0
 */
gsize
garrow_cuda_device_manager_get_n_devices(GArrowCUDADeviceManager *manager)
{
  arrow::cuda::CudaDeviceManager *arrow_manager;
  arrow::cuda::CudaDeviceManager::GetInstance(&arrow_manager);
  return arrow_manager->num_devices();
}


typedef struct GArrowCUDAContextPrivate_ {
  std::shared_ptr<arrow::cuda::CudaContext> context;
} GArrowCUDAContextPrivate;

enum {
  PROP_CONTEXT = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCUDAContext,
                           garrow_cuda_context,
                           G_TYPE_OBJECT)

#define GARROW_CUDA_CONTEXT_GET_PRIVATE(object) \
  static_cast<GArrowCUDAContextPrivate *>(      \
    garrow_cuda_context_get_instance_private(   \
      GARROW_CUDA_CONTEXT(object)))

static void
garrow_cuda_context_finalize(GObject *object)
{
  auto priv = GARROW_CUDA_CONTEXT_GET_PRIVATE(object);

  priv->context = nullptr;

  G_OBJECT_CLASS(garrow_cuda_context_parent_class)->finalize(object);
}

static void
garrow_cuda_context_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CUDA_CONTEXT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CONTEXT:
    priv->context =
      *static_cast<std::shared_ptr<arrow::cuda::CudaContext> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cuda_context_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cuda_context_init(GArrowCUDAContext *object)
{
}

static void
garrow_cuda_context_class_init(GArrowCUDAContextClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_cuda_context_finalize;
  gobject_class->set_property = garrow_cuda_context_set_property;
  gobject_class->get_property = garrow_cuda_context_get_property;

  /**
   * GArrowCUDAContext:context:
   *
   * Since: 0.8.0
   */
  spec = g_param_spec_pointer("context",
                              "Context",
                              "The raw std::shared_ptr<arrow::cuda::CudaContext>",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CONTEXT, spec);
}

/**
 * garrow_cuda_context_get_allocated_size:
 * @context: A #GArrowCUDAContext.
 *
 * Returns: The allocated memory by this context in bytes.
 *
 * Since: 0.8.0
 */
gint64
garrow_cuda_context_get_allocated_size(GArrowCUDAContext *context)
{
  auto arrow_context = garrow_cuda_context_get_raw(context);
  return arrow_context->bytes_allocated();
}


G_DEFINE_TYPE(GArrowCUDABuffer,
              garrow_cuda_buffer,
              GARROW_TYPE_BUFFER)

static void
garrow_cuda_buffer_init(GArrowCUDABuffer *object)
{
}

static void
garrow_cuda_buffer_class_init(GArrowCUDABufferClass *klass)
{
}

/**
 * garrow_cuda_buffer_new:
 * @context: A #GArrowCUDAContext.
 * @size: The number of bytes to be allocated on GPU device for this context.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowCUDABuffer on
 *   success, %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowCUDABuffer *
garrow_cuda_buffer_new(GArrowCUDAContext *context,
                       gint64 size,
                       GError **error)
{
  auto arrow_context = garrow_cuda_context_get_raw(context);
  std::shared_ptr<arrow::cuda::CudaBuffer> arrow_buffer;
  auto status = arrow_context->Allocate(size, &arrow_buffer);
  if (garrow_error_check(error, status, "[cuda][buffer][new]")) {
    return garrow_cuda_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_buffer_new_ipc:
 * @context: A #GArrowCUDAContext.
 * @handle: A #GArrowCUDAIPCMemoryHandle to be communicated.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowCUDABuffer on
 *   success, %NULL on error. The buffer has data from the IPC target.
 *
 * Since: 0.8.0
 */
GArrowCUDABuffer *
garrow_cuda_buffer_new_ipc(GArrowCUDAContext *context,
                           GArrowCUDAIPCMemoryHandle *handle,
                           GError **error)
{
  auto arrow_context = garrow_cuda_context_get_raw(context);
  auto arrow_handle = garrow_cuda_ipc_memory_handle_get_raw(handle);
  std::shared_ptr<arrow::cuda::CudaBuffer> arrow_buffer;
  auto status = arrow_context->OpenIpcBuffer(*arrow_handle, &arrow_buffer);
  if (garrow_error_check(error, status,
                         "[cuda][buffer][new-ipc]")) {
    return garrow_cuda_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_buffer_new_record_batch:
 * @context: A #GArrowCUDAContext.
 * @record_batch: A #GArrowRecordBatch to be serialized.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowCUDABuffer on
 *   success, %NULL on error. The buffer has serialized record batch
 *   data.
 *
 * Since: 0.8.0
 */
GArrowCUDABuffer *
garrow_cuda_buffer_new_record_batch(GArrowCUDAContext *context,
                                    GArrowRecordBatch *record_batch,
                                    GError **error)
{
  auto arrow_context = garrow_cuda_context_get_raw(context);
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  std::shared_ptr<arrow::cuda::CudaBuffer> arrow_buffer;
  auto status = arrow::cuda::SerializeRecordBatch(*arrow_record_batch,
                                                  arrow_context.get(),
                                                  &arrow_buffer);
  if (garrow_error_check(error, status,
                         "[cuda][buffer][new-record-batch]")) {
    return garrow_cuda_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_buffer_copy_to_host:
 * @buffer: A #GArrowCUDABuffer.
 * @position: The offset of memory on GPU device to be copied.
 * @size: The size of memory on GPU device to be copied in bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A #GBytes that have copied memory on CPU
 *   host on success, %NULL on error.
 *
 * Since: 0.8.0
 */
GBytes *
garrow_cuda_buffer_copy_to_host(GArrowCUDABuffer *buffer,
                                gint64 position,
                                gint64 size,
                                GError **error)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  auto data = static_cast<uint8_t *>(g_malloc(size));
  auto status = arrow_buffer->CopyToHost(position, size, data);
  if (garrow_error_check(error, status, "[cuda][buffer][copy-to-host]")) {
    return g_bytes_new_take(data, size);
  } else {
    g_free(data);
    return NULL;
  }
}

/**
 * garrow_cuda_buffer_copy_from_host:
 * @buffer: A #GArrowCUDABuffer.
 * @data: (array length=size): Data on CPU host to be copied.
 * @size: The size of data on CPU host to be copied in bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.8.0
 */
gboolean
garrow_cuda_buffer_copy_from_host(GArrowCUDABuffer *buffer,
                                  const guint8 *data,
                                  gint64 size,
                                  GError **error)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  auto status = arrow_buffer->CopyFromHost(0, data, size);
  return garrow_error_check(error,
                            status,
                            "[cuda][buffer][copy-from-host]");
}

/**
 * garrow_cuda_buffer_export:
 * @buffer: A #GArrowCUDABuffer.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created
 *   #GArrowCUDAIPCMemoryHandle to handle the exported buffer on
 *   success, %NULL on error
 *
 * Since: 0.8.0
 */
GArrowCUDAIPCMemoryHandle *
garrow_cuda_buffer_export(GArrowCUDABuffer *buffer, GError **error)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  std::shared_ptr<arrow::cuda::CudaIpcMemHandle> arrow_handle;
  auto status = arrow_buffer->ExportForIpc(&arrow_handle);
  if (garrow_error_check(error, status, "[cuda][buffer][export-for-ipc]")) {
    return garrow_cuda_ipc_memory_handle_new_raw(&arrow_handle);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_buffer_get_context:
 * @buffer: A #GArrowCUDABuffer.
 *
 * Returns: (transfer full): A newly created #GArrowCUDAContext for the
 *   buffer. Contexts for the same buffer share the same data internally.
 *
 * Since: 0.8.0
 */
GArrowCUDAContext *
garrow_cuda_buffer_get_context(GArrowCUDABuffer *buffer)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  auto arrow_context = arrow_buffer->context();
  return garrow_cuda_context_new_raw(&arrow_context);
}

/**
 * garrow_cuda_buffer_read_record_batch:
 * @buffer: A #GArrowCUDABuffer.
 * @schema: A #GArrowSchema for record batch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowRecordBatch on
 *   success, %NULL on error. The record batch data is located on GPU.
 *
 * Since: 0.8.0
 */
GArrowRecordBatch *
garrow_cuda_buffer_read_record_batch(GArrowCUDABuffer *buffer,
                                     GArrowSchema *schema,
                                     GError **error)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow::cuda::ReadRecordBatch(arrow_schema,
                                             arrow_buffer,
                                             pool,
                                             &arrow_record_batch);
  if (garrow_error_check(error, status,
                         "[cuda][buffer][read-record-batch]")) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowCUDAHostBuffer,
              garrow_cuda_host_buffer,
              GARROW_TYPE_MUTABLE_BUFFER)

static void
garrow_cuda_host_buffer_init(GArrowCUDAHostBuffer *object)
{
}

static void
garrow_cuda_host_buffer_class_init(GArrowCUDAHostBufferClass *klass)
{
}

/**
 * garrow_cuda_host_buffer_new:
 * @gpu_number: A GPU device number for the target context.
 * @size: The number of bytes to be allocated on CPU host.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GArrowCUDAHostBuffer on success,
 *   %NULL on error. The allocated memory is accessible from GPU
 *   device for the @context.
 *
 * Since: 0.8.0
 */
GArrowCUDAHostBuffer *
garrow_cuda_host_buffer_new(gint gpu_number, gint64 size, GError **error)
{
  arrow::cuda::CudaDeviceManager *manager;
  auto status = arrow::cuda::CudaDeviceManager::GetInstance(&manager);
  std::shared_ptr<arrow::cuda::CudaHostBuffer> arrow_buffer;
  status = manager->AllocateHost(gpu_number, size, &arrow_buffer);
  if (garrow_error_check(error, status, "[cuda][host-buffer][new]")) {
    return garrow_cuda_host_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}


typedef struct GArrowCUDAIPCMemoryHandlePrivate_ {
  std::shared_ptr<arrow::cuda::CudaIpcMemHandle> ipc_memory_handle;
} GArrowCUDAIPCMemoryHandlePrivate;

enum {
  PROP_IPC_MEMORY_HANDLE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCUDAIPCMemoryHandle,
                           garrow_cuda_ipc_memory_handle,
                           G_TYPE_OBJECT)

#define GARROW_CUDA_IPC_MEMORY_HANDLE_GET_PRIVATE(object)       \
  static_cast<GArrowCUDAIPCMemoryHandlePrivate *>(              \
    garrow_cuda_ipc_memory_handle_get_instance_private(         \
      GARROW_CUDA_IPC_MEMORY_HANDLE(object)))

static void
garrow_cuda_ipc_memory_handle_finalize(GObject *object)
{
  auto priv = GARROW_CUDA_IPC_MEMORY_HANDLE_GET_PRIVATE(object);

  priv->ipc_memory_handle = nullptr;

  G_OBJECT_CLASS(garrow_cuda_ipc_memory_handle_parent_class)->finalize(object);
}

static void
garrow_cuda_ipc_memory_handle_set_property(GObject *object,
                                           guint prop_id,
                                           const GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GARROW_CUDA_IPC_MEMORY_HANDLE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_IPC_MEMORY_HANDLE:
    priv->ipc_memory_handle =
      *static_cast<std::shared_ptr<arrow::cuda::CudaIpcMemHandle> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cuda_ipc_memory_handle_get_property(GObject *object,
                                           guint prop_id,
                                           GValue *value,
                                           GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cuda_ipc_memory_handle_init(GArrowCUDAIPCMemoryHandle *object)
{
}

static void
garrow_cuda_ipc_memory_handle_class_init(GArrowCUDAIPCMemoryHandleClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_cuda_ipc_memory_handle_finalize;
  gobject_class->set_property = garrow_cuda_ipc_memory_handle_set_property;
  gobject_class->get_property = garrow_cuda_ipc_memory_handle_get_property;

  /**
   * GArrowCUDAIPCMemoryHandle:ipc-memory-handle:
   *
   * Since: 0.8.0
   */
  spec = g_param_spec_pointer("ipc-memory-handle",
                              "IPC Memory Handle",
                              "The raw std::shared_ptr<arrow::cuda::CudaIpcMemHandle>",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_IPC_MEMORY_HANDLE, spec);
}

/**
 * garrow_cuda_ipc_memory_handle_new:
 * @data: (array length=size): A serialized #GArrowCUDAIPCMemoryHandle.
 * @size: The size of data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowCUDAIPCMemoryHandle
 *   on success, %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowCUDAIPCMemoryHandle *
garrow_cuda_ipc_memory_handle_new(const guint8 *data,
                                  gsize size,
                                  GError **error)
{
  std::shared_ptr<arrow::cuda::CudaIpcMemHandle> arrow_handle;
  auto status = arrow::cuda::CudaIpcMemHandle::FromBuffer(data, &arrow_handle);
  if (garrow_error_check(error, status,
                         "[cuda][ipc-memory-handle][new]")) {
    return garrow_cuda_ipc_memory_handle_new_raw(&arrow_handle);
  } else {
    return NULL;
  }
}

/**
 * garrow_cuda_ipc_memory_handle_serialize:
 * @handle: A #GArrowCUDAIPCMemoryHandle.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly created #GArrowBuffer on success,
 *   %NULL on error. The buffer has serialized @handle. The serialized
 *   @handle can be deserialized by garrow_gpu_cuda_ipc_memory_handle_new()
 *   in other process.
 *
 * Since: 0.8.0
 */
GArrowBuffer *
garrow_cuda_ipc_memory_handle_serialize(GArrowCUDAIPCMemoryHandle *handle,
                                        GError **error)
{
  auto arrow_handle = garrow_cuda_ipc_memory_handle_get_raw(handle);
  std::shared_ptr<arrow::Buffer> arrow_buffer;
  auto status = arrow_handle->Serialize(arrow::default_memory_pool(),
                                        &arrow_buffer);
  if (garrow_error_check(error, status,
                         "[cuda][ipc-memory-handle][serialize]")) {
    return garrow_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}

GArrowBuffer *
garrow_cuda_buffer_input_stream_new_raw_readable_interface(std::shared_ptr<arrow::Buffer> *arrow_buffer)
{
  auto buffer = GARROW_BUFFER(g_object_new(GARROW_CUDA_TYPE_BUFFER,
                                           "buffer", arrow_buffer,
                                           NULL));
  return buffer;
}

static std::shared_ptr<arrow::io::Readable>
garrow_cuda_buffer_input_stream_get_raw_readable_interface(GArrowReadable *readable)
{
  auto input_stream = GARROW_INPUT_STREAM(readable);
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);
  return arrow_input_stream;
}

static void
garrow_cuda_buffer_input_stream_readable_interface_init(GArrowReadableInterface *iface)
{
  iface->new_raw =
    garrow_cuda_buffer_input_stream_new_raw_readable_interface;
  iface->get_raw =
    garrow_cuda_buffer_input_stream_get_raw_readable_interface;
}

G_DEFINE_TYPE_WITH_CODE(
  GArrowCUDABufferInputStream,
  garrow_cuda_buffer_input_stream,
  GARROW_TYPE_BUFFER_INPUT_STREAM,
  G_IMPLEMENT_INTERFACE(
    GARROW_TYPE_READABLE,
    garrow_cuda_buffer_input_stream_readable_interface_init))

static void
garrow_cuda_buffer_input_stream_init(GArrowCUDABufferInputStream *object)
{
}

static void
garrow_cuda_buffer_input_stream_class_init(GArrowCUDABufferInputStreamClass *klass)
{
}

/**
 * garrow_cuda_buffer_input_stream_new:
 * @buffer: A #GArrowCUDABuffer.
 *
 * Returns: (transfer full): A newly created
 *   #GArrowCUDABufferInputStream.
 *
 * Since: 0.8.0
 */
GArrowCUDABufferInputStream *
garrow_cuda_buffer_input_stream_new(GArrowCUDABuffer *buffer)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  auto arrow_reader =
    std::make_shared<arrow::cuda::CudaBufferReader>(arrow_buffer);
  return garrow_cuda_buffer_input_stream_new_raw(&arrow_reader);
}


G_DEFINE_TYPE(GArrowCUDABufferOutputStream,
              garrow_cuda_buffer_output_stream,
              GARROW_TYPE_OUTPUT_STREAM)

static void
garrow_cuda_buffer_output_stream_init(GArrowCUDABufferOutputStream *object)
{
}

static void
garrow_cuda_buffer_output_stream_class_init(GArrowCUDABufferOutputStreamClass *klass)
{
}

/**
 * garrow_cuda_buffer_output_stream_new:
 * @buffer: A #GArrowCUDABuffer.
 *
 * Returns: (transfer full): A newly created
 *   #GArrowCUDABufferOutputStream.
 *
 * Since: 0.8.0
 */
GArrowCUDABufferOutputStream *
garrow_cuda_buffer_output_stream_new(GArrowCUDABuffer *buffer)
{
  auto arrow_buffer = garrow_cuda_buffer_get_raw(buffer);
  auto arrow_writer =
    std::make_shared<arrow::cuda::CudaBufferWriter>(arrow_buffer);
  return garrow_cuda_buffer_output_stream_new_raw(&arrow_writer);
}

/**
 * garrow_cuda_buffer_output_stream_set_buffer_size:
 * @stream: A #GArrowCUDABufferOutputStream.
 * @size: A size of CPU buffer in bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Sets CPU buffer size. to limit `cudaMemcpy()` calls. If CPU buffer
 * size is `0`, buffering is disabled.
 *
 * The default is `0`.
 *
 * Since: 0.8.0
 */
gboolean
garrow_cuda_buffer_output_stream_set_buffer_size(GArrowCUDABufferOutputStream *stream,
                                                 gint64 size,
                                                 GError **error)
{
  auto arrow_stream = garrow_cuda_buffer_output_stream_get_raw(stream);
  auto status = arrow_stream->SetBufferSize(size);
  return garrow_error_check(error,
                            status,
                            "[cuda][buffer-output-stream][set-buffer-size]");
}

/**
 * garrow_cuda_buffer_output_stream_get_buffer_size:
 * @stream: A #GArrowCUDABufferOutputStream.
 *
 * Returns: The CPU buffer size in bytes.
 *
 * See garrow_cuda_buffer_output_stream_set_buffer_size() for CPU
 * buffer size details.
 *
 * Since: 0.8.0
 */
gint64
garrow_cuda_buffer_output_stream_get_buffer_size(GArrowCUDABufferOutputStream *stream)
{
  auto arrow_stream = garrow_cuda_buffer_output_stream_get_raw(stream);
  return arrow_stream->buffer_size();
}

/**
 * garrow_cuda_buffer_output_stream_get_buffered_size:
 * @stream: A #GArrowCUDABufferOutputStream.
 *
 * Returns: The size of buffered data in bytes.
 *
 * Since: 0.8.0
 */
gint64
garrow_cuda_buffer_output_stream_get_buffered_size(GArrowCUDABufferOutputStream *stream)
{
  auto arrow_stream = garrow_cuda_buffer_output_stream_get_raw(stream);
  return arrow_stream->num_bytes_buffered();
}


G_END_DECLS

GArrowCUDAContext *
garrow_cuda_context_new_raw(std::shared_ptr<arrow::cuda::CudaContext> *arrow_context)
{
  return GARROW_CUDA_CONTEXT(g_object_new(GARROW_CUDA_TYPE_CONTEXT,
                                          "context", arrow_context,
                                          NULL));
}

std::shared_ptr<arrow::cuda::CudaContext>
garrow_cuda_context_get_raw(GArrowCUDAContext *context)
{
  if (!context)
    return nullptr;

  auto priv = GARROW_CUDA_CONTEXT_GET_PRIVATE(context);
  return priv->context;
}

GArrowCUDAIPCMemoryHandle *
garrow_cuda_ipc_memory_handle_new_raw(std::shared_ptr<arrow::cuda::CudaIpcMemHandle> *arrow_handle)
{
  auto handle = g_object_new(GARROW_CUDA_TYPE_IPC_MEMORY_HANDLE,
                             "ipc-memory-handle", arrow_handle,
                             NULL);
  return GARROW_CUDA_IPC_MEMORY_HANDLE(handle);
}

std::shared_ptr<arrow::cuda::CudaIpcMemHandle>
garrow_cuda_ipc_memory_handle_get_raw(GArrowCUDAIPCMemoryHandle *handle)
{
  if (!handle)
    return nullptr;

  auto priv = GARROW_CUDA_IPC_MEMORY_HANDLE_GET_PRIVATE(handle);
  return priv->ipc_memory_handle;
}

GArrowCUDABuffer *
garrow_cuda_buffer_new_raw(std::shared_ptr<arrow::cuda::CudaBuffer> *arrow_buffer)
{
  return GARROW_CUDA_BUFFER(g_object_new(GARROW_CUDA_TYPE_BUFFER,
                                         "buffer", arrow_buffer,
                                         NULL));
}

std::shared_ptr<arrow::cuda::CudaBuffer>
garrow_cuda_buffer_get_raw(GArrowCUDABuffer *buffer)
{
  if (!buffer)
    return nullptr;

  auto arrow_buffer = garrow_buffer_get_raw(GARROW_BUFFER(buffer));
  return std::static_pointer_cast<arrow::cuda::CudaBuffer>(arrow_buffer);
}

GArrowCUDAHostBuffer *
garrow_cuda_host_buffer_new_raw(std::shared_ptr<arrow::cuda::CudaHostBuffer> *arrow_buffer)
{
  auto buffer = g_object_new(GARROW_CUDA_TYPE_HOST_BUFFER,
                             "buffer", arrow_buffer,
                             NULL);
  return GARROW_CUDA_HOST_BUFFER(buffer);
}

std::shared_ptr<arrow::cuda::CudaHostBuffer>
garrow_cuda_host_buffer_get_raw(GArrowCUDAHostBuffer *buffer)
{
  if (!buffer)
    return nullptr;

  auto arrow_buffer = garrow_buffer_get_raw(GARROW_BUFFER(buffer));
  return std::static_pointer_cast<arrow::cuda::CudaHostBuffer>(arrow_buffer);
}

GArrowCUDABufferInputStream *
garrow_cuda_buffer_input_stream_new_raw(std::shared_ptr<arrow::cuda::CudaBufferReader> *arrow_reader)
{
  auto input_stream = g_object_new(GARROW_CUDA_TYPE_BUFFER_INPUT_STREAM,
                                   "input-stream", arrow_reader,
                                   NULL);
  return GARROW_CUDA_BUFFER_INPUT_STREAM(input_stream);
}

std::shared_ptr<arrow::cuda::CudaBufferReader>
garrow_cuda_buffer_input_stream_get_raw(GArrowCUDABufferInputStream *input_stream)
{
  if (!input_stream)
    return nullptr;

  auto arrow_reader =
    garrow_input_stream_get_raw(GARROW_INPUT_STREAM(input_stream));
  return std::static_pointer_cast<arrow::cuda::CudaBufferReader>(arrow_reader);
}

GArrowCUDABufferOutputStream *
garrow_cuda_buffer_output_stream_new_raw(std::shared_ptr<arrow::cuda::CudaBufferWriter> *arrow_writer)
{
  auto output_stream = g_object_new(GARROW_CUDA_TYPE_BUFFER_OUTPUT_STREAM,
                                    "output-stream", arrow_writer,
                                    NULL);
  return GARROW_CUDA_BUFFER_OUTPUT_STREAM(output_stream);
}

std::shared_ptr<arrow::cuda::CudaBufferWriter>
garrow_cuda_buffer_output_stream_get_raw(GArrowCUDABufferOutputStream *output_stream)
{
  if (!output_stream)
    return nullptr;

  auto arrow_writer =
    garrow_output_stream_get_raw(GARROW_OUTPUT_STREAM(output_stream));
  return std::static_pointer_cast<arrow::cuda::CudaBufferWriter>(arrow_writer);
}
