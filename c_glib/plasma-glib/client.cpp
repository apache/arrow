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

#ifdef HAVE_ARROW_CUDA
#  include <arrow-cuda-glib/cuda.hpp>
#endif

#include <plasma-glib/client.hpp>
#include <plasma-glib/object.hpp>

G_BEGIN_DECLS

/**
 * SECTION: client
 * @section_id: client-classes
 * @title: Client related classes
 * @include: plasma-glib/plasma-glib.h
 *
 * #GPlasmaClientOptions is a class for customizing plasma store
 * connection.
 *
 * #GPlasmaClientCreateOptions is a class for customizing object creation.
 *
 * #GPlasmaClient is a class for an interface with a plasma store.
 *
 * Since: 0.12.0
 */

typedef struct GPlasmaClientCreatePrivate_ {
  gint n_retries;
} GPlasmaClientOptionsPrivate;

enum {
  PROP_N_RETRIES = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaClientOptions,
                           gplasma_client_options,
                           G_TYPE_OBJECT)

#define GPLASMA_CLIENT_OPTIONS_GET_PRIVATE(object)      \
  static_cast<GPlasmaClientOptionsPrivate *>(           \
    gplasma_client_options_get_instance_private(        \
      GPLASMA_CLIENT_OPTIONS(object)))

static void
gplasma_client_options_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GPLASMA_CLIENT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_N_RETRIES:
    priv->n_retries = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_client_options_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GPLASMA_CLIENT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_N_RETRIES:
    g_value_set_int(value, priv->n_retries);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_client_options_init(GPlasmaClientOptions *object)
{
}

static void
gplasma_client_options_class_init(GPlasmaClientOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = gplasma_client_options_set_property;
  gobject_class->get_property = gplasma_client_options_get_property;

  GParamSpec *spec;
  spec = g_param_spec_int("n-retries",
                          "N retries",
                          "The number of retries to connect plasma store. "
                          "-1 means that the system default value is used.",
                          -1,
                          G_MAXINT,
                          -1,
                          static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                   G_PARAM_CONSTRUCT));
  g_object_class_install_property(gobject_class, PROP_N_RETRIES, spec);
}

/**
 * gplasma_client_options_new:
 *
 * Returns: A newly created #GPlasmaClientOptions.
 *
 * Since: 0.12.0
 */
GPlasmaClientOptions *
gplasma_client_options_new(void)
{
  auto options = g_object_new(GPLASMA_TYPE_CLIENT_OPTIONS,
                              NULL);
  return GPLASMA_CLIENT_OPTIONS(options);
}

/**
 * gplasma_client_options_set_n_retries:
 * @options: A #GPlasmaClientOptions.
 * @n_retries: The number of retires on connect.
 *
 * Since: 0.12.0
 */
void
gplasma_client_options_set_n_retries(GPlasmaClientOptions *options,
                                     gint n_retries)
{
  auto priv = GPLASMA_CLIENT_OPTIONS_GET_PRIVATE(options);
  priv->n_retries = n_retries;
}

/**
 * gplasma_client_options_get_n_retries:
 * @options: A #GPlasmaClientOptions.
 *
 * Returns: The number of retries on connect.
 *
 * Since: 0.12.0
 */
gint
gplasma_client_options_get_n_retries(GPlasmaClientOptions *options)
{
  auto priv = GPLASMA_CLIENT_OPTIONS_GET_PRIVATE(options);
  return priv->n_retries;
}


typedef struct GPlasmaClientCreateOptionsPrivate_ {
  guint8 *metadata;
  gsize metadata_size;
  gint gpu_device;
} GPlasmaClientCreateOptionsPrivate;

enum {
  PROP_GPU_DEVICE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaClientCreateOptions,
                           gplasma_client_create_options,
                           G_TYPE_OBJECT)

#define GPLASMA_CLIENT_CREATE_OPTIONS_GET_PRIVATE(object)         \
  static_cast<GPlasmaClientCreateOptionsPrivate *>(               \
    gplasma_client_create_options_get_instance_private(           \
      GPLASMA_CLIENT_CREATE_OPTIONS(object)))

static void
gplasma_client_create_options_set_property(GObject *object,
                                           guint prop_id,
                                           const GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GPLASMA_CLIENT_CREATE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_GPU_DEVICE:
    priv->gpu_device = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_client_create_options_get_property(GObject *object,
                                           guint prop_id,
                                           GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GPLASMA_CLIENT_CREATE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_GPU_DEVICE:
    g_value_set_int(value, priv->gpu_device);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_client_create_options_init(GPlasmaClientCreateOptions *object)
{
}

static void
gplasma_client_create_options_class_init(GPlasmaClientCreateOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = gplasma_client_create_options_set_property;
  gobject_class->get_property = gplasma_client_create_options_get_property;

  GParamSpec *spec;
  spec = g_param_spec_int("gpu-device",
                          "GPU device",
                          "The GPU device number. -1 means GPU isn't used.",
                          -1,
                          G_MAXINT,
                          -1,
                          static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                   G_PARAM_CONSTRUCT));
  g_object_class_install_property(gobject_class, PROP_GPU_DEVICE, spec);
}

/**
 * gplasma_client_create_options_new:
 *
 * Returns: A newly created #GPlasmaClientCreateOptions.
 *
 * Since: 0.12.0
 */
GPlasmaClientCreateOptions *
gplasma_client_create_options_new(void)
{
  auto options = g_object_new(GPLASMA_TYPE_CLIENT_CREATE_OPTIONS,
                              NULL);
  return GPLASMA_CLIENT_CREATE_OPTIONS(options);
}

/**
 * gplasma_client_create_options_set_metadata:
 * @options: A #GPlasmaClientCreateOptions.
 * @metadata: (nullable) (array length=size): The metadata of a created object.
 * @size: The number of bytes of the metadata.
 *
 * Since: 0.12.0
 */
void
gplasma_client_create_options_set_metadata(GPlasmaClientCreateOptions *options,
                                           const guint8 *metadata,
                                           gsize size)
{
  auto priv = GPLASMA_CLIENT_CREATE_OPTIONS_GET_PRIVATE(options);
  if (priv->metadata) {
    g_free(priv->metadata);
  }
  priv->metadata = static_cast<guint8 *>(g_memdup(metadata, size));
  priv->metadata_size = size;
}

/**
 * gplasma_client_create_options_get_metadata:
 * @options: A #GPlasmaClientCreateOptions.
 * @size: (nullable) (out): The number of bytes of the metadata.
 *
 * Returns: (nullable) (array length=size): The metadata of a created object.
 *
 * Since: 0.12.0
 */
const guint8 *
gplasma_client_create_options_get_metadata(GPlasmaClientCreateOptions *options,
                                           gsize *size)
{
  auto priv = GPLASMA_CLIENT_CREATE_OPTIONS_GET_PRIVATE(options);
  if (size) {
    *size = priv->metadata_size;
  }
  return priv->metadata;
}


typedef struct GPlasmaClientPrivate_ {
  plasma::PlasmaClient *client;
  bool disconnected;
} GPlasmaClientPrivate;

enum {
  PROP_CLIENT = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaClient,
                           gplasma_client,
                           G_TYPE_OBJECT)

#define GPLASMA_CLIENT_GET_PRIVATE(object)         \
  static_cast<GPlasmaClientPrivate *>(             \
    gplasma_client_get_instance_private(           \
      GPLASMA_CLIENT(object)))

static void
gplasma_client_finalize(GObject *object)
{
  auto priv = GPLASMA_CLIENT_GET_PRIVATE(object);

  if (!priv->disconnected) {
    auto status = priv->client->Disconnect();
    if (!status.ok()) {
      g_warning("[plasma][client][finalize] Failed to disconnect: %s",
                status.ToString().c_str());
    }
  }
  delete priv->client;

  G_OBJECT_CLASS(gplasma_client_parent_class)->finalize(object);
}

static void
gplasma_client_set_property(GObject *object,
                            guint prop_id,
                            const GValue *value,
                            GParamSpec *pspec)
{
  auto priv = GPLASMA_CLIENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CLIENT:
    priv->client =
      static_cast<plasma::PlasmaClient *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_client_init(GPlasmaClient *object)
{
}

static void
gplasma_client_class_init(GPlasmaClientClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gplasma_client_finalize;
  gobject_class->set_property = gplasma_client_set_property;

  spec = g_param_spec_pointer("client",
                              "Client",
                              "The raw plasma::PlasmaClient *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CLIENT, spec);
}

/**
 * gplasma_client_new:
 * @store_socket_name: The name of the UNIX domain socket.
 * @options: (nullable): The options to custom how to connect to plasma store.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GPlasmaClient on success,
 *   %NULL on error.
 *
 * Since: 0.12.0
 */
GPlasmaClient *
gplasma_client_new(const gchar *store_socket_name,
                   GPlasmaClientOptions *options,
                   GError **error)
{
  auto plasma_client = new plasma::PlasmaClient();
  int n_retries = -1;
  if (options) {
    n_retries = gplasma_client_options_get_n_retries(options);
  }
  auto status = plasma_client->Connect(store_socket_name, "", 0, n_retries);
  if (garrow_error_check(error, status, "[plasma][client][new]")) {
    return gplasma_client_new_raw(plasma_client);
  } else {
    return NULL;
  }
}

/**
 * gplasma_client_create:
 * @client: A #GPlasmaClient.
 * @id: The ID for a newly created object.
 * @data_size: The number of bytes of data for a newly created object.
 * @options: (nullable): The option for creating an object.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly created #GPlasmaCreatedObject
 *   on success, %NULL on error.
 *
 * Since: 0.12.0
 */
GPlasmaCreatedObject *
gplasma_client_create(GPlasmaClient *client,
                      GPlasmaObjectID *id,
                      gsize data_size,
                      GPlasmaClientCreateOptions *options,
                      GError **error)
{
  const auto context = "[plasma][client][create]";
  auto plasma_client = gplasma_client_get_raw(client);
  auto plasma_id = gplasma_object_id_get_raw(id);
  const uint8_t *raw_metadata = nullptr;
  int64_t raw_metadata_size = 0;
  int device_number = 0;
  if (options) {
    auto options_priv = GPLASMA_CLIENT_CREATE_OPTIONS_GET_PRIVATE(options);
    raw_metadata = options_priv->metadata;
    raw_metadata_size = options_priv->metadata_size;
    if (options_priv->gpu_device >= 0) {
#ifndef HAVE_ARROW_CUDA
      g_set_error(error,
                  GARROW_ERROR,
                  GARROW_ERROR_INVALID,
                  "%s Arrow CUDA GLib is needed to use GPU",
                  context);
      return NULL;
#endif
      device_number = options_priv->gpu_device + 1;
    }
  }
  std::shared_ptr<arrow::Buffer> plasma_data;
  auto status = plasma_client->Create(plasma_id,
                                      data_size,
                                      raw_metadata,
                                      raw_metadata_size,
                                      &plasma_data,
                                      device_number);
  if (!garrow_error_check(error, status, context)) {
    return NULL;
  }

  GArrowBuffer *data = nullptr;
  if (device_number == 0) {
    auto plasma_mutable_data =
      std::static_pointer_cast<arrow::MutableBuffer>(plasma_data);
    data = GARROW_BUFFER(garrow_mutable_buffer_new_raw(&plasma_mutable_data));
#ifdef HAVE_ARROW_CUDA
  } else {
    auto plasma_cuda_data =
      std::static_pointer_cast<arrow::cuda::CudaBuffer>(plasma_data);
    data = GARROW_BUFFER(garrow_cuda_buffer_new_raw(&plasma_cuda_data));
#endif
  }
  std::shared_ptr<arrow::Buffer> plasma_metadata;
  GArrowBuffer *metadata = nullptr;
  if (raw_metadata_size > 0) {
    plasma_metadata =
      std::make_shared<arrow::Buffer>(raw_metadata, raw_metadata_size);
    metadata = garrow_buffer_new_raw(&plasma_metadata);
  }
  return gplasma_created_object_new_raw(client,
                                        id,
                                        &plasma_data,
                                        data,
                                        metadata ? &plasma_metadata : nullptr,
                                        metadata,
                                        device_number - 1);
}

/**
 * gplasma_client_refer_object:
 * @client: A #GPlasmaClient.
 * @id: The ID of the target object.
 * @timeout_ms: The timeout in milliseconds. -1 means no timeout.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A found #GPlasmaReferredObject
 *   on success, %NULL on error.
 *
 * Since: 0.12.0
 */
GPlasmaReferredObject *
gplasma_client_refer_object(GPlasmaClient *client,
                            GPlasmaObjectID *id,
                            gint64 timeout_ms,
                            GError **error)
{
  const auto context = "[plasma][client][refer-object]";
  auto plasma_client = gplasma_client_get_raw(client);
  auto plasma_id = gplasma_object_id_get_raw(id);
  std::vector<plasma::ObjectID> plasma_ids;
  plasma_ids.push_back(plasma_id);
  std::vector<plasma::ObjectBuffer> plasma_object_buffers;
  auto status = plasma_client->Get(plasma_ids,
                                   timeout_ms,
                                   &plasma_object_buffers);
  if (!garrow_error_check(error, status, context)) {
    return NULL;
  }

  auto plasma_object_buffer = plasma_object_buffers[0];
  auto plasma_data = plasma_object_buffer.data;
  auto plasma_metadata = plasma_object_buffer.metadata;
  GArrowBuffer *data = nullptr;
  GArrowBuffer *metadata = nullptr;
  if (plasma_object_buffer.device_num == 0) {
    data = garrow_buffer_new_raw(&plasma_data);
    metadata = garrow_buffer_new_raw(&plasma_metadata);
  } else {
#ifdef HAVE_ARROW_CUDA
    std::shared_ptr<arrow::cuda::CudaBuffer> plasma_cuda_data;
    status = arrow::cuda::CudaBuffer::FromBuffer(plasma_data,
                                                 &plasma_cuda_data);
    if (!garrow_error_check(error, status, context)) {
      return NULL;
    }
    std::shared_ptr<arrow::cuda::CudaBuffer> plasma_cuda_metadata;
    status = arrow::cuda::CudaBuffer::FromBuffer(plasma_metadata,
                                                 &plasma_cuda_metadata);
    if (!garrow_error_check(error, status, context)) {
      return NULL;
    }

    data = GARROW_BUFFER(garrow_cuda_buffer_new_raw(&plasma_cuda_data));
    metadata =
      GARROW_BUFFER(garrow_cuda_buffer_new_raw(&plasma_cuda_metadata));
#else
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_INVALID,
                "%s Arrow CUDA GLib is needed to use GPU",
                context);
    return NULL;
#endif
  }
  return gplasma_referred_object_new_raw(client,
                                         id,
                                         &plasma_data,
                                         data,
                                         &plasma_metadata,
                                         metadata,
                                         plasma_object_buffer.device_num - 1);
}

/**
 * gplasma_client_disconnect:
 * @client: A #GPlasmaClient.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.12.0
 */
gboolean
gplasma_client_disconnect(GPlasmaClient *client,
                          GError **error)
{
  auto priv = GPLASMA_CLIENT_GET_PRIVATE(client);
  auto status = priv->client->Disconnect();
  if (garrow_error_check(error, status, "[plasma][client][disconnect]")) {
    priv->disconnected = true;
    return TRUE;
  } else {
    return FALSE;
  }
}

G_END_DECLS

GPlasmaClient *
gplasma_client_new_raw(plasma::PlasmaClient *plasma_client)
{
  auto client = g_object_new(GPLASMA_TYPE_CLIENT,
                             "client", plasma_client,
                             NULL);
  return GPLASMA_CLIENT(client);
}

plasma::PlasmaClient *
gplasma_client_get_raw(GPlasmaClient *client)
{
  auto priv = GPLASMA_CLIENT_GET_PRIVATE(client);
  return priv->client;
}
