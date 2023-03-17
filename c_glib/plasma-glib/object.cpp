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

#include <arrow-glib/error.hpp>

#include <plasma-glib/client.hpp>
#include <plasma-glib/object.hpp>

G_BEGIN_DECLS

/**
 * SECTION: object
 * @section_id: object-classes
 * @title: Object related classes
 * @include: plasma-glib/plasma-glib.h
 *
 * Apache Arrow Plasma C GLib is deprecated since 10.0.0. This will be
 * removed from 12.0.0 or so.
 *
 * #GPlasmaObjectID is a class for an object ID.
 *
 * #GPlasmaObject is a base class for an object stored in plasma store.
 *
 * #GPlasmaCreatedObject is a class for a created object. You can
 * change data of the object until the object is sealed or aborted.
 *
 * #GPlasmaReferredObject is a class for a created object. You can
 * only refer the data and metadata of the object. You can't change
 * the data of the object.
 *
 * Since: 0.12.0
 */

typedef struct GPlasmaObjectIDPrivate_ {
  plasma::ObjectID id;
} GPlasmaObjectIDPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaObjectID,
                           gplasma_object_id,
                           G_TYPE_OBJECT)

#define GPLASMA_OBJECT_ID_GET_PRIVATE(object)   \
  static_cast<GPlasmaObjectIDPrivate *>(        \
    gplasma_object_id_get_instance_private(     \
      GPLASMA_OBJECT_ID(object)))

static void
gplasma_object_id_init(GPlasmaObjectID *object)
{
}

static void
gplasma_object_id_class_init(GPlasmaObjectIDClass *klass)
{
}

/**
 * gplasma_object_id_new:
 * @id: (array length=size): The raw ID bytes.
 * @size: The number of bytes of the ID. It must be 1..20.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GPlasmaObjectID on success,
 *   %NULL on error.
 *
 * Since: 0.12.0
 */
GPlasmaObjectID *
gplasma_object_id_new(const guint8 *id,
                      gsize size,
                      GError **error)
{
  if (size == 0 || size > plasma::kUniqueIDSize) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_INVALID,
                "[plasma][object-id][new] "
                "ID must be 1..20 bytes: <%" G_GSIZE_FORMAT ">",
                size);
    return NULL;
  }

  auto object_id = g_object_new(GPLASMA_TYPE_OBJECT_ID, NULL);
  auto priv = GPLASMA_OBJECT_ID_GET_PRIVATE(object_id);
  memcpy(priv->id.mutable_data(), id, size);
  if (size != plasma::kUniqueIDSize) {
    memset(priv->id.mutable_data() + size, 0, plasma::kUniqueIDSize - size);
  }
  return GPLASMA_OBJECT_ID(object_id);
}

/**
 * gplasma_object_id_to_binary:
 * @id: A #GPlasmaObjectID.
 * @size: (nullable) (out): The number of bytes of the byte string of
 *   the object ID. It's always 20. 20 is `plasma::kUniqueIDSize`.
 *
 * Returns: (array length=size): The byte string of the object ID.
 *
 * Since: 0.12.0
 */
const guint8 *
gplasma_object_id_to_binary(GPlasmaObjectID *id,
                            gsize *size)
{
  auto priv = GPLASMA_OBJECT_ID_GET_PRIVATE(id);
  if (size) {
    *size = plasma::kUniqueIDSize;
  }
  return priv->id.data();
}

/**
 * gplasma_object_id_to_hex:
 * @id: A #GPlasmaObjectID.
 *
 * Returns: (transfer full): The hex representation of the object ID.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.12.0
 */
gchar *
gplasma_object_id_to_hex(GPlasmaObjectID *id)
{
  auto priv = GPLASMA_OBJECT_ID_GET_PRIVATE(id);
  const auto hex = priv->id.hex();
  return g_strdup(hex.c_str());
}

typedef struct GPlasmaObjectPrivate_ {
  GPlasmaClient *client;
  GPlasmaObjectID *id;
  std::shared_ptr<arrow::Buffer> raw_data;
  GArrowBuffer *data;
  std::shared_ptr<arrow::Buffer> raw_metadata;
  GArrowBuffer *metadata;
  gint gpu_device;
} GPlasmaObjectPrivate;

enum {
  PROP_CLIENT = 1,
  PROP_ID,
  PROP_RAW_DATA,
  PROP_DATA,
  PROP_RAW_METADATA,
  PROP_METADATA,
  PROP_GPU_DEVICE
};

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaObject,
                           gplasma_object,
                           G_TYPE_OBJECT)

#define GPLASMA_OBJECT_GET_PRIVATE(object)      \
  static_cast<GPlasmaObjectPrivate *>(          \
    gplasma_object_get_instance_private(        \
      GPLASMA_OBJECT(object)))

static void
gplasma_object_dispose(GObject *object)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);

  // Properties except priv->id must be disposed in subclass.

  if (priv->id) {
    g_object_unref(priv->id);
    priv->id = nullptr;
  }

  G_OBJECT_CLASS(gplasma_object_parent_class)->dispose(object);
}

static void
gplasma_object_finalize(GObject *object)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);

  priv->raw_data.~shared_ptr();
  priv->raw_metadata.~shared_ptr();

  G_OBJECT_CLASS(gplasma_object_parent_class)->finalize(object);
}

static void
gplasma_object_set_property(GObject *object,
                            guint prop_id,
                            const GValue *value,
                            GParamSpec *pspec)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CLIENT:
    priv->client = GPLASMA_CLIENT(g_value_dup_object(value));
    break;
  case PROP_ID:
    priv->id = GPLASMA_OBJECT_ID(g_value_dup_object(value));
    break;
  case PROP_RAW_DATA:
    priv->raw_data =
      *static_cast<std::shared_ptr<arrow::Buffer> *>(g_value_get_pointer(value));
    break;
  case PROP_DATA:
    priv->data = GARROW_BUFFER(g_value_dup_object(value));
    break;
  case PROP_RAW_METADATA:
    {
      auto raw_metadata =
        static_cast<std::shared_ptr<arrow::Buffer> *>(g_value_get_pointer(value));
      if (raw_metadata) {
        priv->raw_metadata = *raw_metadata;
      } else {
        priv->raw_metadata = nullptr;
      }
    }
    break;
  case PROP_METADATA:
    priv->metadata = GARROW_BUFFER(g_value_dup_object(value));
    break;
  case PROP_GPU_DEVICE:
    priv->gpu_device = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_object_get_property(GObject *object,
                            guint prop_id,
                            GValue *value,
                            GParamSpec *pspec)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CLIENT:
    g_value_set_object(value, priv->client);
    break;
  case PROP_ID:
    g_value_set_object(value, priv->id);
    break;
  case PROP_DATA:
    g_value_set_object(value, priv->data);
    break;
  case PROP_METADATA:
    g_value_set_object(value, priv->metadata);
    break;
  case PROP_GPU_DEVICE:
    g_value_set_int(value, priv->gpu_device);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_object_init(GPlasmaObject *object)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);
  new(&priv->raw_data) std::shared_ptr<arrow::Buffer>;
  new(&priv->raw_metadata) std::shared_ptr<arrow::Buffer>;
}

static void
gplasma_object_class_init(GPlasmaObjectClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = gplasma_object_dispose;
  gobject_class->finalize     = gplasma_object_finalize;
  gobject_class->set_property = gplasma_object_set_property;
  gobject_class->get_property = gplasma_object_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("client",
                             "Client",
                             "The client",
                             GPLASMA_TYPE_CLIENT,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CLIENT, spec);

  spec = g_param_spec_object("id",
                             "ID",
                             "The ID of this object",
                             GPLASMA_TYPE_OBJECT_ID,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ID, spec);

  spec = g_param_spec_pointer("raw-data",
                              "Raw data",
                              "The raw data of this object",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RAW_DATA, spec);

  spec = g_param_spec_object("data",
                             "Data",
                             "The data of this object",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA, spec);

  spec = g_param_spec_pointer("raw-metadata",
                              "Raw metadata",
                              "The raw metadata of this object",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RAW_METADATA, spec);

  spec = g_param_spec_object("metadata",
                             "Metadata",
                             "The metadata of this object",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_METADATA, spec);

  spec = g_param_spec_int("gpu-device",
                          "GPU device",
                          "The GPU device number. -1 means GPU isn't used.",
                          -1,
                          G_MAXINT,
                          -1,
                          static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                   G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_GPU_DEVICE, spec);
}

static bool
gplasma_object_check_not_released(GPlasmaObjectPrivate *priv,
                                  GError **error,
                                  const gchar *context)
{
  if (priv->client) {
    return true;
  }

  auto id_priv = GPLASMA_OBJECT_ID_GET_PRIVATE(priv->id);
  auto id_hex = id_priv->id.hex();
  g_set_error(error,
              GARROW_ERROR,
              GARROW_ERROR_INVALID,
              "%s: Can't process released object: <%s>",
              context,
              id_hex.c_str());
  return false;
}

static void
gplasma_object_release_resources(GPlasmaObjectPrivate *priv)
{
  if (priv->client) {
    g_object_unref(priv->client);
    priv->client = nullptr;
  }

  if (priv->data) {
    g_object_unref(priv->data);
    priv->data = nullptr;
  }

  if (priv->metadata) {
    g_object_unref(priv->metadata);
    priv->metadata = nullptr;
  }
}

G_DEFINE_TYPE(GPlasmaCreatedObject,
              gplasma_created_object,
              GPLASMA_TYPE_OBJECT)

static void
gplasma_created_object_dispose(GObject *object)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);

  if (priv->client) {
    gplasma_created_object_abort(GPLASMA_CREATED_OBJECT(object), NULL);
  }

  G_OBJECT_CLASS(gplasma_created_object_parent_class)->dispose(object);
}

static void
gplasma_created_object_init(GPlasmaCreatedObject *object)
{
}

static void
gplasma_created_object_class_init(GPlasmaCreatedObjectClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gplasma_created_object_dispose;
}

/**
 * gplasma_created_object_seal:
 * @object: A #GPlasmaCreatedObject.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Seals the object in the object store. You can't use the sealed
 * object anymore.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 0.12.0
 */
gboolean
gplasma_created_object_seal(GPlasmaCreatedObject *object,
                            GError **error)
{
  const auto context = "[plasma][created-object][seal]";

  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);
  if (!gplasma_object_check_not_released(priv, error, context)) {
    return FALSE;
  }

  auto plasma_client = gplasma_client_get_raw(priv->client);
  auto id_priv = GPLASMA_OBJECT_ID_GET_PRIVATE(priv->id);
  auto status = plasma_client->Seal(id_priv->id);
  auto success = garrow_error_check(error, status, context);
  if (success) {
    status = plasma_client->Release(id_priv->id);
    success = garrow_error_check(error, status, context);
    gplasma_object_release_resources(priv);
  }
  return success;
}

/**
 * gplasma_created_object_abort:
 * @object: A #GPlasmaCreatedObject.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Aborts the object in the object store. You can't use the aborted
 * object anymore.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 0.12.0
 */
gboolean
gplasma_created_object_abort(GPlasmaCreatedObject *object,
                             GError **error)
{
  const auto context = "[plasma][created-object][abort]";

  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);
  if (!gplasma_object_check_not_released(priv, error, context)) {
    return FALSE;
  }

  auto plasma_client = gplasma_client_get_raw(priv->client);
  auto id_priv = GPLASMA_OBJECT_ID_GET_PRIVATE(priv->id);
  auto status = plasma_client->Release(id_priv->id);
  auto success = garrow_error_check(error, status, context);
  if (success) {
    status = plasma_client->Abort(id_priv->id);
    success = garrow_error_check(error, status, context);
    gplasma_object_release_resources(priv);
  }
  return success;
}


G_DEFINE_TYPE(GPlasmaReferredObject,
              gplasma_referred_object,
              GPLASMA_TYPE_OBJECT)

static void
gplasma_referred_object_dispose(GObject *object)
{
  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);

  gplasma_object_release_resources(priv);

  G_OBJECT_CLASS(gplasma_referred_object_parent_class)->dispose(object);
}

static void
gplasma_referred_object_init(GPlasmaReferredObject *object)
{
}

static void
gplasma_referred_object_class_init(GPlasmaReferredObjectClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gplasma_referred_object_dispose;
}

/**
 * gplasma_referred_object_release:
 * @object: A #GPlasmaReferredObject.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Releases the object explicitly. The object is no longer valid.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 0.12.0
 */
gboolean
gplasma_referred_object_release(GPlasmaReferredObject *object,
                                GError **error)
{
  const auto context = "[plasma][referred-object][release]";

  auto priv = GPLASMA_OBJECT_GET_PRIVATE(object);
  if (!gplasma_object_check_not_released(priv, error, context)) {
    return FALSE;
  }

  gplasma_object_release_resources(priv);
  return TRUE;
}

G_END_DECLS

plasma::ObjectID
gplasma_object_id_get_raw(GPlasmaObjectID *id)
{
  auto priv = GPLASMA_OBJECT_ID_GET_PRIVATE(id);
  return priv->id;
}

GPlasmaCreatedObject *
gplasma_created_object_new_raw(GPlasmaClient *client,
                               GPlasmaObjectID *id,
                               std::shared_ptr<arrow::Buffer> *raw_data,
                               GArrowBuffer *data,
                               std::shared_ptr<arrow::Buffer> *raw_metadata,
                               GArrowBuffer *metadata,
                               gint gpu_device)
{
  auto object = g_object_new(GPLASMA_TYPE_CREATED_OBJECT,
                             "client", client,
                             "id", id,
                             "raw-data", raw_data,
                             "data", data,
                             "raw-metadata", raw_metadata,
                             "metadata", metadata,
                             "gpu-device", gpu_device,
                             NULL);
  return GPLASMA_CREATED_OBJECT(object);
}

GPlasmaReferredObject *
gplasma_referred_object_new_raw(GPlasmaClient *client,
                                GPlasmaObjectID *id,
                                std::shared_ptr<arrow::Buffer> *raw_data,
                                GArrowBuffer *data,
                                std::shared_ptr<arrow::Buffer> *raw_metadata,
                                GArrowBuffer *metadata,
                                gint gpu_device)
{
  auto object = g_object_new(GPLASMA_TYPE_REFERRED_OBJECT,
                             "client", client,
                             "id", id,
                             "raw-data", raw_data,
                             "data", data,
                             "raw-metadata", raw_metadata,
                             "metadata", metadata,
                             "gpu-device", gpu_device,
                             NULL);
  return GPLASMA_REFERRED_OBJECT(object);
}
