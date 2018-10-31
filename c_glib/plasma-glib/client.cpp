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

#include <arrow-glib/error.hpp>

#include <plasma-glib/client.hpp>

G_BEGIN_DECLS

/**
 * SECTION: client
 * @title: Client classes
 * @include: plasma-glib/plasma-glib.h
 *
 * #GPlasmaClient is a class for an interface with a plasma store
 * and a plasma manager.
 *
 * Since: 0.12.0
 */

typedef struct GPlasmaClientPrivate_ {
  std::shared_ptr<plasma::PlasmaClient> client;
} GPlasmaClientPrivate;

enum {
  PROP_0,
  PROP_CLIENT
};

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaClient,
                           gplasma_client,
                           G_TYPE_OBJECT)

#define GPLASMA_CLIENT_GET_PRIVATE(obj)                 \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                   \
                               GPLASMA_TYPE_CLIENT,     \
                               GPlasmaClientPrivate))

static void
gplasma_client_finalize(GObject *object)
{
  auto priv = GPLASMA_CLIENT_GET_PRIVATE(object);

  priv->client = nullptr;

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
      *static_cast<std::shared_ptr<plasma::PlasmaClient> *>(g_value_get_pointer(value));
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
                              "The raw std::shared<plasma::PlasmaClient> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CLIENT, spec);
}

/**
 * gplasma_client_new:
 * @store_socket_name: The name of the UNIX domain socket.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GPlasmaClient on success,
 *   %NULL on error.
 *
 * Since: 0.12.0
 */
GPlasmaClient *
gplasma_client_new(const gchar *store_socket_name,
                   GError **error)
{
  auto plasma_client = std::make_shared<plasma::PlasmaClient>();
  auto status = plasma_client->Connect(store_socket_name, "");
  if (garrow_error_check(error, status, "[plasma][client][new]")) {
    return gplasma_client_new_raw(&plasma_client);
  } else {
    return NULL;
  }
}

G_END_DECLS

GPlasmaClient *
gplasma_client_new_raw(std::shared_ptr<plasma::PlasmaClient> *plasma_client)
{
  auto client = g_object_new(GPLASMA_TYPE_CLIENT,
                             "client", plasma_client,
                             NULL);
  return GPLASMA_CLIENT(client);
}

std::shared_ptr<plasma::PlasmaClient>
gplasma_client_get_raw(GPlasmaClient *client)
{
  auto priv = GPLASMA_CLIENT_GET_PRIVATE(client);
  return priv->client;
}
