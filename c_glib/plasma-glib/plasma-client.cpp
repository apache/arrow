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

#include <plasma-glib/plasma-client.hpp>

G_BEGIN_DECLS

/**
 * SECTION: plasma-client
 * @title: PlasmaClient classes
 * @include: plasma-glib/plasma-glib.h
 *
 * #GPlasmaPlasmaClient is a class for an interface with a plasma store
 * and a plasma manager.
 *
 * Since: 0.12.0
 */

typedef struct GPlasmaPlasmaClientPrivate_ {
  std::shared_ptr<plasma::PlasmaClient> plasma_client;
} GPlasmaPlasmaClientPrivate;

enum {
  PROP_0,
  PROP_PLASMA_CLIENT
};

G_DEFINE_TYPE_WITH_PRIVATE(GPlasmaPlasmaClient,
                           gplasma_plasma_client,
                           G_TYPE_OBJECT)

#define GPLASMA_PLASMA_CLIENT_GET_PRIVATE(obj)                 \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                          \
                               GPLASMA_TYPE_PLASMA_CLIENT,     \
                               GPlasmaPlasmaClientPrivate))

static void
gplasma_plasma_client_finalize(GObject *object)
{
  auto priv = GPLASMA_PLASMA_CLIENT_GET_PRIVATE(object);

  priv->plasma_client = nullptr;

  G_OBJECT_CLASS(gplasma_plasma_client_parent_class)->finalize(object);
}

static void
gplasma_plasma_client_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GPLASMA_PLASMA_CLIENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_PLASMA_CLIENT:
    priv->plasma_client =
      *static_cast<std::shared_ptr<plasma::PlasmaClient> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gplasma_plasma_client_init(GPlasmaPlasmaClient *object)
{
}

static void
gplasma_plasma_client_class_init(GPlasmaPlasmaClientClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gplasma_plasma_client_finalize;
  gobject_class->set_property = gplasma_plasma_client_set_property;

  spec = g_param_spec_pointer("plasma_client",
                              "PlasmaClient",
                              "The raw std::shared<plasma::PlasmaClient> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PLASMA_CLIENT, spec);
}

/**
 * gplasma_plasma_client_new:
 *
 * Returns: A newly created #GPlasmaPlasmaClient.
 *
 * Since: 0.12.0
 */
GPlasmaPlasmaClient *
gplasma_plasma_client_new(void)
{
  auto plasma_plasma_client = std::make_shared<plasma::PlasmaClient>();
  return gplasma_plasma_client_new_raw(&plasma_plasma_client);
}

G_END_DECLS

GPlasmaPlasmaClient *
gplasma_plasma_client_new_raw(std::shared_ptr<plasma::PlasmaClient> *plasma_plasma_client)
{
  auto plasma_client = g_object_new(GPLASMA_TYPE_PLASMA_CLIENT,
                                    "plasma_client", plasma_plasma_client,
                                    NULL);
  return GPLASMA_PLASMA_CLIENT(plasma_client);
}

std::shared_ptr<plasma::PlasmaClient>
gplasma_plasma_client_get_raw(GPlasmaPlasmaClient *plasma_client)
{
  auto priv = GPLASMA_PLASMA_CLIENT_GET_PRIVATE(plasma_client);
  return priv->plasma_client;
}
