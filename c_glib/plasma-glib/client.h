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

#include <plasma-glib/object.h>

G_BEGIN_DECLS

#define GPLASMA_TYPE_CLIENT_OPTIONS (gplasma_client_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaClientOptions,
                         gplasma_client_options,
                         GPLASMA,
                         CLIENT_OPTIONS,
                         GObject)

struct _GPlasmaClientOptionsClass
{
  GObjectClass parent_class;
};

GPlasmaClientOptions *gplasma_client_options_new(void);
void
gplasma_client_options_set_n_retries(GPlasmaClientOptions *options,
                                     gint n_retries);
gint
gplasma_client_options_get_n_retries(GPlasmaClientOptions *options);


#define GPLASMA_TYPE_CLIENT_CREATE_OPTIONS      \
  (gplasma_client_create_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaClientCreateOptions,
                         gplasma_client_create_options,
                         GPLASMA,
                         CLIENT_CREATE_OPTIONS,
                         GObject)

struct _GPlasmaClientCreateOptionsClass
{
  GObjectClass parent_class;
};

GPlasmaClientCreateOptions *gplasma_client_create_options_new(void);
void
gplasma_client_create_options_set_metadata(GPlasmaClientCreateOptions *options,
                                           const guint8 *metadata,
                                           gsize size);
const guint8 *
gplasma_client_create_options_get_metadata(GPlasmaClientCreateOptions *options,
                                           gsize *size);


#define GPLASMA_TYPE_CLIENT (gplasma_client_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaClient,
                         gplasma_client,
                         GPLASMA,
                         CLIENT,
                         GObject)

struct _GPlasmaClientClass
{
  GObjectClass parent_class;
};

GPlasmaClient *gplasma_client_new(const gchar *store_socket_name,
                                  GPlasmaClientOptions *options,
                                  GError **error);
GPlasmaCreatedObject *
gplasma_client_create(GPlasmaClient *client,
                      GPlasmaObjectID *id,
                      gsize data_size,
                      GPlasmaClientCreateOptions *options,
                      GError **error);
GPlasmaReferredObject *
gplasma_client_refer_object(GPlasmaClient *client,
                            GPlasmaObjectID *id,
                            gint64 timeout_ms,
                            GError **error);
gboolean gplasma_client_disconnect(GPlasmaClient *client,
                                   GError **error);

G_END_DECLS
