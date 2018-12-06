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

#include <arrow-glib/buffer.h>

G_BEGIN_DECLS

#define GPLASMA_TYPE_OBJECT_ID (gplasma_object_id_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaObjectID,
                         gplasma_object_id,
                         GPLASMA,
                         OBJECT_ID,
                         GObject)

struct _GPlasmaObjectIDClass
{
  GObjectClass parent_class;
};

GPlasmaObjectID *gplasma_object_id_new(const guint8 *id,
                                       gsize size,
                                       GError **error);
const guint8 *gplasma_object_id_to_binary(GPlasmaObjectID *id,
                                          gsize *size);
gchar *gplasma_object_id_to_hex(GPlasmaObjectID *id);

#define GPLASMA_TYPE_OBJECT (gplasma_object_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaObject,
                         gplasma_object,
                         GPLASMA,
                         OBJECT,
                         GObject)

struct _GPlasmaObjectClass
{
  GObjectClass parent_class;
};

#define GPLASMA_TYPE_CREATED_OBJECT (gplasma_created_object_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaCreatedObject,
                         gplasma_created_object,
                         GPLASMA,
                         CREATED_OBJECT,
                         GPlasmaObject)

struct _GPlasmaCreatedObjectClass
{
  GPlasmaObjectClass parent_class;
};

gboolean gplasma_created_object_seal(GPlasmaCreatedObject *object,
                                     GError **error);
gboolean gplasma_created_object_abort(GPlasmaCreatedObject *object,
                                      GError **error);

#define GPLASMA_TYPE_REFERRED_OBJECT (gplasma_referred_object_get_type())
G_DECLARE_DERIVABLE_TYPE(GPlasmaReferredObject,
                         gplasma_referred_object,
                         GPLASMA,
                         REFERRED_OBJECT,
                         GPlasmaObject)

struct _GPlasmaReferredObjectClass
{
  GPlasmaObjectClass parent_class;
};

gboolean gplasma_referred_object_release(GPlasmaReferredObject *object,
                                         GError **error);

G_END_DECLS
