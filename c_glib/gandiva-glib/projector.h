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

#define GGANDIVA_TYPE_PROJECTOR (ggandiva_projector_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaProjector,
                         ggandiva_projector,
                         GGANDIVA,
                         PROJECTOR,
                         GObject)

struct _GGandivaProjectorClass
{
  GObjectClass parent_class;
};

GGandivaProjector *ggandiva_projector_new(GArrowSchema *schema,
                                          GList *expressions,
                                          GError **error);
GList *ggandiva_projector_evaluate(GGandivaProjector *projector,
                                   GArrowRecordBatch *record_batch,
                                   GError **error);

G_END_DECLS
