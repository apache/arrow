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

#include <gandiva-glib/selection-vector.h>

G_BEGIN_DECLS

#define GGANDIVA_TYPE_PROJECTOR (ggandiva_projector_get_type())
GGANDIVA_AVAILABLE_IN_0_12
G_DECLARE_DERIVABLE_TYPE(
  GGandivaProjector, ggandiva_projector, GGANDIVA, PROJECTOR, GObject)

struct _GGandivaProjectorClass
{
  GObjectClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12
GGandivaProjector *
ggandiva_projector_new(GArrowSchema *schema, GList *expressions, GError **error);

GGANDIVA_AVAILABLE_IN_0_12
GList *
ggandiva_projector_evaluate(GGandivaProjector *projector,
                            GArrowRecordBatch *record_batch,
                            GError **error);

#define GGANDIVA_TYPE_SELECTABLE_PROJECTOR (ggandiva_selectable_projector_get_type())
GGANDIVA_AVAILABLE_IN_4_0
G_DECLARE_DERIVABLE_TYPE(GGandivaSelectableProjector,
                         ggandiva_selectable_projector,
                         GGANDIVA,
                         SELECTABLE_PROJECTOR,
                         GGandivaProjector)

struct _GGandivaSelectableProjectorClass
{
  GGandivaProjectorClass parent_class;
};

GGANDIVA_AVAILABLE_IN_4_0
GGandivaSelectableProjector *
ggandiva_selectable_projector_new(GArrowSchema *schema,
                                  GList *expressions,
                                  GGandivaSelectionVectorMode mode,
                                  GError **error);
GGANDIVA_AVAILABLE_IN_4_0
GList *
ggandiva_selectable_projector_evaluate(GGandivaSelectableProjector *projector,
                                       GArrowRecordBatch *record_batch,
                                       GGandivaSelectionVector *selection_vector,
                                       GError **error);

G_END_DECLS
