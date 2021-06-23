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

#include <gandiva-glib/version.h>

G_BEGIN_DECLS

/**
 * GGandivaSelectionVectorMode:
 * @GGANDIVA_SELECTION_VECTOR_MODE_NONE: Selection vector isn't used.
 * @GGANDIVA_SELECTION_VECTOR_MODE_UINT16:
 *   #GGandivaUInt16SelectionVector is used.
 * @GGANDIVA_SELECTION_VECTOR_MODE_UINT32:
 *   #GGandivaUInt32SelectionVector is used.
 * @GGANDIVA_SELECTION_VECTOR_MODE_UINT64:
 *   #GGandivaUInt64SelectionVector is used.
 *
 * They are corresponding to `gandiva::SelectionVector::Mode` values.
 *
 * Since: 4.0.0
 */
typedef enum {
  GGANDIVA_SELECTION_VECTOR_MODE_NONE,
  GGANDIVA_SELECTION_VECTOR_MODE_UINT16,
  GGANDIVA_SELECTION_VECTOR_MODE_UINT32,
  GGANDIVA_SELECTION_VECTOR_MODE_UINT64,
} GGandivaSelectionVectorMode;


#define GGANDIVA_TYPE_SELECTION_VECTOR (ggandiva_selection_vector_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaSelectionVector,
                         ggandiva_selection_vector,
                         GGANDIVA,
                         SELECTION_VECTOR,
                         GObject)

struct _GGandivaSelectionVectorClass
{
  GObjectClass parent_class;
};

GGANDIVA_AVAILABLE_IN_4_0
GGandivaSelectionVectorMode
ggandiva_selection_vector_get_mode(GGandivaSelectionVector *selection_vector);

GGANDIVA_AVAILABLE_IN_4_0
GArrowArray *
ggandiva_selection_vector_to_array(GGandivaSelectionVector *selection_vector);


#define GGANDIVA_TYPE_UINT16_SELECTION_VECTOR   \
  (ggandiva_uint16_selection_vector_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt16SelectionVector,
                         ggandiva_uint16_selection_vector,
                         GGANDIVA,
                         UINT16_SELECTION_VECTOR,
                         GGandivaSelectionVector)

struct _GGandivaUInt16SelectionVectorClass
{
  GGandivaSelectionVectorClass parent_class;
};

GGANDIVA_AVAILABLE_IN_4_0
GGandivaUInt16SelectionVector *
ggandiva_uint16_selection_vector_new(gint64 max_slots,
                                     GError **error);


#define GGANDIVA_TYPE_UINT32_SELECTION_VECTOR   \
  (ggandiva_uint32_selection_vector_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt32SelectionVector,
                         ggandiva_uint32_selection_vector,
                         GGANDIVA,
                         UINT32_SELECTION_VECTOR,
                         GGandivaSelectionVector)

struct _GGandivaUInt32SelectionVectorClass
{
  GGandivaSelectionVectorClass parent_class;
};

GGANDIVA_AVAILABLE_IN_4_0
GGandivaUInt32SelectionVector *
ggandiva_uint32_selection_vector_new(gint64 max_slots,
                                     GError **error);


#define GGANDIVA_TYPE_UINT64_SELECTION_VECTOR   \
  (ggandiva_uint64_selection_vector_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt64SelectionVector,
                         ggandiva_uint64_selection_vector,
                         GGANDIVA,
                         UINT64_SELECTION_VECTOR,
                         GGandivaSelectionVector)

struct _GGandivaUInt64SelectionVectorClass
{
  GGandivaSelectionVectorClass parent_class;
};

GGANDIVA_AVAILABLE_IN_4_0
GGandivaUInt64SelectionVector *
ggandiva_uint64_selection_vector_new(gint64 max_slots,
                                     GError **error);


G_END_DECLS
