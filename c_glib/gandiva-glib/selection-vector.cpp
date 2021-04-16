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

#include <arrow-glib/basic-array.hpp>
#include <arrow-glib/error.hpp>

#include <gandiva-glib/selection-vector.hpp>


G_BEGIN_DECLS

/**
 * SECTION: selection-vector
 * @section_id: selection-vector-classes
 * @title: Selection vector classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaSelectionVector is a base class for a selection vector.
 *
 * #GGandivaUInt16SelectionVector is a class for a selection vector
 * that uses 16-bit unsigned integer for each index.
 *
 * #GGandivaUInt32SelectionVector is a class for a selection vector
 * that uses 32-bit unsigned integer for each index.
 *
 * #GGandivaUInt64SelectionVector is a class for a selection vector
 * that uses 64-bit unsigned integer for each index.
 *
 * Since: 4.0.0
 */

typedef struct GGandivaSelectionVectorPrivate_ {
  std::shared_ptr<gandiva::SelectionVector> selection_vector;
} GGandivaSelectionVectorPrivate;

enum {
  PROP_SELECTION_VECTOR = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GGandivaSelectionVector,
                                    ggandiva_selection_vector,
                                    G_TYPE_OBJECT)

#define GGANDIVA_SELECTION_VECTOR_GET_PRIVATE(object)           \
  static_cast<GGandivaSelectionVectorPrivate *>(                \
    ggandiva_selection_vector_get_instance_private(             \
      GGANDIVA_SELECTION_VECTOR(object)))

static void
ggandiva_selection_vector_finalize(GObject *object)
{
  auto priv = GGANDIVA_SELECTION_VECTOR_GET_PRIVATE(object);

  priv->selection_vector.~shared_ptr();

  G_OBJECT_CLASS(ggandiva_selection_vector_parent_class)->finalize(object);
}

static void
ggandiva_selection_vector_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GGANDIVA_SELECTION_VECTOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SELECTION_VECTOR:
    priv->selection_vector =
      *static_cast<std::shared_ptr<gandiva::SelectionVector> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_selection_vector_init(GGandivaSelectionVector *object)
{
  auto priv = GGANDIVA_SELECTION_VECTOR_GET_PRIVATE(object);
  new(&priv->selection_vector) std::shared_ptr<gandiva::SelectionVector>;
}

static void
ggandiva_selection_vector_class_init(GGandivaSelectionVectorClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_selection_vector_finalize;
  gobject_class->set_property = ggandiva_selection_vector_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("selection-vector",
                              "Selection vector",
                              "The raw std::shared<gandiva::SelectionVector> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SELECTION_VECTOR, spec);
}

/**
 * ggandiva_selection_vector_get_mode:
 * @selection_vector: A #GGandivaSelectionVector.
 *
 * Returns: A #GGandivaSelectionVectorMode for the selection vector.
 *
 * Since: 4.0.0
 */
GGandivaSelectionVectorMode
ggandiva_selection_vector_get_mode(GGandivaSelectionVector *selection_vector)
{
  auto gandiva_selection_vector =
    ggandiva_selection_vector_get_raw(selection_vector);
  auto gandiva_mode = gandiva_selection_vector->GetMode();
  return static_cast<GGandivaSelectionVectorMode>(gandiva_mode);
}

/**
 * ggandiva_selection_vector_to_array:
 * @selection_vector: A #GGandivaSelectionVector.
 *
 * Returns: (transfer full): A #GArrowArray that has the same content
 *   of the selection vector.
 *
 * Since: 4.0.0
 */
GArrowArray *
ggandiva_selection_vector_to_array(GGandivaSelectionVector *selection_vector)
{
  auto gandiva_selection_vector =
    ggandiva_selection_vector_get_raw(selection_vector);
  auto arrow_array = gandiva_selection_vector->ToArray();
  return garrow_array_new_raw(&arrow_array);
}


G_DEFINE_TYPE(GGandivaUInt16SelectionVector,
              ggandiva_uint16_selection_vector,
              GGANDIVA_TYPE_SELECTION_VECTOR)

static void
ggandiva_uint16_selection_vector_init(
  GGandivaUInt16SelectionVector *selection_vector)
{
}

static void
ggandiva_uint16_selection_vector_class_init(
  GGandivaUInt16SelectionVectorClass *klass)
{
}

/**
 * ggandiva_uint16_selection_vector_new:
 * @max_slots: The max number of slots.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GGandivaUInt16SelectionVector.
 *
 * Since: 4.0.0
 */
GGandivaUInt16SelectionVector *
ggandiva_uint16_selection_vector_new(gint64 max_slots,
                                     GError **error)
{
  auto memory_pool = arrow::default_memory_pool();
  std::shared_ptr<gandiva::SelectionVector> gandiva_selection_vector;
  auto status = gandiva::SelectionVector::MakeInt16(max_slots,
                                                    memory_pool,
                                                    &gandiva_selection_vector);
  if (garrow_error_check(error,
                         status,
                         "[gandiva][uint16-selection-vector][new]")) {
    return GGANDIVA_UINT16_SELECTION_VECTOR(
      ggandiva_selection_vector_new_raw(&gandiva_selection_vector));
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GGandivaUInt32SelectionVector,
              ggandiva_uint32_selection_vector,
              GGANDIVA_TYPE_SELECTION_VECTOR)

static void
ggandiva_uint32_selection_vector_init(
  GGandivaUInt32SelectionVector *selection_vector)
{
}

static void
ggandiva_uint32_selection_vector_class_init(
  GGandivaUInt32SelectionVectorClass *klass)
{
}

/**
 * ggandiva_uint32_selection_vector_new:
 * @max_slots: The max number of slots.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GGandivaUInt32SelectionVector.
 *
 * Since: 4.0.0
 */
GGandivaUInt32SelectionVector *
ggandiva_uint32_selection_vector_new(gint64 max_slots,
                                     GError **error)
{
  auto memory_pool = arrow::default_memory_pool();
  std::shared_ptr<gandiva::SelectionVector> gandiva_selection_vector;
  auto status = gandiva::SelectionVector::MakeInt32(max_slots,
                                                    memory_pool,
                                                    &gandiva_selection_vector);
  if (garrow_error_check(error,
                         status,
                         "[gandiva][uint32-selection-vector][new]")) {
    return GGANDIVA_UINT32_SELECTION_VECTOR(
      ggandiva_selection_vector_new_raw(&gandiva_selection_vector));
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GGandivaUInt64SelectionVector,
              ggandiva_uint64_selection_vector,
              GGANDIVA_TYPE_SELECTION_VECTOR)

static void
ggandiva_uint64_selection_vector_init(
  GGandivaUInt64SelectionVector *selection_vector)
{
}

static void
ggandiva_uint64_selection_vector_class_init(
  GGandivaUInt64SelectionVectorClass *klass)
{
}

/**
 * ggandiva_uint64_selection_vector_new:
 * @max_slots: The max number of slots.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GGandivaUInt64SelectionVector.
 *
 * Since: 4.0.0
 */
GGandivaUInt64SelectionVector *
ggandiva_uint64_selection_vector_new(gint64 max_slots,
                                     GError **error)
{
  auto memory_pool = arrow::default_memory_pool();
  std::shared_ptr<gandiva::SelectionVector> gandiva_selection_vector;
  auto status = gandiva::SelectionVector::MakeInt64(max_slots,
                                                    memory_pool,
                                                    &gandiva_selection_vector);
  if (garrow_error_check(error,
                         status,
                         "[gandiva][uint64-selection-vector][new]")) {
    return GGANDIVA_UINT64_SELECTION_VECTOR(
      ggandiva_selection_vector_new_raw(&gandiva_selection_vector));
  } else {
    return NULL;
  }
}


G_END_DECLS


GGandivaSelectionVector *
ggandiva_selection_vector_new_raw(
  std::shared_ptr<gandiva::SelectionVector> *gandiva_selection_vector)
{
  GType type = GGANDIVA_TYPE_SELECTION_VECTOR;
  switch ((*gandiva_selection_vector)->GetMode()) {
  case gandiva::SelectionVector::Mode::MODE_UINT16:
    type = GGANDIVA_TYPE_UINT16_SELECTION_VECTOR;
    break;
  case gandiva::SelectionVector::Mode::MODE_UINT32:
    type = GGANDIVA_TYPE_UINT32_SELECTION_VECTOR;
    break;
  case gandiva::SelectionVector::Mode::MODE_UINT64:
    type = GGANDIVA_TYPE_UINT64_SELECTION_VECTOR;
    break;
  default:
    break;
  }
  auto selection_vector =
    g_object_new(type,
                 "selection-vector", gandiva_selection_vector,
                 NULL);
  return GGANDIVA_SELECTION_VECTOR(selection_vector);
}

std::shared_ptr<gandiva::SelectionVector>
ggandiva_selection_vector_get_raw(GGandivaSelectionVector *selection_vector)
{
  auto priv = GGANDIVA_SELECTION_VECTOR_GET_PRIVATE(selection_vector);
  return priv->selection_vector;
}
