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

#include <limits>

#include <arrow-glib/basic-array.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <gandiva-glib/expression.hpp>
#include <gandiva-glib/filter.hpp>
#include <gandiva-glib/selection-vector.hpp>

G_BEGIN_DECLS

/**
 * SECTION: filter
 * @title: Filter classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaFilter is a class for selecting records by a specific
 * condition.
 *
 * Since: 4.0.0
 */

typedef struct GGandivaFilterPrivate_ {
  std::shared_ptr<gandiva::Filter> filter;
  GArrowSchema *schema;
  GGandivaCondition *condition;
} GGandivaFilterPrivate;

enum {
  PROP_FILTER = 1,
  PROP_SCHEMA,
  PROP_CONDITION,
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaFilter,
                           ggandiva_filter,
                           G_TYPE_OBJECT)

#define GGANDIVA_FILTER_GET_PRIVATE(obj)         \
  static_cast<GGandivaFilterPrivate *>(          \
     ggandiva_filter_get_instance_private(       \
       GGANDIVA_FILTER(obj)))

static void
ggandiva_filter_dispose(GObject *object)
{
  auto priv = GGANDIVA_FILTER_GET_PRIVATE(object);

  if (priv->schema) {
    g_object_unref(priv->schema);
    priv->schema = nullptr;
  }

  if (priv->condition) {
    g_object_unref(priv->condition);
    priv->condition = nullptr;
  }

  G_OBJECT_CLASS(ggandiva_filter_parent_class)->dispose(object);
}

static void
ggandiva_filter_finalize(GObject *object)
{
  auto priv = GGANDIVA_FILTER_GET_PRIVATE(object);

  priv->filter.~shared_ptr();

  G_OBJECT_CLASS(ggandiva_filter_parent_class)->finalize(object);
}

static void
ggandiva_filter_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GGANDIVA_FILTER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILTER:
    priv->filter =
      *static_cast<std::shared_ptr<gandiva::Filter> *>(g_value_get_pointer(value));
    break;
  case PROP_SCHEMA:
    priv->schema = GARROW_SCHEMA(g_value_dup_object(value));
    break;
  case PROP_CONDITION:
    priv->condition = GGANDIVA_CONDITION(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_filter_get_property(GObject *object,
                             guint prop_id,
                             GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GGANDIVA_FILTER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCHEMA:
    g_value_set_object(value, priv->schema);
    break;
  case PROP_CONDITION:
    g_value_set_object(value, priv->condition);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_filter_init(GGandivaFilter *object)
{
  auto priv = GGANDIVA_FILTER_GET_PRIVATE(object);
  new(&priv->filter) std::shared_ptr<gandiva::Filter>;
}

static void
ggandiva_filter_class_init(GGandivaFilterClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = ggandiva_filter_dispose;
  gobject_class->finalize     = ggandiva_filter_finalize;
  gobject_class->set_property = ggandiva_filter_set_property;
  gobject_class->get_property = ggandiva_filter_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("filter",
                              "Filter",
                              "The raw std::shared<gandiva::Filter> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILTER, spec);

  spec = g_param_spec_object("schema",
                             "Schema",
                             "The schema for input record batch",
                             GARROW_TYPE_SCHEMA,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCHEMA, spec);

  spec = g_param_spec_object("condition",
                             "Condition",
                             "The condition for the filter",
                             GGANDIVA_TYPE_CONDITION,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CONDITION, spec);
}

/**
 * ggandiva_filter_new:
 * @schema: A #GArrowSchema.
 * @condition: The condition to be used.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GGandivaFilter on success,
 *   %NULL on error.
 *
 * Since: 4.0.0
 */
GGandivaFilter *
ggandiva_filter_new(GArrowSchema *schema,
                    GGandivaCondition *condition,
                    GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto gandiva_condition = ggandiva_condition_get_raw(condition);
  std::shared_ptr<gandiva::Filter> gandiva_filter;
  auto status = gandiva::Filter::Make(arrow_schema,
                                      gandiva_condition,
                                      &gandiva_filter);
  if (garrow_error_check(error, status, "[gandiva][filter][new]")) {
    return ggandiva_filter_new_raw(&gandiva_filter, schema, condition);
  } else {
    return NULL;
  }
}

/**
 * ggandiva_filter_evaluate:
 * @filter: A #GGandivaFilter.
 * @record_batch: A #GArrowRecordBatch.
 * @selection_vector: A #GGandivaSelectionVector that is used as
 *   output.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * Since: 4.0.0
 */
gboolean
ggandiva_filter_evaluate(GGandivaFilter *filter,
                         GArrowRecordBatch *record_batch,
                         GGandivaSelectionVector *selection_vector,
                         GError **error)
{
  auto gandiva_filter = ggandiva_filter_get_raw(filter);
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto gandiva_selection_vector =
    ggandiva_selection_vector_get_raw(selection_vector);
  auto status = gandiva_filter->Evaluate(*arrow_record_batch,
                                         gandiva_selection_vector);
  return garrow_error_check(error, status, "[gandiva][filter][evaluate]");
}

G_END_DECLS

GGandivaFilter *
ggandiva_filter_new_raw(std::shared_ptr<gandiva::Filter> *gandiva_filter,
                        GArrowSchema *schema,
                        GGandivaCondition *condition)
{
  auto filter = g_object_new(GGANDIVA_TYPE_FILTER,
                             "filter", gandiva_filter,
                             "schema", schema,
                             "condition", condition,
                             NULL);
  return GGANDIVA_FILTER(filter);
}

std::shared_ptr<gandiva::Filter>
ggandiva_filter_get_raw(GGandivaFilter *filter)
{
  auto priv = GGANDIVA_FILTER_GET_PRIVATE(filter);
  return priv->filter;
}
