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
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <gandiva-glib/expression.hpp>
#include <gandiva-glib/projector.hpp>
#include <gandiva-glib/selection-vector.hpp>

G_BEGIN_DECLS

/**
 * SECTION: projector
 * @title: Projector classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaProjector is a class that evaluates given expressions
 * against the given record batches.
 *
 * #GGandivaSelectableProjector is a class that evaluates given expressions
 * against the given selected records in the given record batches.
 *
 * Since: 0.12.0
 */

typedef struct GGandivaProjectorPrivate_ {
  std::shared_ptr<gandiva::Projector> projector;
  GArrowSchema *schema;
  GList *expressions;
} GGandivaProjectorPrivate;

enum {
  PROP_PROJECTOR = 1,
  PROP_SCHEMA,
  PROP_EXPRESSIONS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaProjector,
                           ggandiva_projector,
                           G_TYPE_OBJECT)

#define GGANDIVA_PROJECTOR_GET_PRIVATE(obj)         \
  static_cast<GGandivaProjectorPrivate *>(          \
     ggandiva_projector_get_instance_private(       \
       GGANDIVA_PROJECTOR(obj)))

static void
ggandiva_projector_dispose(GObject *object)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(object);

  if (priv->schema) {
    g_object_unref(G_OBJECT(priv->schema));
    priv->schema = nullptr;
  }

  g_list_free_full(priv->expressions, g_object_unref);
  priv->expressions = nullptr;

  G_OBJECT_CLASS(ggandiva_projector_parent_class)->dispose(object);
}

static void
ggandiva_projector_finalize(GObject *object)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(object);

  priv->projector.~shared_ptr();

  G_OBJECT_CLASS(ggandiva_projector_parent_class)->finalize(object);
}

static void
ggandiva_projector_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_PROJECTOR:
    priv->projector =
      *static_cast<std::shared_ptr<gandiva::Projector> *>(g_value_get_pointer(value));
    break;
  case PROP_SCHEMA:
    priv->schema = GARROW_SCHEMA(g_value_dup_object(value));
    break;
  case PROP_EXPRESSIONS:
    priv->expressions =
      g_list_copy_deep(static_cast<GList *>(g_value_get_pointer(value)),
                       reinterpret_cast<GCopyFunc>(g_object_ref),
                       nullptr);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_projector_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCHEMA:
    g_value_set_object(value, priv->schema);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_projector_init(GGandivaProjector *object)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(object);
  new(&priv->projector) std::shared_ptr<gandiva::Projector>;
}

static void
ggandiva_projector_class_init(GGandivaProjectorClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = ggandiva_projector_dispose;
  gobject_class->finalize     = ggandiva_projector_finalize;
  gobject_class->set_property = ggandiva_projector_set_property;
  gobject_class->get_property = ggandiva_projector_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("projector",
                              "Projector",
                              "The raw std::shared<gandiva::Projector> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PROJECTOR, spec);

  spec = g_param_spec_object("schema",
                             "Schema",
                             "The schema of the projector",
                             GARROW_TYPE_SCHEMA,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCHEMA, spec);

  spec = g_param_spec_pointer("expressions",
                              "Expressions",
                              "The expressions for the projector",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXPRESSIONS, spec);
}

/**
 * ggandiva_projector_new:
 * @schema: A #GArrowSchema.
 * @expressions: (element-type GGandivaExpression): The built expressions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GGandivaProjector on success,
 *   %NULL on error.
 *
 * Since: 0.12.0
 */
GGandivaProjector *
ggandiva_projector_new(GArrowSchema *schema,
                       GList *expressions,
                       GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<gandiva::Expression>> gandiva_expressions;
  for (auto node = expressions; node; node = g_list_next(node)) {
    auto expression = GGANDIVA_EXPRESSION(node->data);
    auto gandiva_expression = ggandiva_expression_get_raw(expression);
    gandiva_expressions.push_back(gandiva_expression);
  }
  std::shared_ptr<gandiva::Projector> gandiva_projector;
  auto status =
    gandiva_projector->Make(arrow_schema,
                            gandiva_expressions,
                            &gandiva_projector);
  if (garrow_error_check(error, status, "[gandiva][projector][new]")) {
    return ggandiva_projector_new_raw(&gandiva_projector,
                                      schema,
                                      expressions);
  } else {
    return NULL;
  }
}

/**
 * ggandiva_projector_evaluate:
 * @projector: A #GGandivaProjector.
 * @record_batch: A #GArrowRecordBatch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (element-type GArrowArray) (nullable) (transfer full):
 *   The #GArrowArray as the result evaluated on success, %NULL on error.
 *
 * Since: 0.12.0
 */
GList *
ggandiva_projector_evaluate(GGandivaProjector *projector,
                            GArrowRecordBatch *record_batch,
                            GError **error)
{
  auto gandiva_projector = ggandiva_projector_get_raw(projector);
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto memory_pool = arrow::default_memory_pool();
  arrow::ArrayVector arrow_arrays;
  auto status =
    gandiva_projector->Evaluate(*arrow_record_batch,
                                memory_pool,
                                &arrow_arrays);
  if (garrow_error_check(error, status, "[gandiva][projector][evaluate]")) {
    GList *arrays = NULL;
    for (auto arrow_array : arrow_arrays) {
      auto array = garrow_array_new_raw(&arrow_array);
      arrays = g_list_prepend(arrays, array);
    }
    return g_list_reverse(arrays);
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GGandivaSelectableProjector,
              ggandiva_selectable_projector,
              GGANDIVA_TYPE_PROJECTOR)

static void
ggandiva_selectable_projector_init(GGandivaSelectableProjector *object)
{
}

static void
ggandiva_selectable_projector_class_init(GGandivaSelectableProjectorClass *klass)
{
}

/**
 * ggandiva_selectable_projector_new:
 * @schema: A #GArrowSchema.
 * @expressions: (element-type GGandivaExpression): The built expressions.
 * @mode: A #GGandivaSelectionVectorMode to be used.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GGandivaProjector on success,
 *   %NULL on error.
 *
 * Since: 4.0.0
 */
GGandivaSelectableProjector *
ggandiva_selectable_projector_new(GArrowSchema *schema,
                                  GList *expressions,
                                  GGandivaSelectionVectorMode mode,
                                  GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<gandiva::Expression>> gandiva_expressions;
  for (auto node = expressions; node; node = g_list_next(node)) {
    auto expression = GGANDIVA_EXPRESSION(node->data);
    auto gandiva_expression = ggandiva_expression_get_raw(expression);
    gandiva_expressions.push_back(gandiva_expression);
  }
  auto gandiva_mode = static_cast<gandiva::SelectionVector::Mode>(mode);
  auto gandiva_configuration =
    gandiva::ConfigurationBuilder::DefaultConfiguration();
  std::shared_ptr<gandiva::Projector> gandiva_projector;
  auto status = gandiva_projector->Make(arrow_schema,
                                        gandiva_expressions,
                                        gandiva_mode,
                                        gandiva_configuration,
                                        &gandiva_projector);
  if (garrow_error_check(error,
                         status,
                         "[gandiva][selectable-projector][new]")) {
    return ggandiva_selectable_projector_new_raw(&gandiva_projector,
                                                 schema,
                                                 expressions);
  } else {
    return NULL;
  }
}

/**
 * ggandiva_selectable_projector_evaluate:
 * @projector: A #GGandivaSelectableProjector.
 * @record_batch: A #GArrowRecordBatch.
 * @selection_vector: A #GGandivaSelectionVector that specifies
 *   the filtered row positions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (element-type GArrowArray) (nullable) (transfer full):
 *   The #GArrowArray as the result evaluated on success, %NULL on error.
 *
 * Since: 4.0.0
 */
GList *
ggandiva_selectable_projector_evaluate(
  GGandivaSelectableProjector *projector,
  GArrowRecordBatch *record_batch,
  GGandivaSelectionVector *selection_vector,
  GError **error)
{
  auto gandiva_projector =
    ggandiva_projector_get_raw(GGANDIVA_PROJECTOR(projector));
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto gandiva_selection_vector =
    ggandiva_selection_vector_get_raw(selection_vector).get();
  auto memory_pool = arrow::default_memory_pool();
  arrow::ArrayVector arrow_arrays;
  auto status =
    gandiva_projector->Evaluate(*arrow_record_batch,
                                gandiva_selection_vector,
                                memory_pool,
                                &arrow_arrays);
  if (garrow_error_check(error,
                         status,
                         "[gandiva][selectable-projector][evaluate]")) {
    GList *arrays = NULL;
    for (auto arrow_array : arrow_arrays) {
      auto array = garrow_array_new_raw(&arrow_array);
      arrays = g_list_prepend(arrays, array);
    }
    return g_list_reverse(arrays);
  } else {
    return NULL;
  }
}

G_END_DECLS

GGandivaProjector *
ggandiva_projector_new_raw(
  std::shared_ptr<gandiva::Projector> *gandiva_projector,
  GArrowSchema *schema,
  GList *expressions)
{
  auto projector = g_object_new(GGANDIVA_TYPE_PROJECTOR,
                                "projector", gandiva_projector,
                                "schema", schema,
                                "expressions", expressions,
                                NULL);
  return GGANDIVA_PROJECTOR(projector);
}

GGandivaSelectableProjector *
ggandiva_selectable_projector_new_raw(
  std::shared_ptr<gandiva::Projector> *gandiva_projector,
  GArrowSchema *schema,
  GList *expressions)
{
  auto projector = g_object_new(GGANDIVA_TYPE_SELECTABLE_PROJECTOR,
                                "projector", gandiva_projector,
                                NULL);
  return GGANDIVA_SELECTABLE_PROJECTOR(projector);
}

std::shared_ptr<gandiva::Projector>
ggandiva_projector_get_raw(GGandivaProjector *projector)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(projector);
  return priv->projector;
}
