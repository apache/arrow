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

#include <arrow-glib/basic-array.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-glib/error.hpp>
#include <gandiva-glib/expression.hpp>
#include <gandiva-glib/projector.hpp>

G_BEGIN_DECLS

/**
 * SECTION: projector
 * @title: Projector classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaProjector is a class for building a specific schema
 * and vector of expressions.
 *
 * Since: 0.12.0
 */

typedef struct GGandivaProjectorPrivate_ {
  std::shared_ptr<gandiva::Projector> projector;
} GGandivaProjectorPrivate;

enum {
  PROP_0,
  PROP_PROJECTOR
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaProjector,
                           ggandiva_projector,
                           G_TYPE_OBJECT)

#define GGANDIVA_PROJECTOR_GET_PRIVATE(obj)                 \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                       \
                               GGANDIVA_TYPE_PROJECTOR,     \
                               GGandivaProjectorPrivate))

static void
ggandiva_projector_finalize(GObject *object)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(object);

  priv->projector = nullptr;

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
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_projector_init(GGandivaProjector *object)
{
}

static void
ggandiva_projector_class_init(GGandivaProjectorClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_projector_finalize;
  gobject_class->set_property = ggandiva_projector_set_property;

  spec = g_param_spec_pointer("projector",
                              "Projector",
                              "The raw std::shared<gandiva::Projector> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PROJECTOR, spec);
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
    return ggandiva_projector_new_raw(&gandiva_projector);
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

G_END_DECLS

GGandivaProjector *
ggandiva_projector_new_raw(std::shared_ptr<gandiva::Projector> *gandiva_projector)
{
  auto projector = g_object_new(GGANDIVA_TYPE_PROJECTOR,
                                "projector", gandiva_projector,
                                NULL);
  return GGANDIVA_PROJECTOR(projector);
}

std::shared_ptr<gandiva::Projector>
ggandiva_projector_get_raw(GGandivaProjector *projector)
{
  auto priv = GGANDIVA_PROJECTOR_GET_PRIVATE(projector);
  return priv->projector;
}
