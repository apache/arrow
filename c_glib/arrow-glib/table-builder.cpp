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

#include <arrow-glib/array-builder.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table-builder.hpp>

G_BEGIN_DECLS

/**
 * SECTION: table-builder
 * @section_id: table-builder-classes
 * @title: Table builder classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowRecordBatchBuilder is a class to create
 * new #GArrowRecordBatch.
 */

typedef struct GArrowRecordBatchBuilderPrivate_ {
  arrow::RecordBatchBuilder *record_batch_builder;
  GPtrArray *fields;
} GArrowRecordBatchBuilderPrivate;

enum {
  PROP_0,
  PROP_RECORD_BATCH_BUILDER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchBuilder,
                           garrow_record_batch_builder,
                           G_TYPE_OBJECT)

#define GARROW_RECORD_BATCH_BUILDER_GET_PRIVATE(object)            \
  static_cast<GArrowRecordBatchBuilderPrivate *>(                  \
    garrow_record_batch_builder_get_instance_private(              \
      GARROW_RECORD_BATCH_BUILDER(object)))

static void
garrow_record_batch_builder_constructed(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_BUILDER_GET_PRIVATE(object);
  auto arrow_builder = priv->record_batch_builder;
  auto n_fields = arrow_builder->num_fields();
  priv->fields = g_ptr_array_new_full(n_fields, g_object_unref);
  for (int i = 0; i < n_fields; ++i) {
    auto arrow_array_builder = arrow_builder->GetField(i);
    auto array_builder = garrow_array_builder_new_raw(arrow_array_builder);
    garrow_array_builder_release_ownership(array_builder);
    g_ptr_array_add(priv->fields, array_builder);
  }

  G_OBJECT_CLASS(garrow_record_batch_builder_parent_class)->constructed(object);
}

static void
garrow_record_batch_builder_finalize(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_BUILDER_GET_PRIVATE(object);

  g_ptr_array_free(priv->fields, TRUE);
  delete priv->record_batch_builder;

  G_OBJECT_CLASS(garrow_record_batch_builder_parent_class)->finalize(object);
}

static void
garrow_record_batch_builder_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_BUILDER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RECORD_BATCH_BUILDER:
    priv->record_batch_builder =
      static_cast<arrow::RecordBatchBuilder *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_builder_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_builder_init(GArrowRecordBatchBuilder *builder)
{
}

static void
garrow_record_batch_builder_class_init(GArrowRecordBatchBuilderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->constructed  = garrow_record_batch_builder_constructed;
  gobject_class->finalize     = garrow_record_batch_builder_finalize;
  gobject_class->set_property = garrow_record_batch_builder_set_property;
  gobject_class->get_property = garrow_record_batch_builder_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("record-batch-builder",
                              "RecordBatch builder",
                              "The raw arrow::RecordBatchBuilder *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class,
                                  PROP_RECORD_BATCH_BUILDER,
                                  spec);
}

/**
 * garrow_record_batch_builder_new:
 * @schema: A #GArrowSchema.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GArrowRecordBatchBuilder on success,
 *   %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowRecordBatchBuilder *
garrow_record_batch_builder_new(GArrowSchema *schema, GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto memory_pool = arrow::default_memory_pool();
  std::unique_ptr<arrow::RecordBatchBuilder> arrow_builder;
  auto status = arrow::RecordBatchBuilder::Make(arrow_schema,
                                                memory_pool,
                                                &arrow_builder);
  if (garrow_error_check(error, status, "[record-batch-builder][new]")) {
    return garrow_record_batch_builder_new_raw(arrow_builder.release());
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_builder_get_initial_capacity:
 * @builder: A #GArrowRecordBatchBuilder.
 *
 * Returns: The initial capacity for array builders.
 *
 * Since: 0.8.0
 */
gint64
garrow_record_batch_builder_get_initial_capacity(GArrowRecordBatchBuilder *builder)
{
  auto arrow_builder = garrow_record_batch_builder_get_raw(builder);
  return arrow_builder->initial_capacity();
}

/**
 * garrow_record_batch_builder_set_initial_capacity:
 * @builder: A #GArrowRecordBatchBuilder.
 * @capacity: The new initial capacity for array builders.
 *
 * Since: 0.8.0
 */
void
garrow_record_batch_builder_set_initial_capacity(GArrowRecordBatchBuilder *builder,
                                                 gint64 capacity)
{
  auto arrow_builder = garrow_record_batch_builder_get_raw(builder);
  arrow_builder->SetInitialCapacity(capacity);
}

/**
 * garrow_record_batch_builder_get_schema:
 * @builder: A #GArrowRecordBatchBuilder.
 *
 * Returns: (transfer full): The #GArrowSchema of the record batch builder.
 *
 * Since: 0.8.0
 */
GArrowSchema *
garrow_record_batch_builder_get_schema(GArrowRecordBatchBuilder *builder)
{
  auto arrow_builder = garrow_record_batch_builder_get_raw(builder);
  auto arrow_schema = arrow_builder->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_record_batch_builder_get_n_fields:
 * @builder: A #GArrowRecordBatchBuilder.
 *
 * Returns: The number of fields.
 *
 * Since: 0.8.0
 */
gint
garrow_record_batch_builder_get_n_fields(GArrowRecordBatchBuilder *builder)
{
  auto arrow_builder = garrow_record_batch_builder_get_raw(builder);
  return arrow_builder->num_fields();
}

/**
 * garrow_record_batch_builder_get_field:
 * @builder: A #GArrowRecordBatchBuilder.
 * @i: The field index. If it's negative, index is counted backward
 *   from the end of the fields. `-1` means the last field.
 *
 * Returns: (transfer none) (nullable): The #GArrowArrayBuilder for
 *   the `i`-th field on success, %NULL on out of index.
 *
 * Since: 0.8.0
 */
GArrowArrayBuilder *
garrow_record_batch_builder_get_field(GArrowRecordBatchBuilder *builder,
                                      gint i)
{
  auto priv = GARROW_RECORD_BATCH_BUILDER_GET_PRIVATE(builder);
  if (i < 0) {
    i += priv->fields->len;
  }
  if (i < 0) {
    return NULL;
  }
  if (static_cast<guint>(i) >= priv->fields->len) {
    return NULL;
  }

  return GARROW_ARRAY_BUILDER(g_ptr_array_index(priv->fields, i));
}

/**
 * garrow_record_batch_builder_flush:
 * @builder: A #GArrowRecordBatchBuilder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The built #GArrowRecordBatch on success,
 *   %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowRecordBatch *
garrow_record_batch_builder_flush(GArrowRecordBatchBuilder *builder,
                                  GError **error)
{
  auto arrow_builder = garrow_record_batch_builder_get_raw(builder);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_builder->Flush(&arrow_record_batch);
  if (garrow_error_check(error, status, "[record-batch-builder][flush]")) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowRecordBatchBuilder *
garrow_record_batch_builder_new_raw(arrow::RecordBatchBuilder *arrow_builder)
{
  auto builder = g_object_new(GARROW_TYPE_RECORD_BATCH_BUILDER,
                              "record-batch-builder", arrow_builder,
                              NULL);
  return GARROW_RECORD_BATCH_BUILDER(builder);
}

arrow::RecordBatchBuilder *
garrow_record_batch_builder_get_raw(GArrowRecordBatchBuilder *builder)
{
  auto priv = GARROW_RECORD_BATCH_BUILDER_GET_PRIVATE(builder);
  return priv->record_batch_builder;
}
