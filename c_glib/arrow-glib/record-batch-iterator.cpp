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

#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/record-batch-iterator.hpp>

#include <arrow/util/iterator.h>

G_BEGIN_DECLS

typedef struct GArrowRecordBatchIteratorPrivate_ {
  arrow::RecordBatchIterator iterator;
} GArrowRecordBatchIteratorPrivate;

enum {
  PROP_ITERATOR = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchIterator,
                           garrow_record_batch_iterator,
                           G_TYPE_OBJECT)

#define GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(obj)        \
  static_cast<GArrowRecordBatchIteratorPrivate *>(           \
     garrow_record_batch_iterator_get_instance_private(      \
       GARROW_RECORD_BATCH_ITERATOR(obj)))

static void
garrow_record_batch_iterator_finalize(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(object);

  priv->iterator.~Iterator();

  G_OBJECT_CLASS(garrow_record_batch_iterator_parent_class)->finalize(object);
}

static void
garrow_record_batch_iterator_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ITERATOR:
    priv->iterator =
      std::move(*static_cast<arrow::RecordBatchIterator *>(g_value_get_pointer(value)));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_iterator_get_property(GObject *object,
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
garrow_record_batch_iterator_init(GArrowRecordBatchIterator *object)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(object);
  new(&priv->iterator) arrow::RecordBatchIterator;
}

static void
garrow_record_batch_iterator_class_init(GArrowRecordBatchIteratorClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_iterator_finalize;
  gobject_class->set_property = garrow_record_batch_iterator_set_property;
  gobject_class->get_property = garrow_record_batch_iterator_get_property;

  GParamSpec *spec;

  spec = g_param_spec_pointer("iterator",
                              "Iterator",
                              "The raw arrow::RecordBatchIterator",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ITERATOR, spec);
}

/**
 * garrow_record_batch_iterator_new:
 * @record_batches: (element-type GArrowRecordBatch):
 *   The record batches.
 *
 * Returns: A newly created #GArrowRecordBatchIterator.
 *
 * Since: 0.17.0
 */
GArrowRecordBatchIterator *
garrow_record_batch_iterator_new(GList *record_batches)
{
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  for (GList *node = record_batches; node; node = node->next) {
    auto record_batch = GARROW_RECORD_BATCH(node->data);
    arrow_record_batches.push_back(garrow_record_batch_get_raw(record_batch));
  }

  auto arrow_iterator = arrow::MakeVectorIterator(arrow_record_batches);
  return garrow_record_batch_iterator_new_raw(&arrow_iterator);
}

/**
 * garrow_record_batch_iterator_next:
 * @iterator: A #GArrowRecordBatchIterator.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next #GArrowRecordBatch, or %NULL when the iterator is completed.
 *
 * Since: 0.17.0
 */
GArrowRecordBatch *
garrow_record_batch_iterator_next(GArrowRecordBatchIterator *iterator,
                                  GError **error)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);

  auto result = priv->iterator.Next();
  if (garrow::check(error, result, "[record-batch-iterator][next]")) {
    auto arrow_record_batch = result.ValueOrDie();
    if (arrow_record_batch) {
      return garrow_record_batch_new_raw(&arrow_record_batch);
    }
  }
  return NULL;
}

/**
 * garrow_record_batch_iterator_equal:
 * @iterator: A #GArrowRecordBatchIterator.
 * @other_iterator: A #GArrowRecordBatchIterator to be compared.
 *
 * Returns: %TRUE if both iterators are the same, %FALSE otherwise.
 *
 * Since: 0.17.0
 */
gboolean
garrow_record_batch_iterator_equal(GArrowRecordBatchIterator *iterator,
                                   GArrowRecordBatchIterator *other_iterator)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);
  auto priv_other = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(other_iterator);
  return priv->iterator.Equals(priv_other->iterator);
}

/**
 * garrow_record_batch_iterator_to_list:
 * @iterator: A #GArrowRecordBatchIterator.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (element-type GArrowRecordBatch) (transfer full):
 *   A #GList contains every moved elements from the iterator.
 *
 * Since: 0.17.0
 */
GList*
garrow_record_batch_iterator_to_list(GArrowRecordBatchIterator *iterator,
                                     GError **error)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);
  auto result = priv->iterator.ToVector();
  if (garrow::check(error, result, "[record-batch-iterator][to-list]")) {
    auto arrow_record_batches = result.ValueOrDie();
    GList *record_batches = NULL;
    for (auto arrow_record_batch : arrow_record_batches) {
      auto record_batch = garrow_record_batch_new_raw(&arrow_record_batch);
      record_batches = g_list_prepend(record_batches, record_batch);
    }
    return g_list_reverse(record_batches);
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowRecordBatchIterator *
garrow_record_batch_iterator_new_raw(arrow::RecordBatchIterator *arrow_iterator)
{
  auto iterator = GARROW_RECORD_BATCH_ITERATOR(g_object_new(GARROW_TYPE_RECORD_BATCH_ITERATOR,
                                                            "iterator", arrow_iterator,
                                                            NULL));
  return iterator;
}

arrow::RecordBatchIterator *
garrow_record_batch_iterator_get_raw(GArrowRecordBatchIterator *iterator)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);
  return &priv->iterator;
}
