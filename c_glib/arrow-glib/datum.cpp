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

#include <arrow-glib/array.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/datum.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/table.hpp>

G_BEGIN_DECLS

/**
 * SECTION: datum
 * @section_id: datum-classes
 * @title: Datum classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowDatum is an abstract class to hold a datum. Subclasses such
 * as #GArrowArrayDatum and #GArrowTableDatum can hold a specific
 * datum.
 *
 * #GArrowArrayDatum is a class to hold an #GArrowArray.
 *
 * #GArrowChunkedArrayDatum is a class to hold an #GArrowChunkedArray.
 *
 * #GArrowRecordBatchDatum is a class to hold an #GArrowRecordBatch.
 *
 * #GArrowTableDatum is a class to hold an #GArrowTable.
 */

typedef struct GArrowDatumPrivate_ {
  std::shared_ptr<arrow::Datum> datum;
} GArrowDatumPrivate;

enum {
  PROP_DATUM = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowDatum, garrow_datum, G_TYPE_OBJECT)

#define GARROW_DATUM_GET_PRIVATE(obj)         \
  static_cast<GArrowDatumPrivate *>(          \
    garrow_datum_get_instance_private(        \
      GARROW_DATUM(obj)))

static void
garrow_datum_finalize(GObject *object)
{
  auto priv = GARROW_DATUM_GET_PRIVATE(object);

  priv->datum.~shared_ptr();

  G_OBJECT_CLASS(garrow_datum_parent_class)->finalize(object);
}

static void
garrow_datum_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GARROW_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATUM:
    priv->datum =
      *static_cast<std::shared_ptr<arrow::Datum> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_datum_init(GArrowDatum *object)
{
  auto priv = GARROW_DATUM_GET_PRIVATE(object);
  new(&priv->datum) std::shared_ptr<arrow::Datum>;
}

static void
garrow_datum_class_init(GArrowDatumClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_datum_finalize;
  gobject_class->set_property = garrow_datum_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("datum",
                              "Datum",
                              "The raw std::shared_ptr<arrow::Datum> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATUM, spec);
}

/**
 * garrow_datum_is_array:
 * @datum: A #GArrowDatum.
 *
 * Returns: %TRUE if the datum holds a #GArrowArray, %FALSE
 *   otherwise.
 *
 * Since: 1.0.0
 */
gboolean
garrow_datum_is_array(GArrowDatum *datum)
{
  const auto arrow_datum = garrow_datum_get_raw(datum);
  return arrow_datum->is_array();
}

/**
 * garrow_datum_is_array_like:
 * @datum: A #GArrowDatum.
 *
 * Returns: %TRUE if the datum holds a #GArrowArray or
 *   #GArrowChunkedArray, %FALSE otherwise.
 *
 * Since: 1.0.0
 */
gboolean
garrow_datum_is_array_like(GArrowDatum *datum)
{
  const auto arrow_datum = garrow_datum_get_raw(datum);
  return arrow_datum->is_arraylike();
}

/**
 * garrow_datum_equal:
 * @datum: A #GArrowDatum.
 * @other_datum: A #GArrowDatum to be compared.
 *
 * Returns: %TRUE if both of them have the same datum, %FALSE
 *   otherwise.
 *
 * Since: 1.0.0
 */
gboolean
garrow_datum_equal(GArrowDatum *datum, GArrowDatum *other_datum)
{
  const auto arrow_datum = garrow_datum_get_raw(datum);
  const auto arrow_other_datum = garrow_datum_get_raw(other_datum);
  return arrow_datum->Equals(*arrow_other_datum);
}

/**
 * garrow_datum_to_string:
 * @datum: A #GArrowDatum.
 *
 * Returns: (transfer full): The formatted datum content.
 *
 *   The returned string should be freed when with g_free() when no
 *   longer needed.
 *
 * Since: 1.0.0
 */
gchar *
garrow_datum_to_string(GArrowDatum *datum)
{
  const auto arrow_datum = garrow_datum_get_raw(datum);
  return g_strdup(arrow_datum->ToString().c_str());
}


typedef struct GArrowArrayDatumPrivate_ {
  GArrowArray *value;
} GArrowArrayDatumPrivate;

enum {
  PROP_VALUE = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowArrayDatum,
                           garrow_array_datum,
                           GARROW_TYPE_DATUM)

#define GARROW_ARRAY_DATUM_GET_PRIVATE(obj)         \
  static_cast<GArrowArrayDatumPrivate *>(           \
    garrow_array_datum_get_instance_private(        \
      GARROW_ARRAY_DATUM(obj)))

static void
garrow_array_datum_dispose(GObject *object)
{
  auto priv = GARROW_ARRAY_DATUM_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_array_datum_parent_class)->dispose(object);
}

static void
garrow_array_datum_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_datum_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    g_value_set_object(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_datum_init(GArrowArrayDatum *object)
{
}

static void
garrow_array_datum_class_init(GArrowArrayDatumClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_array_datum_dispose;
  gobject_class->set_property = garrow_array_datum_set_property;
  gobject_class->get_property = garrow_array_datum_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("value",
                             "Value",
                             "The array held by this datum",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

/**
 * garrow_array_datum_new:
 * @value: A #GArrowArray.
 *
 * Returns: A newly created #GArrowArrayDatum.
 *
 * Since: 1.0.0
 */
GArrowArrayDatum *
garrow_array_datum_new(GArrowArray *value)
{
  auto arrow_value = garrow_array_get_raw(value);
  auto arrow_datum = std::make_shared<arrow::Datum>(arrow_value);
  return garrow_array_datum_new_raw(&arrow_datum, value);
}


typedef struct GArrowChunkedArrayDatumPrivate_ {
  GArrowChunkedArray *value;
} GArrowChunkedArrayDatumPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowChunkedArrayDatum,
                           garrow_chunked_array_datum,
                           GARROW_TYPE_DATUM)

#define GARROW_CHUNKED_ARRAY_DATUM_GET_PRIVATE(obj)     \
  static_cast<GArrowChunkedArrayDatumPrivate *>(        \
    garrow_chunked_array_datum_get_instance_private(    \
      GARROW_CHUNKED_ARRAY_DATUM(obj)))

static void
garrow_chunked_array_datum_dispose(GObject *object)
{
  auto priv = GARROW_CHUNKED_ARRAY_DATUM_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_array_datum_parent_class)->dispose(object);
}

static void
garrow_chunked_array_datum_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_CHUNKED_ARRAY_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_CHUNKED_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_chunked_array_datum_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_CHUNKED_ARRAY_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    g_value_set_object(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_chunked_array_datum_init(GArrowChunkedArrayDatum *object)
{
}

static void
garrow_chunked_array_datum_class_init(GArrowChunkedArrayDatumClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_chunked_array_datum_dispose;
  gobject_class->set_property = garrow_chunked_array_datum_set_property;
  gobject_class->get_property = garrow_chunked_array_datum_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("value",
                             "Value",
                             "The chunked array held by this datum",
                             GARROW_TYPE_CHUNKED_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

/**
 * garrow_chunked_array_datum_new:
 * @value: A #GArrowChunkedArray.
 *
 * Returns: A newly created #GArrowChunkedArrayDatum.
 *
 * Since: 1.0.0
 */
GArrowChunkedArrayDatum *
garrow_chunked_array_datum_new(GArrowChunkedArray *value)
{
  auto arrow_value = garrow_chunked_array_get_raw(value);
  auto arrow_datum = std::make_shared<arrow::Datum>(arrow_value);
  return garrow_chunked_array_datum_new_raw(&arrow_datum, value);
}


typedef struct GArrowRecordBatchDatumPrivate_ {
  GArrowRecordBatch *value;
} GArrowRecordBatchDatumPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchDatum,
                           garrow_record_batch_datum,
                           GARROW_TYPE_DATUM)

#define GARROW_RECORD_BATCH_DATUM_GET_PRIVATE(obj)     \
  static_cast<GArrowRecordBatchDatumPrivate *>(        \
    garrow_record_batch_datum_get_instance_private(    \
      GARROW_RECORD_BATCH_DATUM(obj)))

static void
garrow_record_batch_datum_dispose(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_DATUM_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_array_datum_parent_class)->dispose(object);
}

static void
garrow_record_batch_datum_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_RECORD_BATCH(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_datum_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    g_value_set_object(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_datum_init(GArrowRecordBatchDatum *object)
{
}

static void
garrow_record_batch_datum_class_init(GArrowRecordBatchDatumClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_record_batch_datum_dispose;
  gobject_class->set_property = garrow_record_batch_datum_set_property;
  gobject_class->get_property = garrow_record_batch_datum_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("value",
                             "Value",
                             "The chunked array held by this datum",
                             GARROW_TYPE_RECORD_BATCH,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

/**
 * garrow_record_batch_datum_new:
 * @value: A #GArrowRecordBatch.
 *
 * Returns: A newly created #GArrowRecordBatchDatum.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchDatum *
garrow_record_batch_datum_new(GArrowRecordBatch *value)
{
  auto arrow_value = garrow_record_batch_get_raw(value);
  auto arrow_datum = std::make_shared<arrow::Datum>(arrow_value);
  return garrow_record_batch_datum_new_raw(&arrow_datum, value);
}


typedef struct GArrowTableDatumPrivate_ {
  GArrowTable *value;
} GArrowTableDatumPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowTableDatum,
                           garrow_table_datum,
                           GARROW_TYPE_DATUM)

#define GARROW_TABLE_DATUM_GET_PRIVATE(obj)         \
  static_cast<GArrowTableDatumPrivate *>(           \
    garrow_table_datum_get_instance_private(        \
      GARROW_TABLE_DATUM(obj)))

static void
garrow_table_datum_dispose(GObject *object)
{
  auto priv = GARROW_TABLE_DATUM_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_table_datum_parent_class)->dispose(object);
}

static void
garrow_table_datum_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_TABLE_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_TABLE(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_table_datum_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_TABLE_DATUM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    g_value_set_object(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_table_datum_init(GArrowTableDatum *object)
{
}

static void
garrow_table_datum_class_init(GArrowTableDatumClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_table_datum_dispose;
  gobject_class->set_property = garrow_table_datum_set_property;
  gobject_class->get_property = garrow_table_datum_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("value",
                             "Value",
                             "The table held by this datum",
                             GARROW_TYPE_TABLE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

/**
 * garrow_table_datum_new:
 * @value: A #GArrowTable.
 *
 * Returns: A newly created #GArrowTableDatum.
 *
 * Since: 1.0.0
 */
GArrowTableDatum *
garrow_table_datum_new(GArrowTable *value)
{
  auto arrow_value = garrow_table_get_raw(value);
  auto arrow_datum = std::make_shared<arrow::Datum>(arrow_value);
  return garrow_table_datum_new_raw(&arrow_datum, value);
}


G_END_DECLS

std::shared_ptr<arrow::Datum>
garrow_datum_get_raw(GArrowDatum *datum)
{
  auto priv = GARROW_DATUM_GET_PRIVATE(datum);
  return priv->datum;
}

GArrowArrayDatum *
garrow_array_datum_new_raw(std::shared_ptr<arrow::Datum> *arrow_datum,
                           GArrowArray *value)
{
  return GARROW_ARRAY_DATUM(g_object_new(GARROW_TYPE_ARRAY_DATUM,
                                         "datum", arrow_datum,
                                         "value", value,
                                         NULL));
}

GArrowChunkedArrayDatum *
garrow_chunked_array_datum_new_raw(std::shared_ptr<arrow::Datum> *arrow_datum,
                                   GArrowChunkedArray *value)
{
  return GARROW_CHUNKED_ARRAY_DATUM(g_object_new(GARROW_TYPE_CHUNKED_ARRAY_DATUM,
                                                 "datum", arrow_datum,
                                                 "value", value,
                                                 NULL));
}

GArrowRecordBatchDatum *
garrow_record_batch_datum_new_raw(std::shared_ptr<arrow::Datum> *arrow_datum,
                                  GArrowRecordBatch *value)
{
  return GARROW_RECORD_BATCH_DATUM(g_object_new(GARROW_TYPE_RECORD_BATCH_DATUM,
                                                "datum", arrow_datum,
                                                "value", value,
                                                NULL));
}

GArrowTableDatum *
garrow_table_datum_new_raw(std::shared_ptr<arrow::Datum> *arrow_datum,
                           GArrowTable *value)
{
  return GARROW_TABLE_DATUM(g_object_new(GARROW_TYPE_TABLE_DATUM,
                                         "datum", arrow_datum,
                                         "value", value,
                                         NULL));
}
