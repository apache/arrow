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

#include <arrow-glib/array.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-dataset-glib/enums.h>
#include <arrow-dataset-glib/partitioning.hpp>

G_BEGIN_DECLS

/**
 * SECTION: partitioning
 * @section_id: partitioning
 * @title: Partitioning classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADatasetPartitioningOptions is a class for partitioning options.
 *
 * #GADatasetPartitioning is a base class for partitioning classes
 * such as #GADatasetDirectoryPartitioning.
 *
 * #GADatasetKeyValuePartitioning is a base class for key-value style
 * partitioning classes such as #GADatasetDirectoryPartitioning.
 *
 * #GADatasetDirectoryPartitioning is a class for partitioning that
 * uses directory structure.
 *
 * Since: 6.0.0
 */

typedef struct GADatasetPartitioningOptionsPrivate_ {
  gboolean infer_dictionary;
  GArrowSchema *schema;
  GADatasetSegmentEncoding segment_encoding;
} GADatasetPartitioningOptionsPrivate;

enum {
  PROP_INFER_DICTIONARY = 1,
  PROP_SCHEMA,
  PROP_SEGMENT_ENCODING,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetPartitioningOptions,
                           gadataset_partitioning_options,
                           G_TYPE_OBJECT)

#define GADATASET_PARTITIONING_OPTIONS_GET_PRIVATE(obj)         \
  static_cast<GADatasetPartitioningOptionsPrivate *>(           \
    gadataset_partitioning_options_get_instance_private(        \
      GADATASET_PARTITIONING_OPTIONS(obj)))

static void
gadataset_partitioning_options_dispose(GObject *object)
{
  auto priv = GADATASET_PARTITIONING_OPTIONS_GET_PRIVATE(object);

  if (priv->schema) {
    g_object_unref(priv->schema);
    priv->schema = nullptr;
  }

  G_OBJECT_CLASS(gadataset_partitioning_options_parent_class)->dispose(object);
}

static void
gadataset_partitioning_options_set_property(GObject *object,
                                            guint prop_id,
                                            const GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GADATASET_PARTITIONING_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INFER_DICTIONARY:
    priv->infer_dictionary = g_value_get_boolean(value);
    break;
  case PROP_SCHEMA:
    {
      auto schema = g_value_get_object(value);
      if (priv->schema == schema) {
        break;
      }
      auto old_schema = priv->schema;
      if (schema) {
        g_object_ref(schema);
        priv->schema = GARROW_SCHEMA(schema);
      } else {
        priv->schema = NULL;
      }
      if (old_schema) {
        g_object_unref(old_schema);
      }
    }
    break;
  case PROP_SEGMENT_ENCODING:
    priv->segment_encoding =
      static_cast<GADatasetSegmentEncoding>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_partitioning_options_get_property(GObject *object,
                                            guint prop_id,
                                            GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GADATASET_PARTITIONING_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INFER_DICTIONARY:
    g_value_set_boolean(value, priv->infer_dictionary);
    break;
  case PROP_SCHEMA:
    g_value_set_object(value, priv->schema);
    break;
  case PROP_SEGMENT_ENCODING:
    g_value_set_enum(value, priv->segment_encoding);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_partitioning_options_init(GADatasetPartitioningOptions *object)
{
}

static void
gadataset_partitioning_options_class_init(
  GADatasetPartitioningOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gadataset_partitioning_options_dispose;
  gobject_class->set_property = gadataset_partitioning_options_set_property;
  gobject_class->get_property = gadataset_partitioning_options_get_property;

  arrow::dataset::PartitioningFactoryOptions default_options;
  GParamSpec *spec;
  /**
   * GADatasetPartitioningOptions:infer-dictionary:
   *
   * When inferring a schema for partition fields, yield dictionary
   * encoded types instead of plain. This can be more efficient when
   * materializing virtual columns, and Expressions parsed by the
   * finished Partitioning will include dictionaries of all unique
   * inspected values for each field.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_boolean("infer-dictionary",
                              "Infer dictionary",
                              "Whether encode partitioned field values as "
                              "dictionary",
                              default_options.infer_dictionary,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_INFER_DICTIONARY, spec);

  /**
   * GADatasetPartitioningOptions:schema:
   *
   * Optionally, an expected schema can be provided, in which case
   * inference will only check discovered fields against the schema
   * and update internal state (such as dictionaries).
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("schema",
                             "Schema",
                             "Inference will only check discovered fields "
                             "against the schema and update internal state",
                             GARROW_TYPE_SCHEMA,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_SCHEMA, spec);

  /**
   * GADatasetPartitioningOptions:segment-encoding:
   *
   * After splitting a path into components, decode the path
   * components before parsing according to this scheme.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_enum("segment-encoding",
                           "Segment encoding",
                           "After splitting a path into components, "
                           "decode the path components before "
                           "parsing according to this scheme",
                           GADATASET_TYPE_SEGMENT_ENCODING,
                           static_cast<GADatasetSegmentEncoding>(
                             default_options.segment_encoding),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_SEGMENT_ENCODING, spec);
}

/**
 * gadataset_partitioning_options_new:
 *
 * Returns: The newly created #GADatasetPartitioningOptions.
 *
 * Since: 6.0.0
 */
GADatasetPartitioningOptions *
gadataset_partitioning_options_new(void)
{
  return GADATASET_PARTITIONING_OPTIONS(
    g_object_new(GADATASET_TYPE_PARTITIONING_OPTIONS,
                 NULL));
}


typedef struct GADatasetPartitioningPrivate_ {
  std::shared_ptr<arrow::dataset::Partitioning> partitioning;
} GADatasetPartitioningPrivate;

enum {
  PROP_PARTITIONING = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetPartitioning,
                           gadataset_partitioning,
                           G_TYPE_OBJECT)

#define GADATASET_PARTITIONING_GET_PRIVATE(obj)         \
  static_cast<GADatasetPartitioningPrivate *>(          \
    gadataset_partitioning_get_instance_private(        \
      GADATASET_PARTITIONING(obj)))

static void
gadataset_partitioning_finalize(GObject *object)
{
  auto priv = GADATASET_PARTITIONING_GET_PRIVATE(object);
  priv->partitioning.~shared_ptr();
  G_OBJECT_CLASS(gadataset_partitioning_parent_class)->finalize(object);
}

static void
gadataset_partitioning_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GADATASET_PARTITIONING_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_PARTITIONING:
    priv->partitioning =
      *static_cast<std::shared_ptr<arrow::dataset::Partitioning> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_partitioning_init(GADatasetPartitioning *object)
{
  auto priv = GADATASET_PARTITIONING_GET_PRIVATE(object);
  new(&priv->partitioning) std::shared_ptr<arrow::dataset::Partitioning>;
}

static void
gadataset_partitioning_class_init(GADatasetPartitioningClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gadataset_partitioning_finalize;
  gobject_class->set_property = gadataset_partitioning_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("partitioning",
                              "Partitioning",
                              "The raw "
                              "std::shared<arrow::dataset::Partitioning> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PARTITIONING, spec);
}

/**
 * gadataset_partitioning_new:
 *
 * Returns: The newly created #GADatasetPartitioning that doesn't
 *   partition.
 *
 * Since: 6.0.0
 */
GADatasetPartitioning *
gadataset_partitioning_new(void)
{
  auto arrow_partitioning = arrow::dataset::Partitioning::Default();
  return GADATASET_PARTITIONING(
    g_object_new(GADATASET_TYPE_PARTITIONING,
                 "partitioning", &arrow_partitioning,
                 NULL));
}

/**
 * gadataset_partitioning_get_type_name:
 * @partitioning: A #GADatasetPartitioning.
 *
 * Returns: The type name of @partitioning.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gchar *
gadataset_partitioning_get_type_name(GADatasetPartitioning *partitioning)
{
  auto arrow_partitioning = gadataset_partitioning_get_raw(partitioning);
  auto arrow_type_name = arrow_partitioning->type_name();
  return g_strndup(arrow_type_name.c_str(),
                   arrow_type_name.size());
}


G_DEFINE_TYPE(GADatasetKeyValuePartitioning,
              gadataset_key_value_partitioning,
              GADATASET_TYPE_PARTITIONING)

static void
gadataset_key_value_partitioning_init(GADatasetKeyValuePartitioning *object)
{
}

static void
gadataset_key_value_partitioning_class_init(
  GADatasetKeyValuePartitioningClass *klass)
{
}


G_DEFINE_TYPE(GADatasetDirectoryPartitioning,
              gadataset_directory_partitioning,
              GADATASET_TYPE_KEY_VALUE_PARTITIONING)

static void
gadataset_directory_partitioning_init(GADatasetDirectoryPartitioning *object)
{
}

static void
gadataset_directory_partitioning_class_init(
  GADatasetDirectoryPartitioningClass *klass)
{
}

/**
 * gadataset_directory_partitioning_new:
 * @schema: A #GArrowSchema that describes all partitioned segments.
 * @dictionaries: (nullable) (element-type GArrowArray): A list of #GArrowArray
 *   for dictionary data types in @schema.
 * @options: (nullable): A #GADatasetPartitioningOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The newly created #GADatasetDirectoryPartitioning on success,
 *   %NULL on error.
 *
 * Since: 6.0.0
 */
GADatasetDirectoryPartitioning *
gadataset_directory_partitioning_new(GArrowSchema *schema,
                                     GList *dictionaries,
                                     GADatasetPartitioningOptions *options,
                                     GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<arrow::Array>> arrow_dictionaries;
  for (auto node = dictionaries; node; node = node->next) {
    auto dictionary = GARROW_ARRAY(node->data);
    if (dictionary) {
      arrow_dictionaries.push_back(garrow_array_get_raw(dictionary));
    } else {
      arrow_dictionaries.push_back(nullptr);
    }
  }
  arrow::dataset::KeyValuePartitioningOptions arrow_options;
  if (options) {
    arrow_options =
      gadataset_partitioning_options_get_raw_key_value_partitioning_options(
        options);
  }
  auto arrow_partitioning =
    std::make_shared<arrow::dataset::DirectoryPartitioning>(
      arrow_schema,
      arrow_dictionaries,
      arrow_options);
  return GADATASET_DIRECTORY_PARTITIONING(
    g_object_new(GADATASET_TYPE_DIRECTORY_PARTITIONING,
                 "partitioning", &arrow_partitioning,
                 NULL));
}


G_END_DECLS

arrow::dataset::KeyValuePartitioningOptions
gadataset_partitioning_options_get_raw_key_value_partitioning_options(
  GADatasetPartitioningOptions *options)
{
  auto priv = GADATASET_PARTITIONING_OPTIONS_GET_PRIVATE(options);
  arrow::dataset::KeyValuePartitioningOptions arrow_options;
  arrow_options.segment_encoding =
    static_cast<arrow::dataset::SegmentEncoding>(priv->segment_encoding);
  return arrow_options;
}

std::shared_ptr<arrow::dataset::Partitioning>
gadataset_partitioning_get_raw(GADatasetPartitioning *partitioning)
{
  auto priv = GADATASET_PARTITIONING_GET_PRIVATE(partitioning);
  return priv->partitioning;
}
