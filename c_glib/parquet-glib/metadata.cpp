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

#include <arrow-glib/arrow-glib.hpp>

#include <parquet-glib/metadata.hpp>

G_BEGIN_DECLS

/**
 * SECTION: metadata
 * @title: Metadata related classes
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetFileMetadata is a class for file-level metadata.
 */

typedef struct GParquetFileMetadataPrivate_ {
  std::shared_ptr<parquet::FileMetaData> metadata;
} GParquetFileMetadataPrivate;

enum {
  PROP_METADATA = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetFileMetadata,
                           gparquet_file_metadata,
                           G_TYPE_OBJECT)

#define GPARQUET_FILE_METADATA_GET_PRIVATE(object)      \
  static_cast<GParquetFileMetadataPrivate *>(           \
    gparquet_file_metadata_get_instance_private(        \
      GPARQUET_FILE_METADATA(object)))

static void
gparquet_file_metadata_finalize(GObject *object)
{
  auto priv = GPARQUET_FILE_METADATA_GET_PRIVATE(object);
  priv->metadata.~shared_ptr();
  G_OBJECT_CLASS(gparquet_file_metadata_parent_class)->finalize(object);
}

static void
gparquet_file_metadata_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GPARQUET_FILE_METADATA_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_METADATA:
    priv->metadata =
      *static_cast<std::shared_ptr<parquet::FileMetaData> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_file_metadata_init(GParquetFileMetadata *object)
{
  auto priv = GPARQUET_FILE_METADATA_GET_PRIVATE(object);
  new(&priv->metadata) std::shared_ptr<parquet::FileMetaData>;
}

static void
gparquet_file_metadata_class_init(GParquetFileMetadataClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gparquet_file_metadata_finalize;
  gobject_class->set_property = gparquet_file_metadata_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("metadata",
                              "Metadata",
                              "The raw std::shared_ptr<parquet::FileMetaData>",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_METADATA, spec);
}

/**
 * gparquet_file_metadata_equal:
 * @metadata: A #GParquetFileMetadata.
 * @other_metadata: A #GParquetFileMetadata.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_file_metadata_equal(GParquetFileMetadata *metadata,
                             GParquetFileMetadata *other_metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  auto parquet_other_metadata = gparquet_file_metadata_get_raw(other_metadata);
  return parquet_metadata->Equals(*parquet_other_metadata);
}

/**
 * gparquet_file_metadata_get_n_columns:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: The number of top-level columns in the schema.
 *
 *   Parquet thrift definition requires that nested schema elements are
 *   flattened. This method returns the number of columns in the un-flattened
 *   version.
 *
 * Since: 8.0.0
 */
gint
gparquet_file_metadata_get_n_columns(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->num_columns();
}

/**
 * gparquet_file_metadata_get_n_schema_elements:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: The number of flattened schema elements.
 *
 *   Parquet thrift definition requires that nested schema elements are
 *   flattened. This method returns the total number of elements in the
 *   flattened list.
 *
 * Since: 8.0.0
 */
gint
gparquet_file_metadata_get_n_schema_elements(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->num_schema_elements();
}

/**
 * gparquet_file_metadata_get_n_rows:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: The total number of rows.
 *
 * Since: 8.0.0
 */
gint64
gparquet_file_metadata_get_n_rows(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->num_rows();
}

/**
 * gparquet_file_metadata_get_n_row_groups:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: The number of row groups in the file.
 *
 * Since: 8.0.0
 */
gint
gparquet_file_metadata_get_n_row_groups(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->num_row_groups();
}

/**
 * gparquet_file_metadata_get_created_by:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: The application's user-agent string of the writer.
 *
 * Since: 8.0.0
 */
const gchar *
gparquet_file_metadata_get_created_by(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->created_by().c_str();
}

/**
 * gparquet_file_metadata_get_size:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: The size of the original thrift encoded metadata footer.
 *
 * Since: 8.0.0
 */
guint32
gparquet_file_metadata_get_size(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->size();
}

/**
 * gparquet_file_metadata_can_decompress:
 * @metadata: A #GParquetFileMetadata.
 *
 * Returns: %TRUE if all of the row groups can be decompressed, %FALSE
 *   otherwise.
 *
 *   This will return false if any of the RowGroup's page is
 *   compressed with a compression format which is not compiled in the
 *   current Parquet library.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_file_metadata_can_decompress(GParquetFileMetadata *metadata)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  return parquet_metadata->can_decompress();
}

G_END_DECLS

GParquetFileMetadata *
gparquet_file_metadata_new_raw(
  std::shared_ptr<parquet::FileMetaData> *parquet_metadata)
{
  auto metadata =
    GPARQUET_FILE_METADATA(g_object_new(GPARQUET_TYPE_FILE_METADATA,
                                        "metadata", parquet_metadata,
                                        NULL));
  return metadata;
}

std::shared_ptr<parquet::FileMetaData>
gparquet_file_metadata_get_raw(GParquetFileMetadata *metadata)
{
  auto priv = GPARQUET_FILE_METADATA_GET_PRIVATE(metadata);
  return priv->metadata;
}
