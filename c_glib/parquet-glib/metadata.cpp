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
#include <parquet-glib/statistics.hpp>

G_BEGIN_DECLS

/**
 * SECTION: metadata
 * @title: Metadata related classes
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetColumnChunkMetadata is a class for column chunk level metadata.
 *
 * #GParquetRowGroupMetadata is a class for row group level metadata.
 *
 * #GParquetFileMetadata is a class for file level metadata.
 */

struct GParquetColumnChunkMetadataPrivate {
  parquet::ColumnChunkMetaData *metadata;
  GParquetRowGroupMetadata *owner;
};

enum {
  PROP_METADATA = 1,
  PROP_OWNER,
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetColumnChunkMetadata,
                           gparquet_column_chunk_metadata,
                           G_TYPE_OBJECT)

#define GPARQUET_COLUMN_CHUNK_METADATA_GET_PRIVATE(object)      \
  static_cast<GParquetColumnChunkMetadataPrivate *>(            \
    gparquet_column_chunk_metadata_get_instance_private(        \
      GPARQUET_COLUMN_CHUNK_METADATA(object)))

static void
gparquet_column_chunk_metadata_dispose(GObject *object)
{
  auto priv = GPARQUET_COLUMN_CHUNK_METADATA_GET_PRIVATE(object);
  if (priv->owner) {
    g_object_unref(priv->owner);
    priv->owner = nullptr;
  }
  G_OBJECT_CLASS(gparquet_column_chunk_metadata_parent_class)->dispose(object);
}

static void
gparquet_column_chunk_metadata_set_property(GObject *object,
                                            guint prop_id,
                                            const GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GPARQUET_COLUMN_CHUNK_METADATA_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_METADATA:
    priv->metadata =
      static_cast<parquet::ColumnChunkMetaData *>(g_value_get_pointer(value));
    break;
  case PROP_OWNER:
    priv->owner = GPARQUET_ROW_GROUP_METADATA(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_column_chunk_metadata_init(GParquetColumnChunkMetadata *object)
{
}

static void
gparquet_column_chunk_metadata_class_init(
  GParquetColumnChunkMetadataClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = gparquet_column_chunk_metadata_dispose;
  gobject_class->set_property = gparquet_column_chunk_metadata_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("metadata",
                              "Metadata",
                              "The raw parquet::ColumnChunkMetaData *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_METADATA, spec);

  spec = g_param_spec_object("owner",
                             "Owner",
                             "The row group metadata that owns this metadata",
                             GPARQUET_TYPE_ROW_GROUP_METADATA,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OWNER, spec);
}

/**
 * gparquet_column_chunk_metadata_equal:
 * @metadata: A #GParquetColumnChunkMetadata.
 * @other_metadata: A #GParquetColumnChunkMetadata.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_column_chunk_metadata_equal(GParquetColumnChunkMetadata *metadata,
                                     GParquetColumnChunkMetadata *other_metadata)
{
  auto parquet_metadata = gparquet_column_chunk_metadata_get_raw(metadata);
  auto parquet_other_metadata =
    gparquet_column_chunk_metadata_get_raw(other_metadata);
  return parquet_metadata->Equals(*parquet_other_metadata);
}

/**
 * gparquet_column_chunk_metadata_get_total_size:
 * @metadata: A #GParquetColumnChunkMetadata.
 *
 * Returns: Total byte size of all the uncompressed data in this
 *   column chunk.
 *
 * Since: 8.0.0
 */
gint64
gparquet_column_chunk_metadata_get_total_size(GParquetColumnChunkMetadata *metadata)
{
  auto parquet_metadata = gparquet_column_chunk_metadata_get_raw(metadata);
  return parquet_metadata->total_uncompressed_size();
}

/**
 * gparquet_column_chunk_metadata_get_total_compressed_size:
 * @metadata: A #GParquetColumnChunkMetadata.
 *
 * Returns: Total byte size of all the compressed (and potentially
 *   encrypted) data in this column chunk.
 *
 * Since: 8.0.0
 */
gint64
gparquet_column_chunk_metadata_get_total_compressed_size(
  GParquetColumnChunkMetadata *metadata)
{
  auto parquet_metadata = gparquet_column_chunk_metadata_get_raw(metadata);
  return parquet_metadata->total_compressed_size();
}

/**
 * gparquet_column_chunk_metadata_get_file_offset:
 * @metadata: A #GParquetColumnChunkMetadata.
 *
 * Returns: Byte offset from beginning of file to first page (data or
 *   dictionary) in this column chunk.
 *
 * Since: 8.0.0
 */
gint64
gparquet_column_chunk_metadata_get_file_offset(
  GParquetColumnChunkMetadata *metadata)
{
  auto parquet_metadata = gparquet_column_chunk_metadata_get_raw(metadata);
  return parquet_metadata->file_offset();
}

/**
 * gparquet_column_chunk_metadata_can_decompress:
 * @metadata: A #GParquetColumnChunkMetadata.
 *
 * Returns: %TRUE if all of the column chunk can be decompressed,
 *   %FALSE otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_column_chunk_metadata_can_decompress(
  GParquetColumnChunkMetadata *metadata)
{
  auto parquet_metadata = gparquet_column_chunk_metadata_get_raw(metadata);
  return parquet_metadata->can_decompress();
}

/**
 * gparquet_column_chunk_metadata_get_statistics:
 * @metadata: A #GParquetColumnChunkMetadata.
 *
 * Returns: (transfer full) (nullable): The statistics of this column chunk if
 *   it's set, %NULL otherwise.
 *
 * Since: 8.0.0
 */
GParquetStatistics *
gparquet_column_chunk_metadata_get_statistics(
  GParquetColumnChunkMetadata *metadata)
{
  auto parquet_metadata = gparquet_column_chunk_metadata_get_raw(metadata);
  auto parquet_statistics = parquet_metadata->statistics();
  if (parquet_statistics) {
    return gparquet_statistics_new_raw(&parquet_statistics);
  } else {
    return NULL;
  }
}


struct GParquetRowGroupMetadataPrivate {
  parquet::RowGroupMetaData *metadata;
  GParquetFileMetadata *owner;
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetRowGroupMetadata,
                           gparquet_row_group_metadata,
                           G_TYPE_OBJECT)

#define GPARQUET_ROW_GROUP_METADATA_GET_PRIVATE(object)      \
  static_cast<GParquetRowGroupMetadataPrivate *>(            \
    gparquet_row_group_metadata_get_instance_private(        \
      GPARQUET_ROW_GROUP_METADATA(object)))

static void
gparquet_row_group_metadata_dispose(GObject *object)
{
  auto priv = GPARQUET_ROW_GROUP_METADATA_GET_PRIVATE(object);
  if (priv->owner) {
    g_object_unref(priv->owner);
    priv->owner = nullptr;
  }
  G_OBJECT_CLASS(gparquet_row_group_metadata_parent_class)->dispose(object);
}

static void
gparquet_row_group_metadata_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GPARQUET_ROW_GROUP_METADATA_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_METADATA:
    priv->metadata =
      static_cast<parquet::RowGroupMetaData *>(g_value_get_pointer(value));
    break;
  case PROP_OWNER:
    priv->owner = GPARQUET_FILE_METADATA(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_row_group_metadata_init(GParquetRowGroupMetadata *object)
{
}

static void
gparquet_row_group_metadata_class_init(GParquetRowGroupMetadataClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gparquet_row_group_metadata_dispose;
  gobject_class->set_property = gparquet_row_group_metadata_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("metadata", "Metadata",
                              "The raw parquet::RowGroupMetaData *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_METADATA, spec);

  spec = g_param_spec_object("owner",
                             "Owner",
                             "The file group metadata that owns this metadata",
                             GPARQUET_TYPE_FILE_METADATA,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OWNER, spec);
}

/**
 * gparquet_row_group_metadata_equal:
 * @metadata: A #GParquetRowGroupMetadata.
 * @other_metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_row_group_metadata_equal(GParquetRowGroupMetadata *metadata,
                                  GParquetRowGroupMetadata *other_metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  auto parquet_other_metadata =
    gparquet_row_group_metadata_get_raw(other_metadata);
  return parquet_metadata->Equals(*parquet_other_metadata);
}

/**
 * gparquet_row_group_metadata_get_n_columns:
 * @metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: The number of columns in this row group. The order must
 *   match the parent's column ordering.
 *
 * Since: 8.0.0
 */
gint
gparquet_row_group_metadata_get_n_columns(GParquetRowGroupMetadata *metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  return parquet_metadata->num_columns();
}

/**
 * gparquet_row_group_metadata_get_column_chunk:
 * @metadata: A #GParquetRowGroupMetadata.
 * @index: An index of the column chunk to retrieve.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): A #GParquetColumnChunkMetadata
 *   at @index on success, %NULL on error.
 *
 * Since: 8.0.0
 */
GParquetColumnChunkMetadata *
gparquet_row_group_metadata_get_column_chunk(GParquetRowGroupMetadata *metadata,
                                             gint index,
                                             GError **error)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  std::unique_ptr<parquet::ColumnChunkMetaData> parquet_column_chunk_metadata;
  auto status = ([&] {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    parquet_column_chunk_metadata = parquet_metadata->ColumnChunk(index);
    return arrow::Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  })();
  if (garrow::check(error,
                    status,
                    "[parquet][row-group-metadata][get-column-chunk]")) {
    return gparquet_column_chunk_metadata_new_raw(
      parquet_column_chunk_metadata.release(),
      metadata);
  } else {
    return NULL;
  }
}

/**
 * gparquet_row_group_metadata_get_n_rows:
 * @metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: The number of rows in this row group.
 *
 * Since: 8.0.0
 */
gint64
gparquet_row_group_metadata_get_n_rows(GParquetRowGroupMetadata *metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  return parquet_metadata->num_rows();
}

/**
 * gparquet_row_group_metadata_get_total_size:
 * @metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: Total byte size of all the uncompressed column data in
 *   this row group.
 *
 * Since: 8.0.0
 */
gint64
gparquet_row_group_metadata_get_total_size(GParquetRowGroupMetadata *metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  return parquet_metadata->total_byte_size();
}

/**
 * gparquet_row_group_metadata_get_total_compressed_size:
 * @metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: Total byte size of all the compressed (and potentially
 *   encrypted) column data in this row group.
 *
 * Since: 8.0.0
 */
gint64
gparquet_row_group_metadata_get_total_compressed_size(
  GParquetRowGroupMetadata *metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  return parquet_metadata->total_compressed_size();
}

/**
 * gparquet_row_group_metadata_get_file_offset:
 * @metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: Byte offset from beginning of file to first page (data or
 *   dictionary) in this row group.
 *
 *   The `file_offset` field that this method exposes is
 *   optional. This method will return 0 if that field is not set to a
 *   meaningful value.
 *
 * Since: 8.0.0
 */
gint64
gparquet_row_group_metadata_get_file_offset(GParquetRowGroupMetadata *metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  return parquet_metadata->file_offset();
}

/**
 * gparquet_row_group_metadata_can_decompress:
 * @metadata: A #GParquetRowGroupMetadata.
 *
 * Returns: %TRUE if all of the row group's column chunks can be
 *   decompressed, %FALSE otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_row_group_metadata_can_decompress(GParquetRowGroupMetadata *metadata)
{
  auto parquet_metadata = gparquet_row_group_metadata_get_raw(metadata);
  return parquet_metadata->can_decompress();
}


struct GParquetFileMetadataPrivate {
  std::shared_ptr<parquet::FileMetaData> metadata;
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
 * gparquet_file_metadata_get_row_group:
 * @metadata: A #GParquetFileMetadata.
 * @index: An index of the row group to retrieve.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): A #GParquetRowGroupMetadata
 *   at @index on success, %NULL on error.
 *
 * Since: 8.0.0
 */
GParquetRowGroupMetadata *
gparquet_file_metadata_get_row_group(GParquetFileMetadata *metadata,
                                     gint index,
                                     GError **error)
{
  auto parquet_metadata = gparquet_file_metadata_get_raw(metadata);
  std::unique_ptr<parquet::RowGroupMetaData> parquet_row_group_metadata;
  auto status = ([&] {
    BEGIN_PARQUET_CATCH_EXCEPTIONS
    parquet_row_group_metadata = parquet_metadata->RowGroup(index);
    return arrow::Status::OK();
    END_PARQUET_CATCH_EXCEPTIONS
  })();
  if (garrow::check(error, status, "[parquet][file-metadata][get-row-group]")) {
    return gparquet_row_group_metadata_new_raw(parquet_row_group_metadata.release(),
                                               metadata);
  } else {
    return NULL;
  }
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


GParquetColumnChunkMetadata *
gparquet_column_chunk_metadata_new_raw(
  parquet::ColumnChunkMetaData *parquet_metadata,
  GParquetRowGroupMetadata *owner)
{
  auto metadata =
    GPARQUET_COLUMN_CHUNK_METADATA(
      g_object_new(GPARQUET_TYPE_COLUMN_CHUNK_METADATA,
                   "metadata", parquet_metadata,
                   "owner", owner,
                   NULL));
  return metadata;
}

parquet::ColumnChunkMetaData *
gparquet_column_chunk_metadata_get_raw(GParquetColumnChunkMetadata *metadata)
{
  auto priv = GPARQUET_COLUMN_CHUNK_METADATA_GET_PRIVATE(metadata);
  return priv->metadata;
}


GParquetRowGroupMetadata *
gparquet_row_group_metadata_new_raw(parquet::RowGroupMetaData *parquet_metadata,
                                    GParquetFileMetadata *owner)
{
  auto metadata =
    GPARQUET_ROW_GROUP_METADATA(g_object_new(GPARQUET_TYPE_ROW_GROUP_METADATA,
                                             "metadata", parquet_metadata,
                                             "owner", owner,
                                             NULL));
  return metadata;
}

parquet::RowGroupMetaData *
gparquet_row_group_metadata_get_raw(GParquetRowGroupMetadata *metadata)
{
  auto priv = GPARQUET_ROW_GROUP_METADATA_GET_PRIVATE(metadata);
  return priv->metadata;
}


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
