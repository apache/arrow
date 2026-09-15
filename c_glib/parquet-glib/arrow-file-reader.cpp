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
#include <arrow-glib/internal-index.hpp>

#include <parquet-glib/arrow-file-reader.hpp>
#include <parquet-glib/metadata.hpp>

#include <parquet/file_reader.h>

namespace {
  GParquetArrowFileReader *
  open_reader_with_properties(std::shared_ptr<arrow::io::RandomAccessFile> source,
                              GParquetReaderProperties *properties,
                              GError **error,
                              const char *tag)
  {
    auto parquet_properties = properties ? gparquet_reader_properties_get_raw(properties)
                                         : parquet::default_reader_properties();
    parquet::arrow::FileReaderBuilder builder;
    if (!garrow::check(error, builder.Open(source, parquet_properties), tag)) {
      return NULL;
    }
    if (parquet_properties.is_buffered_stream_enabled()) {
      // Read-ahead would bypass the buffered stream by caching whole column chunks.
      auto arrow_properties = parquet::default_arrow_reader_properties();
      arrow_properties.set_pre_buffer(false);
      builder.properties(arrow_properties);
    }
    auto result = builder.Build();
    if (!garrow::check(error, result, tag)) {
      return NULL;
    }
    return gparquet_arrow_file_reader_new_raw(result->release());
  }
} // namespace

G_BEGIN_DECLS

/**
 * SECTION: arrow-file-reader
 * @short_description: Arrow file reader class
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetReaderProperties is a class for configuring Parquet reads.
 *
 * #GParquetArrowFileReader is a class for reading Apache Parquet data
 * from file and returns them as Apache Arrow data.
 */

typedef struct GParquetReaderPropertiesPrivate_
{
  parquet::ReaderProperties properties;
} GParquetReaderPropertiesPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GParquetReaderProperties,
                           gparquet_reader_properties,
                           G_TYPE_OBJECT)

#define GPARQUET_READER_PROPERTIES_GET_PRIVATE(object)                                   \
  static_cast<GParquetReaderPropertiesPrivate *>(                                        \
    gparquet_reader_properties_get_instance_private(GPARQUET_READER_PROPERTIES(object)))

static void
gparquet_reader_properties_finalize(GObject *object)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(object);
  priv->properties.~ReaderProperties();
  G_OBJECT_CLASS(gparquet_reader_properties_parent_class)->finalize(object);
}

static void
gparquet_reader_properties_init(GParquetReaderProperties *object)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(object);
  new (&priv->properties) parquet::ReaderProperties(parquet::default_reader_properties());
}

static void
gparquet_reader_properties_class_init(GParquetReaderPropertiesClass *klass)
{
  G_OBJECT_CLASS(klass)->finalize = gparquet_reader_properties_finalize;
}

/**
 * gparquet_reader_properties_new:
 *
 * Returns: A newly created #GParquetReaderProperties.
 *
 * Since: 26.0.0
 */
GParquetReaderProperties *
gparquet_reader_properties_new(void)
{
  return GPARQUET_READER_PROPERTIES(g_object_new(GPARQUET_TYPE_READER_PROPERTIES, NULL));
}

/**
 * gparquet_reader_properties_enable_buffered_stream:
 * @properties: A #GParquetReaderProperties.
 *
 * Enable buffered stream reading. Readers constructed with these properties
 * disable read-ahead of whole column chunks to use buffered streams instead.
 * This does not impose a limit on the memory used by decoded data.
 *
 * Since: 26.0.0
 */
void
gparquet_reader_properties_enable_buffered_stream(GParquetReaderProperties *properties)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(properties);
  priv->properties.enable_buffered_stream();
}

/**
 * gparquet_reader_properties_disable_buffered_stream:
 * @properties: A #GParquetReaderProperties.
 *
 * Disable buffered stream reading. This is the default.
 *
 * Since: 26.0.0
 */
void
gparquet_reader_properties_disable_buffered_stream(GParquetReaderProperties *properties)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(properties);
  priv->properties.disable_buffered_stream();
}

/**
 * gparquet_reader_properties_is_buffered_stream_enabled:
 * @properties: A #GParquetReaderProperties.
 *
 * Returns: %TRUE if buffered stream reading is enabled, %FALSE otherwise.
 *
 * Since: 26.0.0
 */
gboolean
gparquet_reader_properties_is_buffered_stream_enabled(
  GParquetReaderProperties *properties)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(properties);
  return priv->properties.is_buffered_stream_enabled();
}

/**
 * gparquet_reader_properties_set_buffer_size:
 * @properties: A #GParquetReaderProperties.
 * @buffer_size: The buffer size in bytes. This must be positive when buffering is
 * enabled.
 *
 * Set the buffered stream size. This does not enable buffered stream reading.
 * Reads required for a data page may exceed this size.
 *
 * Since: 26.0.0
 */
void
gparquet_reader_properties_set_buffer_size(GParquetReaderProperties *properties,
                                           gint64 buffer_size)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(properties);
  priv->properties.set_buffer_size(buffer_size);
}

/**
 * gparquet_reader_properties_get_buffer_size:
 * @properties: A #GParquetReaderProperties.
 *
 * Returns: The buffered stream size in bytes.
 *
 * Since: 26.0.0
 */
gint64
gparquet_reader_properties_get_buffer_size(GParquetReaderProperties *properties)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(properties);
  return priv->properties.buffer_size();
}

typedef struct GParquetArrowFileReaderPrivate_
{
  parquet::arrow::FileReader *arrow_file_reader;
  GArrowSeekableInputStream *source;
} GParquetArrowFileReaderPrivate;

enum {
  PROP_0,
  PROP_ARROW_FILE_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetArrowFileReader,
                           gparquet_arrow_file_reader,
                           G_TYPE_OBJECT)

#define GPARQUET_ARROW_FILE_READER_GET_PRIVATE(obj)                                      \
  static_cast<GParquetArrowFileReaderPrivate *>(                                         \
    gparquet_arrow_file_reader_get_instance_private(GPARQUET_ARROW_FILE_READER(obj)))

static void
gparquet_arrow_file_reader_dispose(GObject *object)
{
  auto priv = GPARQUET_ARROW_FILE_READER_GET_PRIVATE(object);
  g_clear_object(&priv->source);
  G_OBJECT_CLASS(gparquet_arrow_file_reader_parent_class)->dispose(object);
}

static void
gparquet_arrow_file_reader_finalize(GObject *object)
{
  auto priv = GPARQUET_ARROW_FILE_READER_GET_PRIVATE(object);

  delete priv->arrow_file_reader;

  G_OBJECT_CLASS(gparquet_arrow_file_reader_parent_class)->finalize(object);
}

static void
gparquet_arrow_file_reader_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GPARQUET_ARROW_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARROW_FILE_READER:
    priv->arrow_file_reader =
      static_cast<parquet::arrow::FileReader *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_arrow_file_reader_get_property(GObject *object,
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
gparquet_arrow_file_reader_init(GParquetArrowFileReader *object)
{
}

static void
gparquet_arrow_file_reader_class_init(GParquetArrowFileReaderClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gparquet_arrow_file_reader_dispose;
  gobject_class->finalize = gparquet_arrow_file_reader_finalize;
  gobject_class->set_property = gparquet_arrow_file_reader_set_property;
  gobject_class->get_property = gparquet_arrow_file_reader_get_property;

  spec = g_param_spec_pointer(
    "arrow-file-reader",
    "ArrowFileReader",
    "The raw parquet::arrow::FileReader *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARROW_FILE_READER, spec);
}

/**
 * gparquet_arrow_file_reader_new_arrow:
 * @source: Arrow source to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileReader.
 *
 * Since: 0.11.0
 */
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_arrow(GArrowSeekableInputStream *source, GError **error)
{
  auto arrow_random_access_file = garrow_seekable_input_stream_get_raw(source);
  auto arrow_memory_pool = arrow::default_memory_pool();
  auto parquet_arrow_file_reader_result =
    parquet::arrow::OpenFile(arrow_random_access_file, arrow_memory_pool);
  if (garrow::check(error,
                    parquet_arrow_file_reader_result,
                    "[parquet][arrow][file-reader][new-arrow]")) {
    return gparquet_arrow_file_reader_new_raw(
      parquet_arrow_file_reader_result->release());
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_reader_new_path:
 * @path: Path to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileReader.
 *
 * Since: 0.11.0
 */
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_path(const gchar *path, GError **error)
{
  auto arrow_memory_mapped_file =
    arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ);
  if (!garrow::check(error,
                     arrow_memory_mapped_file,
                     "[parquet][arrow][file-reader][new-path]")) {
    return NULL;
  }

  std::shared_ptr<arrow::io::RandomAccessFile> arrow_random_access_file =
    arrow_memory_mapped_file.ValueOrDie();
  auto arrow_memory_pool = arrow::default_memory_pool();
  auto parquet_arrow_file_reader_result =
    parquet::arrow::OpenFile(arrow_random_access_file, arrow_memory_pool);
  if (garrow::check(error,
                    parquet_arrow_file_reader_result,
                    "[parquet][arrow][file-reader][new-path]")) {
    return gparquet_arrow_file_reader_new_raw(
      parquet_arrow_file_reader_result->release());
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_reader_new_arrow_with_properties:
 * @source: Arrow source to be read.
 * @properties: (nullable): Reader properties or %NULL for the defaults.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * The reader copies @properties at construction. Later changes to @properties
 * do not affect the reader. The native source is retained by the reader.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileReader.
 *
 * Since: 26.0.0
 */
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_arrow_with_properties(GArrowSeekableInputStream *source,
                                                     GParquetReaderProperties *properties,
                                                     GError **error)
{
  auto reader = open_reader_with_properties(
    garrow_seekable_input_stream_get_raw(source),
    properties,
    error,
    "[parquet][arrow][file-reader][new-arrow-with-properties]");
  if (reader) {
    auto priv = GPARQUET_ARROW_FILE_READER_GET_PRIVATE(reader);
    priv->source = GARROW_SEEKABLE_INPUT_STREAM(g_object_ref(source));
  }
  return reader;
}

/**
 * gparquet_arrow_file_reader_new_path_with_properties:
 * @path: Path to be read.
 * @properties: (nullable): Reader properties or %NULL for the defaults.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * The reader copies @properties at construction. Later changes to @properties
 * do not affect the reader. The file is memory mapped, as with
 * gparquet_arrow_file_reader_new_path().
 *
 * Returns: (nullable): A newly created #GParquetArrowFileReader.
 *
 * Since: 26.0.0
 */
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_path_with_properties(const gchar *path,
                                                    GParquetReaderProperties *properties,
                                                    GError **error)
{
  const char *tag = "[parquet][arrow][file-reader][new-path-with-properties]";
  auto source = arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ);
  if (!garrow::check(error, source, tag)) {
    return NULL;
  }
  return open_reader_with_properties(*source, properties, error, tag);
}

/**
 * gparquet_arrow_file_reader_close:
 * @reader: A #GParquetArrowFileReader.
 *
 * Close the reader.
 *
 * Since: 23.0.0
 */
void
gparquet_arrow_file_reader_close(GParquetArrowFileReader *reader)
{
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);
  auto parquet_reader = parquet_arrow_file_reader->parquet_reader();
  if (parquet_reader) {
    parquet_reader->Close();
  }
}

/**
 * gparquet_arrow_file_reader_read_table:
 * @reader: A #GParquetArrowFileReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): A read #GArrowTable.
 *
 * Since: 0.11.0
 */
GArrowTable *
gparquet_arrow_file_reader_read_table(GParquetArrowFileReader *reader, GError **error)
{
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);
  auto arrow_table_result = parquet_arrow_file_reader->ReadTable();
  if (garrow::check(error,
                    arrow_table_result,
                    "[parquet][arrow][file-reader][read-table]")) {
    return garrow_table_new_raw(&(*arrow_table_result));
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_reader_read_row_group:
 * @reader: A #GParquetArrowFileReader.
 * @row_group_index: A row group index to be read.
 * @column_indices: (array length=n_column_indices) (nullable):
 *   Column indices to be read. %NULL means that all columns are read.
 *   If an index is negative, the index is counted backward from the
 *   end of the columns. `-1` means the last column.
 * @n_column_indices: The number of elements of @column_indices.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): A read #GArrowTable.
 *
 * Since: 1.0.0
 */
GArrowTable *
gparquet_arrow_file_reader_read_row_group(GParquetArrowFileReader *reader,
                                          gint row_group_index,
                                          gint *column_indices,
                                          gsize n_column_indices,
                                          GError **error)
{
  const gchar *tag = "[parquet][arrow][file-reader][read-row-group]";
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);
  arrow::Result<std::shared_ptr<arrow::Table>> arrow_table_result;
  if (column_indices) {
    const auto n_columns =
      parquet_arrow_file_reader->parquet_reader()->metadata()->num_columns();
    std::vector<int> parquet_column_indices;
    for (gsize i = 0; i < n_column_indices; ++i) {
      auto column_index = column_indices[i];
      if (!garrow_internal_index_adjust(column_index, n_columns)) {
        garrow_error_check(error,
                           arrow::Status::IndexError("Out of index: "
                                                     "<0..",
                                                     n_columns,
                                                     ">: "
                                                     "<",
                                                     column_index,
                                                     ">"),
                           tag);
        return NULL;
      }
      parquet_column_indices.push_back(column_index);
    }
    arrow_table_result =
      parquet_arrow_file_reader->ReadRowGroup(row_group_index, parquet_column_indices);
  } else {
    arrow_table_result = parquet_arrow_file_reader->ReadRowGroup(row_group_index);
  }
  if (garrow::check(error, arrow_table_result, tag)) {
    return garrow_table_new_raw(&(*arrow_table_result));
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_reader_get_schema:
 * @reader: A #GParquetArrowFileReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): A got #GArrowSchema.
 *
 * Since: 0.12.0
 */
GArrowSchema *
gparquet_arrow_file_reader_get_schema(GParquetArrowFileReader *reader, GError **error)
{
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);

  std::shared_ptr<arrow::Schema> arrow_schema;
  auto status = parquet_arrow_file_reader->GetSchema(&arrow_schema);
  if (garrow_error_check(error, status, "[parquet][arrow][file-reader][get-schema]")) {
    return garrow_schema_new_raw(&arrow_schema);
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_reader_read_column_data:
 * @reader: A #GParquetArrowFileReader.
 * @i: The index of the column to be read.
 *   If an index is negative, the index is counted backward from the
 *   end of the columns. `-1` means the last column.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): A read #GArrowChunkedArray.
 *
 * Since: 0.15.0
 */
GArrowChunkedArray *
gparquet_arrow_file_reader_read_column_data(GParquetArrowFileReader *reader,
                                            gint i,
                                            GError **error)
{
  const auto tag = "[parquet][arrow][file-reader][read-column-data]";
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);

  const auto n_columns =
    parquet_arrow_file_reader->parquet_reader()->metadata()->num_columns();
  if (!garrow_internal_index_adjust(i, n_columns)) {
    garrow_error_check(error,
                       arrow::Status::IndexError("Out of index: "
                                                 "<0..",
                                                 n_columns,
                                                 ">: "
                                                 "<",
                                                 i,
                                                 ">"),
                       tag);
    return NULL;
  }

  std::shared_ptr<arrow::ChunkedArray> arrow_chunked_array;
  auto status = parquet_arrow_file_reader->ReadColumn(i, &arrow_chunked_array);
  if (!garrow_error_check(error, status, tag)) {
    return NULL;
  }

  return garrow_chunked_array_new_raw(&arrow_chunked_array);
}

/**
 * gparquet_arrow_file_reader_get_n_row_groups:
 * @reader: A #GParquetArrowFileReader.
 *
 * Returns: The number of row groups.
 *
 * Since: 0.11.0
 */
gint
gparquet_arrow_file_reader_get_n_row_groups(GParquetArrowFileReader *reader)
{
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);
  return parquet_arrow_file_reader->num_row_groups();
}

/**
 * gparquet_arrow_file_reader_get_n_rows:
 * @reader: A #GParquetArrowFileReader.
 *
 * Returns: The number of rows.
 *
 * Since: 6.0.0
 */
gint64
gparquet_arrow_file_reader_get_n_rows(GParquetArrowFileReader *reader)
{
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);
  return parquet_arrow_file_reader->parquet_reader()->metadata()->num_rows();
}

/**
 * gparquet_arrow_file_reader_use_threads:
 * @reader: A #GParquetArrowFileReader.
 * @use_threads: Whether use threads or not.
 *
 * Since: 0.11.0
 */
void
gparquet_arrow_file_reader_set_use_threads(GParquetArrowFileReader *reader,
                                           gboolean use_threads)
{
  auto parquet_arrow_file_reader = gparquet_arrow_file_reader_get_raw(reader);
  parquet_arrow_file_reader->set_use_threads(use_threads);
}

/**
 * gparquet_arrow_file_reader_get_metadata:
 * @reader: A #GParquetArrowFileReader.
 *
 * Returns: (transfer full): The metadata.
 *
 * Since: 8.0.0
 */
GParquetFileMetadata *
gparquet_arrow_file_reader_get_metadata(GParquetArrowFileReader *reader)
{
  auto parquet_reader = gparquet_arrow_file_reader_get_raw(reader);
  auto parquet_metadata = parquet_reader->parquet_reader()->metadata();
  return gparquet_file_metadata_new_raw(&parquet_metadata);
}

G_END_DECLS

GParquetArrowFileReader *
gparquet_arrow_file_reader_new_raw(parquet::arrow::FileReader *parquet_arrow_file_reader)
{
  auto arrow_file_reader =
    GPARQUET_ARROW_FILE_READER(g_object_new(GPARQUET_TYPE_ARROW_FILE_READER,
                                            "arrow-file-reader",
                                            parquet_arrow_file_reader,
                                            NULL));
  return arrow_file_reader;
}

parquet::arrow::FileReader *
gparquet_arrow_file_reader_get_raw(GParquetArrowFileReader *arrow_file_reader)
{
  auto priv = GPARQUET_ARROW_FILE_READER_GET_PRIVATE(arrow_file_reader);
  return priv->arrow_file_reader;
}

parquet::ReaderProperties
gparquet_reader_properties_get_raw(GParquetReaderProperties *properties)
{
  auto priv = GPARQUET_READER_PROPERTIES_GET_PRIVATE(properties);
  return priv->properties;
}
