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

G_BEGIN_DECLS

/**
 * SECTION: arrow-file-reader
 * @short_description: Arrow file reader class
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetArrowFileReader is a class for reading Apache Parquet data
 * from file and returns them as Apache Arrow data.
 */

typedef struct GParquetArrowFileReaderPrivate_
{
  parquet::arrow::FileReader *arrow_file_reader;
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
  std::unique_ptr<parquet::arrow::FileReader> parquet_arrow_file_reader;
  auto status = parquet::arrow::OpenFile(arrow_random_access_file,
                                         arrow_memory_pool,
                                         &parquet_arrow_file_reader);
  if (garrow_error_check(error, status, "[parquet][arrow][file-reader][new-arrow]")) {
    return gparquet_arrow_file_reader_new_raw(parquet_arrow_file_reader.release());
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
  std::unique_ptr<parquet::arrow::FileReader> parquet_arrow_file_reader;
  auto status = parquet::arrow::OpenFile(arrow_random_access_file,
                                         arrow_memory_pool,
                                         &parquet_arrow_file_reader);
  if (garrow::check(error, status, "[parquet][arrow][file-reader][new-path]")) {
    return gparquet_arrow_file_reader_new_raw(parquet_arrow_file_reader.release());
  } else {
    return NULL;
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
  std::shared_ptr<arrow::Table> arrow_table;
  auto status = parquet_arrow_file_reader->ReadTable(&arrow_table);
  if (garrow_error_check(error, status, "[parquet][arrow][file-reader][read-table]")) {
    return garrow_table_new_raw(&arrow_table);
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
  std::shared_ptr<arrow::Table> arrow_table;
  arrow::Status status;
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
    status = parquet_arrow_file_reader->ReadRowGroup(row_group_index,
                                                     parquet_column_indices,
                                                     &arrow_table);
  } else {
    status = parquet_arrow_file_reader->ReadRowGroup(row_group_index, &arrow_table);
  }
  if (garrow_error_check(error, status, tag)) {
    return garrow_table_new_raw(&arrow_table);
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
