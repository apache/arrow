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

#include <stdlib.h>

#include <arrow-glib/arrow-glib.h>

static void
print_array(GArrowArray *array)
{
  GArrowType value_type;
  gint64 i, n;

  value_type = garrow_array_get_value_type(array);

  g_print("[");
  n = garrow_array_get_length(array);

#define ARRAY_CASE(type, Type, TYPE, format)                            \
  case GARROW_TYPE_ ## TYPE:                                            \
    {                                                                   \
      GArrow ## Type ## Array *real_array;                              \
      real_array = GARROW_ ## TYPE ## _ARRAY(array);                    \
      for (i = 0; i < n; i++) {                                         \
        if (i > 0) {                                                    \
          g_print(", ");                                                \
        }                                                               \
        g_print(format,                                                 \
                garrow_ ## type ## _array_get_value(real_array, i));    \
      }                                                                 \
    }                                                                   \
    break

  switch (value_type) {
    ARRAY_CASE(uint8,  UInt8,  UINT8,  "%hhu");
    ARRAY_CASE(uint16, UInt16, UINT16, "%" G_GUINT16_FORMAT);
    ARRAY_CASE(uint32, UInt32, UINT32, "%" G_GUINT32_FORMAT);
    ARRAY_CASE(uint64, UInt64, UINT64, "%" G_GUINT64_FORMAT);
    ARRAY_CASE( int8,   Int8,   INT8,  "%hhd");
    ARRAY_CASE( int16,  Int16,  INT16, "%" G_GINT16_FORMAT);
    ARRAY_CASE( int32,  Int32,  INT32, "%" G_GINT32_FORMAT);
    ARRAY_CASE( int64,  Int64,  INT64, "%" G_GINT64_FORMAT);
    ARRAY_CASE( float,  Float,  FLOAT, "%g");
    ARRAY_CASE(double, Double, DOUBLE, "%g");
  default:
    break;
  }
#undef ARRAY_CASE

  g_print("]\n");
}

static void
print_record_batch(GArrowRecordBatch *record_batch)
{
  guint nth_column, n_columns;

  n_columns = garrow_record_batch_get_n_columns(record_batch);
  for (nth_column = 0; nth_column < n_columns; nth_column++) {
    GArrowArray *array;

    g_print("columns[%u](%s): ",
            nth_column,
            garrow_record_batch_get_column_name(record_batch, nth_column));
    array = garrow_record_batch_get_column(record_batch, nth_column);
    print_array(array);
  }
}

int
main(int argc, char **argv)
{
  const char *input_path = "/tmp/batch.arrow";
  GArrowMemoryMappedInputStream *input;
  GError *error = NULL;

  if (argc > 1)
    input_path = argv[1];
  input = garrow_memory_mapped_input_stream_new(input_path,
                                                &error);
  if (!input) {
    g_print("failed to open file: %s\n", error->message);
    g_error_free(error);
    return EXIT_FAILURE;
  }

  {
    GArrowRecordBatchFileReader *reader;

    reader =
      garrow_record_batch_file_reader_new(GARROW_SEEKABLE_INPUT_STREAM(input),
                                          &error);
    if (!reader) {
      g_print("failed to open file reader: %s\n", error->message);
      g_error_free(error);
      g_object_unref(input);
      return EXIT_FAILURE;
    }

    {
      guint i, n;

      n = garrow_record_batch_file_reader_get_n_record_batches(reader);
      for (i = 0; i < n; i++) {
        GArrowRecordBatch *record_batch;

        record_batch =
          garrow_record_batch_file_reader_read_record_batch(reader, i, &error);
        if (!record_batch) {
          g_print("failed to open file reader: %s\n", error->message);
          g_error_free(error);
          g_object_unref(reader);
          g_object_unref(input);
          return EXIT_FAILURE;
        }

        print_record_batch(record_batch);
        g_object_unref(record_batch);
      }
    }

    g_object_unref(reader);
  }

  g_object_unref(input);

  return EXIT_SUCCESS;
}
