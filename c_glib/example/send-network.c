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

static GArrowSchema *
build_schema(void)
{
  GList *fields = NULL;
  GArrowBooleanDataType *boolean_data_type = garrow_boolean_data_type_new();
  fields = g_list_append(fields,
                         garrow_field_new("boolean",
                                          GARROW_DATA_TYPE(boolean_data_type)));
  GArrowInt32DataType *int32_data_type = garrow_int32_data_type_new();
  fields = g_list_append(fields,
                         garrow_field_new("int32",
                                          GARROW_DATA_TYPE(int32_data_type)));
  GArrowSchema *schema = garrow_schema_new(fields);
  g_list_free_full(fields, g_object_unref);

  return schema;
}

static GArrowRecordBatch *
build_record_batch(void)
{
  GArrowSchema *schema = build_schema();
  if (!schema) {
    return NULL;
  }
  GError *error = NULL;
  GArrowRecordBatchBuilder *builder =
    garrow_record_batch_builder_new(schema, &error);
  g_object_unref(schema);
  if (!builder) {
    g_print("failed to build record batch builder: %s\n", error->message);
    g_error_free(error);
    return NULL;
  }

  const gint64 n_records = 3;
  GArrowBooleanArrayBuilder *boolean_builder =
    GARROW_BOOLEAN_ARRAY_BUILDER(
      garrow_record_batch_builder_get_column_builder(builder, 0));
  gboolean boolean_values[] = {TRUE, TRUE, FALSE};
  gboolean boolean_is_valids[] = {TRUE, FALSE, TRUE};
  if (!garrow_boolean_array_builder_append_values(boolean_builder,
                                                  boolean_values,
                                                  n_records,
                                                  boolean_is_valids,
                                                  n_records,
                                                  &error)) {
    g_print("failed to append boolean values: %s\n", error->message);
    g_error_free(error);
    g_object_unref(boolean_builder);
    g_object_unref(builder);
    return NULL;
  }

  GArrowInt32ArrayBuilder *int32_builder =
    GARROW_INT32_ARRAY_BUILDER(
      garrow_record_batch_builder_get_column_builder(builder, 1));
  gint32 int32_values[] = {1, 11, 111};
  gint32 int32_is_valids[] = {FALSE, TRUE, TRUE};
  if (!garrow_int32_array_builder_append_values(int32_builder,
                                                int32_values,
                                                n_records,
                                                int32_is_valids,
                                                n_records,
                                                &error)) {
    g_print("failed to append int32 values: %s\n", error->message);
    g_error_free(error);
    g_object_unref(int32_builder);
    g_object_unref(builder);
    return NULL;
  }

  GArrowRecordBatch *record_batch =
    garrow_record_batch_builder_flush(builder, &error);
  if (!record_batch) {
    g_print("failed to build record batch: %s\n", error->message);
    g_error_free(error);
    g_object_unref(builder);
    return NULL;
  }

  g_object_unref(builder);

  return record_batch;
}

int
main(int argc, char **argv)
{
  if (argc != 2) {
    g_print("Usage: %s PORT\n", argv[0]);
    g_print(" e.g.: %s 2929\n", argv[0]);
    return EXIT_FAILURE;
  }

  guint port = atoi(argv[1]);

  GSocketClient *client = g_socket_client_new();
  GSocketAddress *address = g_inet_socket_address_new_from_string("127.0.0.1",
                                                                  port);
  GError *error = NULL;
  GSocketConnection *connection =
    g_socket_client_connect(client, G_SOCKET_CONNECTABLE(address), NULL, &error);
  if (!connection) {
    g_print("failed to connect: %s\n", error->message);
    g_error_free(error);
    return EXIT_FAILURE;
  }

  GArrowSchema *schema = build_schema();
  if (!schema) {
    return EXIT_FAILURE;
  }
  GArrowGIOOutputStream *output =
    garrow_gio_output_stream_new(
      g_io_stream_get_output_stream(G_IO_STREAM(connection)));
  GArrowRecordBatchStreamWriter *writer =
    garrow_record_batch_stream_writer_new(GARROW_OUTPUT_STREAM(output),
                                          schema,
                                          &error);
  g_object_unref(schema);
  if (!writer) {
    g_print("failed to create writer: %s\n", error->message);
    g_error_free(error);
    g_object_unref(output);
    g_object_unref(connection);
    g_object_unref(client);
    return EXIT_FAILURE;
  }

  gsize n_record_batches = 5;
  gsize i;
  for (i = 0; i < n_record_batches; i++) {
    GArrowRecordBatch *record_batch = build_record_batch();
    if (!record_batch) {
      g_object_unref(writer);
      g_object_unref(output);
      g_object_unref(connection);
      g_object_unref(client);
      return EXIT_FAILURE;
    }
    gboolean success =
      garrow_record_batch_writer_write_record_batch(
        GARROW_RECORD_BATCH_WRITER(writer),
        record_batch,
        &error);
    g_object_unref(record_batch);
    if (!success) {
      g_print("failed to write record batch: %s\n", error->message);
      g_error_free(error);
      g_object_unref(output);
      g_object_unref(connection);
      g_object_unref(client);
      return EXIT_FAILURE;
    }
  }

  g_object_unref(writer);
  g_object_unref(output);
  g_object_unref(connection);
  g_object_unref(client);

  return EXIT_SUCCESS;
}
