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

#ifdef G_OS_UNIX
#  include <glib-unix.h>
#  include <signal.h>
#endif

static void
service_event(GSocketListener *listener,
              GSocketListenerEvent event,
              GSocket *socket,
              gpointer user_data)
{
  if (event != G_SOCKET_LISTENER_BOUND) {
    return;
  }

  GError *error = NULL;
  GSocketAddress* local_address = g_socket_get_local_address(socket, &error);
  if (!local_address) {
    g_print("failed to get local address: %s\n", error->message);
    g_error_free(error);
    g_object_unref(socket);
    return;
  }
  gchar *local_address_string =
    g_socket_connectable_to_string(G_SOCKET_CONNECTABLE(local_address));
  g_print("address: %s\n", local_address_string);
  g_free(local_address_string);
  g_object_unref(local_address);
}

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
    array = garrow_record_batch_get_column_data(record_batch, nth_column);
    print_array(array);
    g_object_unref(array);
  }
}

static gboolean
service_incoming(GSocketService *service,
                 GSocketConnection *connection,
                 GObject *source_object,
                 gpointer user_data)
{
  GArrowGIOInputStream *input =
    garrow_gio_input_stream_new(
      g_io_stream_get_input_stream(G_IO_STREAM(connection)));
  GError *error = NULL;
  GArrowRecordBatchStreamReader *reader =
    garrow_record_batch_stream_reader_new(GARROW_INPUT_STREAM(input), &error);
  if (!reader) {
    g_print("failed to create reader: %s\n", error->message);
    g_error_free(error);
    g_object_unref(input);
    return FALSE;
  }

  while (TRUE) {
    GArrowRecordBatch *record_batch =
      garrow_record_batch_reader_read_next(GARROW_RECORD_BATCH_READER(reader),
                                           &error);
    if (error) {
      g_print("failed to read the next record batch: %s\n", error->message);
      g_error_free(error);
      g_object_unref(reader);
      g_object_unref(input);
      return EXIT_FAILURE;
    }

    if (!record_batch) {
      break;
    }

    print_record_batch(record_batch);
    g_object_unref(record_batch);
  }

  g_object_unref(reader);
  g_object_unref(input);

  return FALSE;
}

#ifdef G_OS_UNIX
typedef struct {
  GSocketService *service;
  GMainLoop *loop;
} StopData;

static gboolean
stop(gpointer user_data)
{
  StopData* data = user_data;
  g_object_unref(data->service);
  g_main_loop_quit(data->loop);
  return G_SOURCE_REMOVE;
}
#endif

int
main(int argc, char **argv)
{
  GSocketService *service = g_threaded_socket_service_new(-1);
  g_signal_connect(service, "event", G_CALLBACK(service_event), NULL);
  g_signal_connect(service, "incoming", G_CALLBACK(service_incoming), NULL);

  GError *error = NULL;
  gboolean success =
    g_socket_listener_add_any_inet_port(G_SOCKET_LISTENER(service),
                                        NULL,
                                        &error);
  if (!success) {
    g_print("failed to add a listen IP address: %s\n", error->message);
    g_error_free(error);
    return EXIT_FAILURE;
  }

  g_socket_service_start(service);

  GMainLoop *loop = g_main_loop_new(NULL, FALSE);
#ifdef G_OS_UNIX
  StopData data;
  data.service = service;
  data.loop = loop;
  g_unix_signal_add(SIGINT, stop, &data);
  g_unix_signal_add(SIGTERM, stop, &data);
#else
  /* TODO: Implement graceful stop. */
#endif
  g_main_loop_run(loop);
  g_main_loop_unref(loop);

  return EXIT_SUCCESS;
}
