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

#define EXAMPLE_TYPE_UUID_ARRAY (example_uuid_array_get_type())
G_DECLARE_DERIVABLE_TYPE(ExampleUUIDArray,
                         example_uuid_array,
                         EXAMPLE,
                         UUID_ARRAY,
                         GArrowExtensionArray)
struct _ExampleUUIDArrayClass
{
  GArrowExtensionArrayClass parent_class;
};

G_DEFINE_TYPE(ExampleUUIDArray,
              example_uuid_array,
              GARROW_TYPE_EXTENSION_ARRAY)

static void
example_uuid_array_init(ExampleUUIDArray *object)
{
}

static void
example_uuid_array_class_init(ExampleUUIDArrayClass *klass)
{
}


#define EXAMPLE_TYPE_UUID_DATA_TYPE (example_uuid_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(ExampleUUIDDataType,
                         example_uuid_data_type,
                         EXAMPLE,
                         UUID_DATA_TYPE,
                         GArrowExtensionDataType)
struct _ExampleUUIDDataTypeClass
{
  GArrowExtensionDataTypeClass parent_class;
};


G_DEFINE_TYPE(ExampleUUIDDataType,
              example_uuid_data_type,
              GARROW_TYPE_EXTENSION_DATA_TYPE)

static gchar *
example_uuid_data_type_get_extension_name(GArrowExtensionDataType *data_type)
{
  return g_strdup("uuid");
}

static gboolean
example_uuid_data_type_equal(GArrowExtensionDataType *data_type,
                             GArrowExtensionDataType *other_data_type)
{
  /* Compare parameters if they exists. */
  return TRUE;
}

static const gchar *example_uuid_data_type_serialize_id = "uuid-serialized";
static ExampleUUIDDataType *example_uuid_data_type_new(void);

static GArrowDataType *
example_uuid_data_type_deserialize(GArrowExtensionDataType *data_type,
                                   GArrowDataType *storage_data_type,
                                   GBytes *serialized_data,
                                   GError **error)
{
  gsize raw_data_size;
  gconstpointer raw_data = g_bytes_get_data(serialized_data, &raw_data_size);
  if (!(raw_data_size == strlen(example_uuid_data_type_serialize_id) &&
        strncmp(raw_data,
                example_uuid_data_type_serialize_id,
                raw_data_size) == 0)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_INVALID,
                "[uuid-data-type][deserialize] "
                "identifier must be <%s>: <%.*s>",
                example_uuid_data_type_serialize_id,
                (gint)raw_data_size,
                (const gchar *)raw_data);
    return NULL;
  }

  GArrowDataType *expected_storage_data_type;
  g_object_get(data_type,
               "storage-data-type", &expected_storage_data_type,
               NULL);
  if (!garrow_data_type_equal(storage_data_type,
                              expected_storage_data_type)) {
    gchar *expected = garrow_data_type_to_string(expected_storage_data_type);
    gchar *actual = garrow_data_type_to_string(storage_data_type);
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_INVALID,
                "[uuid-data-type][deserialize] "
                "storage data type must be <%s>: <%s>",
                expected,
                actual);
    g_free(actual);
    g_free(expected);
    return NULL;
  }

  return GARROW_DATA_TYPE(example_uuid_data_type_new());
}

static GBytes *
example_uuid_data_type_serialize(GArrowExtensionDataType *data_type)
{
  return g_bytes_new_static(example_uuid_data_type_serialize_id,
                            strlen(example_uuid_data_type_serialize_id));
}

static GType
example_uuid_data_type_get_array_gtype(GArrowExtensionDataType *data_type)
{
  return EXAMPLE_TYPE_UUID_ARRAY;
}

static void
example_uuid_data_type_init(ExampleUUIDDataType *object)
{
}

static void
example_uuid_data_type_class_init(ExampleUUIDDataTypeClass *klass)
{
  GArrowExtensionDataTypeClass *extension_klass =
    GARROW_EXTENSION_DATA_TYPE_CLASS(klass);
  extension_klass->get_extension_name =
    example_uuid_data_type_get_extension_name;
  extension_klass->equal           = example_uuid_data_type_equal;
  extension_klass->deserialize     = example_uuid_data_type_deserialize;
  extension_klass->serialize       = example_uuid_data_type_serialize;
  extension_klass->get_array_gtype = example_uuid_data_type_get_array_gtype;
}

static ExampleUUIDDataType *
example_uuid_data_type_new(void)
{
  GArrowFixedSizeBinaryDataType *storage_data_type =
    garrow_fixed_size_binary_data_type_new(16);
  return g_object_new(EXAMPLE_TYPE_UUID_DATA_TYPE,
                      "storage-data-type", storage_data_type,
                      NULL);
}


int
main(int argc, char **argv)
{
  GArrowExtensionDataTypeRegistry *registry =
    garrow_extension_data_type_registry_default();

  /* Create UUID extension data type. */
  ExampleUUIDDataType *uuid_data_type = example_uuid_data_type_new();
  GArrowExtensionDataType *extension_data_type =
    GARROW_EXTENSION_DATA_TYPE(uuid_data_type);
  /* Register the created UUID extension data type. */
  GError *error = NULL;
  if (!garrow_extension_data_type_registry_register(registry,
                                                    extension_data_type,
                                                    &error)) {
    g_print("failed to register: %s\n", error->message);
    g_error_free(error);
    g_object_unref(registry);
    return EXIT_FAILURE;
  }

  {
    /* Build storage data for the created UUID extension data type. */
    GArrowFixedSizeBinaryDataType *storage_data_type;
    g_object_get(extension_data_type,
                 "storage-data-type", &storage_data_type,
                 NULL);
    GArrowFixedSizeBinaryArrayBuilder *builder =
      garrow_fixed_size_binary_array_builder_new(storage_data_type);
    g_object_unref(storage_data_type);
    garrow_fixed_size_binary_array_builder_append_value(
      builder,
      (const guint8 *)"0123456789012345",
      16,
      &error);
    if (!error) {
      garrow_array_builder_append_null(GARROW_ARRAY_BUILDER(builder), &error);
    }
    if (!error) {
      garrow_fixed_size_binary_array_builder_append_value(
        builder,
        (const guint8 *)"abcdefghijklmnop",
        16,
        &error);
    }
    if (error) {
      g_print("failed to append elements: %s\n", error->message);
      g_error_free(error);
      g_object_unref(builder);
      goto exit;
    }
    GArrowArray *storage =
      garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder), &error);
    g_object_unref(builder);
    if (error) {
      g_print("failed to build storage: %s\n", error->message);
      g_error_free(error);
      goto exit;
    }

    /* Wrap the created storage data as the created UUID extension array. */
    GArrowExtensionArray *extension_array =
      garrow_extension_data_type_wrap_array(extension_data_type,
                                            storage);
    g_object_unref(storage);
    gint64 n_rows = garrow_array_get_length(GARROW_ARRAY(extension_array));

    /* Create a record batch to serialize the created UUID extension array. */
    GList *fields = NULL;
    fields = g_list_append(fields,
                           garrow_field_new("uuid",
                                            GARROW_DATA_TYPE(uuid_data_type)));
    GArrowSchema *schema = garrow_schema_new(fields);
    g_list_free_full(fields, g_object_unref);
    GList *columns = NULL;
    columns = g_list_append(columns, extension_array);
    GArrowRecordBatch *record_batch =
      garrow_record_batch_new(schema, n_rows, columns, &error);
    g_list_free_full(columns, g_object_unref);
    if (error) {
      g_print("failed to create record batch: %s\n", error->message);
      g_error_free(error);
      g_object_unref(schema);
      goto exit;
    }

    /* Serialize the created record batch. */
    GArrowResizableBuffer *buffer = garrow_resizable_buffer_new(0, &error);
    if (error) {
      g_print("failed to create buffer: %s\n", error->message);
      g_error_free(error);
      g_object_unref(schema);
      g_object_unref(record_batch);
      goto exit;
    }
    {
      GArrowBufferOutputStream *output =
        garrow_buffer_output_stream_new(buffer);
      GArrowRecordBatchStreamWriter *writer =
        garrow_record_batch_stream_writer_new(GARROW_OUTPUT_STREAM(output),
                                              schema,
                                              &error);
      if (error) {
        g_print("failed to create writer: %s\n", error->message);
        g_error_free(error);
        g_object_unref(output);
        g_object_unref(buffer);
        g_object_unref(schema);
        g_object_unref(record_batch);
        goto exit;
      }
      garrow_record_batch_writer_write_record_batch(
        GARROW_RECORD_BATCH_WRITER(writer),
        record_batch,
        &error);
      if (error) {
        g_print("failed to write record batch: %s\n", error->message);
        g_error_free(error);
        g_object_unref(writer);
        g_object_unref(output);
        g_object_unref(buffer);
        g_object_unref(schema);
        g_object_unref(record_batch);
        goto exit;
      }
      g_object_unref(schema);
      g_object_unref(record_batch);
      garrow_record_batch_writer_close(GARROW_RECORD_BATCH_WRITER(writer),
                                       &error);
      g_object_unref(writer);
      g_object_unref(output);
      if (error) {
        g_print("failed to close writer: %s\n", error->message);
        g_error_free(error);
        g_object_unref(buffer);
        goto exit;
      }
    }

    /* Deserialize the serialized record batch. */
    {
      GArrowBufferInputStream *input =
        garrow_buffer_input_stream_new(GARROW_BUFFER(buffer));
      GArrowRecordBatchStreamReader *reader =
        garrow_record_batch_stream_reader_new(GARROW_INPUT_STREAM(input),
                                              &error);
      if (error) {
        g_print("failed to create reader: %s\n", error->message);
        g_error_free(error);
        g_object_unref(input);
        g_object_unref(buffer);
        goto exit;
      }
      record_batch =
        garrow_record_batch_reader_read_next(GARROW_RECORD_BATCH_READER(reader),
                                             &error);
      if (error) {
        g_print("failed to read record batch: %s\n", error->message);
        g_error_free(error);
        g_object_unref(reader);
        g_object_unref(input);
        g_object_unref(buffer);
        goto exit;
      }
      /* Show the deserialize record batch. */
      gchar *record_batch_content =
        garrow_record_batch_to_string(record_batch,
                                      &error);
      if (error) {
        g_print("failed to dump record batch content: %s\n", error->message);
        g_error_free(error);
        error = NULL;
      } else {
        g_print("record batch:\n%s\n", record_batch_content);
      }
      /* Get the deserialize UUID extension array. */
      GArrowArray *deserialized_array =
        garrow_record_batch_get_column_data(record_batch, 0);
      g_print("array: %s\n", G_OBJECT_TYPE_NAME(deserialized_array));
      g_object_unref(deserialized_array);

      g_object_unref(record_batch);
      g_object_unref(reader);
      g_object_unref(input);
    }

    g_object_unref(buffer);
  }

exit:
  /* Unregister the created UUID extension data type. */
  {
    gchar *data_type_name =
      garrow_extension_data_type_get_extension_name(extension_data_type);
    gboolean success =
      garrow_extension_data_type_registry_unregister(registry,
                                                     data_type_name,
                                                     &error);
    g_free(data_type_name);
    if (!success) {
      g_print("failed to unregister: %s\n", error->message);
      g_error_free(error);
      g_object_unref(registry);
      return EXIT_FAILURE;
    }
  }

  g_object_unref(registry);

  return EXIT_SUCCESS;
}
