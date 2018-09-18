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

#include <arrow/api.h>

#include <arrow-glib/error.hpp>
#include <arrow-glib/writable-file.hpp>

G_BEGIN_DECLS

/**
 * SECTION: writable-file
 * @title: GArrowWritableFile
 * @short_description: File output interface
 *
 * #GArrowWritableFile is an interface for file output.
 */

G_DEFINE_INTERFACE(GArrowWritableFile,
                   garrow_writable_file,
                   G_TYPE_OBJECT)

static void
garrow_writable_file_default_init(GArrowWritableFileInterface *iface)
{
}

/**
 * garrow_writable_file_write_at:
 * @writable_file: A #GArrowWritableFile.
 * @position: The write start position.
 * @data: (array length=n_bytes): The data to be written.
 * @n_bytes: The number of bytes to be written.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_writable_file_write_at(GArrowWritableFile *writable_file,
                              gint64 position,
                              const guint8 *data,
                              gint64 n_bytes,
                              GError **error)
{
  const auto arrow_writable_file =
    garrow_writable_file_get_raw(writable_file);

  auto status = arrow_writable_file->WriteAt(position, data, n_bytes);
  return garrow_error_check(error, status, "[io][writable-file][write-at]");
}

G_END_DECLS

std::shared_ptr<arrow::io::WritableFile>
garrow_writable_file_get_raw(GArrowWritableFile *writable_file)
{
  auto *iface = GARROW_WRITABLE_FILE_GET_IFACE(writable_file);
  return iface->get_raw(writable_file);
}
