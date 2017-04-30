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

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/random-access-file.hpp>

G_BEGIN_DECLS

/**
 * SECTION: random-access-file
 * @title: GArrowRandomAccessFile
 * @short_description: File input interface
 *
 * #GArrowRandomAccessFile is an interface for file input.
 */

G_DEFINE_INTERFACE(GArrowRandomAccessFile,
                   garrow_random_access_file,
                   G_TYPE_OBJECT)

static void
garrow_random_access_file_default_init (GArrowRandomAccessFileInterface *iface)
{
}

/**
 * garrow_random_access_file_get_size:
 * @file: A #GArrowRandomAccessFile.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The size of the file.
 */
guint64
garrow_random_access_file_get_size(GArrowRandomAccessFile *file,
                                 GError **error)
{
  auto *iface = GARROW_RANDOM_ACCESS_FILE_GET_IFACE(file);
  auto arrow_random_access_file = iface->get_raw(file);
  int64_t size;

  auto status = arrow_random_access_file->GetSize(&size);
  if (garrow_error_check(error, status, "[io][random-access-file][get-size]")) {
    return size;
  } else {
    return 0;
  }
}

/**
 * garrow_random_access_file_get_support_zero_copy:
 * @file: A #GArrowRandomAccessFile.
 *
 * Returns: Whether zero copy read is supported or not.
 */
gboolean
garrow_random_access_file_get_support_zero_copy(GArrowRandomAccessFile *file)
{
  auto *iface = GARROW_RANDOM_ACCESS_FILE_GET_IFACE(file);
  auto arrow_random_access_file = iface->get_raw(file);

  return arrow_random_access_file->supports_zero_copy();
}

/**
 * garrow_random_access_file_read_at:
 * @file: A #GArrowRandomAccessFile.
 * @position: The read start position.
 * @n_bytes: The number of bytes to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): #GArrowBuffer that has read
 *   data on success, %NULL if there was an error.
 */
GArrowBuffer *
garrow_random_access_file_read_at(GArrowRandomAccessFile *file,
                                     gint64 position,
                                     gint64 n_bytes,
                                     GError **error)
{
  const auto arrow_random_access_file =
    garrow_random_access_file_get_raw(file);

  std::shared_ptr<arrow::Buffer> arrow_buffer;
  auto status = arrow_random_access_file->ReadAt(position,
                                                 n_bytes,
                                                 &arrow_buffer);
  if (garrow_error_check(error, status, "[io][random-access-file][read-at]")) {
    return garrow_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}

G_END_DECLS

std::shared_ptr<arrow::io::RandomAccessFile>
garrow_random_access_file_get_raw(GArrowRandomAccessFile *random_access_file)
{
  auto *iface = GARROW_RANDOM_ACCESS_FILE_GET_IFACE(random_access_file);
  return iface->get_raw(random_access_file);
}
