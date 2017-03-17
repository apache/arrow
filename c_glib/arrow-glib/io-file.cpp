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
#include <arrow-glib/io-file.hpp>
#include <arrow-glib/io-file-mode.hpp>

G_BEGIN_DECLS

/**
 * SECTION: io-file
 * @title: GArrowIOFile
 * @short_description: File interface
 *
 * #GArrowIOFile is an interface for file.
 */

G_DEFINE_INTERFACE(GArrowIOFile,
                   garrow_io_file,
                   G_TYPE_OBJECT)

static void
garrow_io_file_default_init (GArrowIOFileInterface *iface)
{
}

/**
 * garrow_io_file_close:
 * @file: A #GArrowIOFile.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_io_file_close(GArrowIOFile *file,
                     GError **error)
{
  auto arrow_file = garrow_io_file_get_raw(file);

  auto status = arrow_file->Close();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[io][file][close]");
    return FALSE;
  }
}

/**
 * garrow_io_file_tell:
 * @file: A #GArrowIOFile.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The current offset on success, -1 if there was an error.
 */
gint64
garrow_io_file_tell(GArrowIOFile *file,
                    GError **error)
{
  auto arrow_file = garrow_io_file_get_raw(file);

  gint64 position;
  auto status = arrow_file->Tell(&position);
  if (status.ok()) {
    return position;
  } else {
    garrow_error_set(error, status, "[io][file][tell]");
    return -1;
  }
}

/**
 * garrow_io_file_get_mode:
 * @file: A #GArrowIOFile.
 *
 * Returns: The mode of the file.
 */
GArrowIOFileMode
garrow_io_file_get_mode(GArrowIOFile *file)
{
  auto arrow_file = garrow_io_file_get_raw(file);

  auto arrow_mode = arrow_file->mode();
  return garrow_io_file_mode_from_raw(arrow_mode);
}

G_END_DECLS

std::shared_ptr<arrow::io::FileInterface>
garrow_io_file_get_raw(GArrowIOFile *file)
{
  auto *iface = GARROW_IO_FILE_GET_IFACE(file);
  return iface->get_raw(file);
}
