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
#include <arrow-glib/io-writeable.hpp>

G_BEGIN_DECLS

/**
 * SECTION: io-writeable
 * @title: GArrowIOWriteable
 * @short_description: Output interface
 *
 * #GArrowIOWriteable is an interface for output. Output must be
 * writeable.
 */

G_DEFINE_INTERFACE(GArrowIOWriteable,
                   garrow_io_writeable,
                   G_TYPE_OBJECT)

static void
garrow_io_writeable_default_init (GArrowIOWriteableInterface *iface)
{
}

/**
 * garrow_io_writeable_write:
 * @writeable: A #GArrowIOWriteable.
 * @data: (array length=n_bytes): The data to be written.
 * @n_bytes: The number of bytes to be written.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_io_writeable_write(GArrowIOWriteable *writeable,
                          const guint8 *data,
                          gint64 n_bytes,
                          GError **error)
{
  const auto arrow_writeable = garrow_io_writeable_get_raw(writeable);

  auto status = arrow_writeable->Write(data, n_bytes);
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[io][writeable][write]");
    return FALSE;
  }
}

/**
 * garrow_io_writeable_flush:
 * @writeable: A #GArrowIOWriteable.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * It ensures writing all data on memory to storage.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_io_writeable_flush(GArrowIOWriteable *writeable,
                          GError **error)
{
  const auto arrow_writeable = garrow_io_writeable_get_raw(writeable);

  auto status = arrow_writeable->Flush();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[io][writeable][flush]");
    return FALSE;
  }
}

G_END_DECLS

std::shared_ptr<arrow::io::Writeable>
garrow_io_writeable_get_raw(GArrowIOWriteable *writeable)
{
  auto *iface = GARROW_IO_WRITEABLE_GET_IFACE(writeable);
  return iface->get_raw(writeable);
}
