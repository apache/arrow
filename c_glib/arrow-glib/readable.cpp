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
#include <arrow-glib/readable.hpp>

G_BEGIN_DECLS

/**
 * SECTION: readable
 * @title: GArrowReadable
 * @short_description: Input interface
 *
 * #GArrowReadable is an interface for input. Input must be
 * readable.
 */

G_DEFINE_INTERFACE(GArrowReadable,
                   garrow_readable,
                   G_TYPE_OBJECT)

static void
garrow_readable_default_init (GArrowReadableInterface *iface)
{
}

/**
 * garrow_readable_read:
 * @readable: A #GArrowReadable.
 * @n_bytes: The number of bytes to be read.
 * @n_read_bytes: (out): The read number of bytes.
 * @buffer: (array length=n_bytes): The buffer to be read data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_readable_read(GArrowReadable *readable,
                        gint64 n_bytes,
                        gint64 *n_read_bytes,
                        guint8 *buffer,
                        GError **error)
{
  const auto arrow_readable = garrow_readable_get_raw(readable);

  auto status = arrow_readable->Read(n_bytes, n_read_bytes, buffer);
  return garrow_error_check(error, status, "[io][readable][read]");
}

G_END_DECLS

std::shared_ptr<arrow::io::Readable>
garrow_readable_get_raw(GArrowReadable *readable)
{
  auto *iface = GARROW_READABLE_GET_IFACE(readable);
  return iface->get_raw(readable);
}
