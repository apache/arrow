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
#include <arrow-glib/writable.hpp>

G_BEGIN_DECLS

/**
 * SECTION: writable
 * @title: GArrowWritable
 * @short_description: Output interface
 *
 * #GArrowWritable is an interface for output. Output must be
 * writable.
 */

G_DEFINE_INTERFACE(GArrowWritable,
                   garrow_writable,
                   G_TYPE_OBJECT)

static void
garrow_writable_default_init(GArrowWritableInterface *iface)
{
}

/**
 * garrow_writable_write:
 * @writable: A #GArrowWritable.
 * @data: (array length=n_bytes): The data to be written.
 * @n_bytes: The number of bytes to be written.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_writable_write(GArrowWritable *writable,
                      const guint8 *data,
                      gint64 n_bytes,
                      GError **error)
{
  const auto arrow_writable = garrow_writable_get_raw(writable);

  auto status = arrow_writable->Write(data, n_bytes);
  return garrow_error_check(error, status, "[io][writable][write]");
}

/**
 * garrow_writable_flush:
 * @writable: A #GArrowWritable.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * It ensures writing all data on memory to storage.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_writable_flush(GArrowWritable *writable,
                      GError **error)
{
  const auto arrow_writable = garrow_writable_get_raw(writable);

  auto status = arrow_writable->Flush();
  return garrow_error_check(error, status, "[io][writable][flush]");
}

G_END_DECLS

std::shared_ptr<arrow::io::Writable>
garrow_writable_get_raw(GArrowWritable *writable)
{
  auto *iface = GARROW_WRITABLE_GET_IFACE(writable);
  return iface->get_raw(writable);
}
