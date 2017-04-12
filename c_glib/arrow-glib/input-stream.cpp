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
#include <arrow-glib/input-stream.hpp>

G_BEGIN_DECLS

/**
 * SECTION: input-stream
 * @title: GArrowInputStream
 * @short_description: Stream input interface
 *
 * #GArrowInputStream is an interface for stream input. Stream input
 * is file based and readable.
 */

G_DEFINE_INTERFACE(GArrowInputStream,
                   garrow_input_stream,
                   G_TYPE_OBJECT)

static void
garrow_input_stream_default_init (GArrowInputStreamInterface *iface)
{
}

G_END_DECLS

std::shared_ptr<arrow::io::InputStream>
garrow_input_stream_get_raw(GArrowInputStream *input_stream)
{
  auto *iface = GARROW_INPUT_STREAM_GET_IFACE(input_stream);
  return iface->get_raw(input_stream);
}
