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
#include <arrow-glib/output-stream.hpp>

G_BEGIN_DECLS

/**
 * SECTION: output-stream
 * @title: GArrowOutputStream
 * @short_description: Stream output interface
 *
 * #GArrowOutputStream is an interface for stream output. Stream
 * output is file based and writeable
 */

G_DEFINE_INTERFACE(GArrowOutputStream,
                   garrow_output_stream,
                   G_TYPE_OBJECT)

static void
garrow_output_stream_default_init (GArrowOutputStreamInterface *iface)
{
}

G_END_DECLS

std::shared_ptr<arrow::io::OutputStream>
garrow_output_stream_get_raw(GArrowOutputStream *output_stream)
{
  auto *iface = GARROW_OUTPUT_STREAM_GET_IFACE(output_stream);
  return iface->get_raw(output_stream);
}
