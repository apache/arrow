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

#pragma once

#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>

G_BEGIN_DECLS

#define GARROW_TYPE_STREAM_LISTENER (garrow_stream_listener_get_type())
GARROW_AVAILABLE_IN_18_0
G_DECLARE_DERIVABLE_TYPE(
  GArrowStreamListener, garrow_stream_listener, GARROW, STREAM_LISTENER, GObject)
struct _GArrowStreamListenerClass
{
  GObjectClass parent_class;

  gboolean (*on_eos)(GArrowStreamListener *listener, GError **error);
  gboolean (*on_record_batch_decoded)(GArrowStreamListener *listener,
                                      GArrowRecordBatch *record_batch,
                                      GHashTable *metadata,
                                      GError **error);
  gboolean (*on_schema_decoded)(GArrowStreamListener *listener,
                                GArrowSchema *schema,
                                GArrowSchema *filtered_schema,
                                GError **error);
};

GARROW_AVAILABLE_IN_18_0
gboolean
garrow_stream_listener_on_eos(GArrowStreamListener *listener, GError **error);

GARROW_AVAILABLE_IN_18_0
gboolean
garrow_stream_listener_on_record_batch_decoded(GArrowStreamListener *listener,
                                               GArrowRecordBatch *record_batch,
                                               GHashTable *metadata,
                                               GError **error);

GARROW_AVAILABLE_IN_18_0
gboolean
garrow_stream_listener_on_schema_decoded(GArrowStreamListener *listener,
                                         GArrowSchema *schema,
                                         GArrowSchema *filtered_schema,
                                         GError **error);

#define GARROW_TYPE_STREAM_DECODER (garrow_stream_decoder_get_type())
GARROW_AVAILABLE_IN_18_0
G_DECLARE_DERIVABLE_TYPE(
  GArrowStreamDecoder, garrow_stream_decoder, GARROW, STREAM_DECODER, GObject)
struct _GArrowStreamDecoderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_18_0
GArrowStreamDecoder *
garrow_stream_decoder_new(GArrowStreamListener *listener, GArrowReadOptions *options);
GARROW_AVAILABLE_IN_18_0
gboolean
garrow_stream_decoder_consume_bytes(GArrowStreamDecoder *decoder,
                                    GBytes *bytes,
                                    GError **error);
GARROW_AVAILABLE_IN_18_0
gboolean
garrow_stream_decoder_consume_buffer(GArrowStreamDecoder *decoder,
                                     GArrowBuffer *buffer,
                                     GError **error);
GARROW_AVAILABLE_IN_18_0
gboolean
garrow_stream_decoder_reset(GArrowStreamDecoder *decoder, GError **error);
GARROW_AVAILABLE_IN_18_0
GArrowSchema *
garrow_stream_decoder_get_schema(GArrowStreamDecoder *decoder);
GARROW_AVAILABLE_IN_18_0
gsize
garrow_stream_decoder_get_next_required_size(GArrowStreamDecoder *decoder);

G_END_DECLS
