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

#include <glib-object.h>

#include <arrow-glib/version.h>

G_BEGIN_DECLS

/**
 * GArrowCompressionType:
 * @GARROW_COMPRESSION_TYPE_UNCOMPRESSED: Not compressed.
 * @GARROW_COMPRESSION_TYPE_SNAPPY: Snappy compression.
 * @GARROW_COMPRESSION_TYPE_GZIP: gzip compression.
 * @GARROW_COMPRESSION_TYPE_BROTLI: Brotli compression.
 * @GARROW_COMPRESSION_TYPE_ZSTD: Zstandard compression.
 * @GARROW_COMPRESSION_TYPE_LZ4: LZ4 compression.
 * @GARROW_COMPRESSION_TYPE_LZO: LZO compression.
 * @GARROW_COMPRESSION_TYPE_BZ2: bzip2 compression.
 *
 * They are corresponding to `arrow::Compression::type` values.
 */
typedef enum {
  GARROW_COMPRESSION_TYPE_UNCOMPRESSED,
  GARROW_COMPRESSION_TYPE_SNAPPY,
  GARROW_COMPRESSION_TYPE_GZIP,
  GARROW_COMPRESSION_TYPE_BROTLI,
  GARROW_COMPRESSION_TYPE_ZSTD,
  GARROW_COMPRESSION_TYPE_LZ4,
  GARROW_COMPRESSION_TYPE_LZO,
  GARROW_COMPRESSION_TYPE_BZ2
} GArrowCompressionType;

#define GARROW_TYPE_CODEC (garrow_codec_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowCodec, garrow_codec, GARROW, CODEC, GObject)
struct _GArrowCodecClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowCodec *
garrow_codec_new(GArrowCompressionType type, GError **error);

GARROW_AVAILABLE_IN_ALL
const gchar *
garrow_codec_get_name(GArrowCodec *codec);

GARROW_AVAILABLE_IN_2_0
GArrowCompressionType
garrow_codec_get_compression_type(GArrowCodec *codec);

GARROW_AVAILABLE_IN_2_0
gint
garrow_codec_get_compression_level(GArrowCodec *codec);

G_END_DECLS
