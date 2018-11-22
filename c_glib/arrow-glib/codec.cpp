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

#include <arrow-glib/codec.hpp>
#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: codec
 * @title: Codec related type and class
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowCompressionType provides compression types corresponding to
 * `arrow::Compression::type`.
 *
 * #GArrowCodec is a class for compressing and decompressing data.
 */

typedef struct GArrowCodecPrivate_ {
  arrow::util::Codec *codec;
} GArrowCodecPrivate;

enum {
  PROP_CODEC = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCodec, garrow_codec, G_TYPE_OBJECT)

#define GARROW_CODEC_GET_PRIVATE(object)        \
  static_cast<GArrowCodecPrivate *>(            \
    garrow_codec_get_instance_private(          \
      GARROW_CODEC(object)))

static void
garrow_codec_finalize(GObject *object)
{
  auto priv = GARROW_CODEC_GET_PRIVATE(object);

  delete priv->codec;

  G_OBJECT_CLASS(garrow_codec_parent_class)->finalize(object);
}

static void
garrow_codec_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GARROW_CODEC_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CODEC:
    priv->codec = static_cast<arrow::util::Codec *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_codec_get_property(GObject *object,
                          guint prop_id,
                          GValue *value,
                          GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_codec_init(GArrowCodec *object)
{
}

static void
garrow_codec_class_init(GArrowCodecClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_codec_finalize;
  gobject_class->set_property = garrow_codec_set_property;
  gobject_class->get_property = garrow_codec_get_property;

  spec = g_param_spec_pointer("codec",
                              "Codec",
                              "The raw arrow::util::Codec *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CODEC, spec);
}

/**
 * garrow_codec_new:
 * @type: A #GArrowCodompressionType.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GArrowCodec on success, %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowCodec *
garrow_codec_new(GArrowCompressionType type,
                 GError **error)
{
  auto arrow_type = garrow_compression_type_to_raw(type);
  std::unique_ptr<arrow::util::Codec> arrow_codec;
  auto status = arrow::util::Codec::Create(arrow_type, &arrow_codec);
  if (garrow_error_check(error, status, "[codec][new]")) {
    return garrow_codec_new_raw(arrow_codec.release());
  } else {
    return NULL;
  }
}

/**
 * garrow_codec_get_name:
 * @codec: A #GArrowCodec.
 *
 * Returns: The name of the codec.
 *
 * Since: 0.12.0
 */
const gchar *
garrow_codec_get_name(GArrowCodec *codec)
{
  auto arrow_codec = garrow_codec_get_raw(codec);
  return arrow_codec->name();
}

G_END_DECLS

GArrowCompressionType
garrow_compression_type_from_raw(arrow::Compression::type arrow_type)
{
  switch (arrow_type) {
  case arrow::Compression::type::UNCOMPRESSED:
    return GARROW_COMPRESSION_TYPE_UNCOMPRESSED;
  case arrow::Compression::type::SNAPPY:
    return GARROW_COMPRESSION_TYPE_SNAPPY;
  case arrow::Compression::type::GZIP:
    return GARROW_COMPRESSION_TYPE_GZIP;
  case arrow::Compression::type::BROTLI:
    return GARROW_COMPRESSION_TYPE_BROTLI;
  case arrow::Compression::type::ZSTD:
    return GARROW_COMPRESSION_TYPE_ZSTD;
  case arrow::Compression::type::LZ4:
    return GARROW_COMPRESSION_TYPE_LZ4;
  case arrow::Compression::type::LZO:
    return GARROW_COMPRESSION_TYPE_LZO;
  case arrow::Compression::type::BZ2:
    return GARROW_COMPRESSION_TYPE_BZ2;
  default:
    return GARROW_COMPRESSION_TYPE_UNCOMPRESSED;
  }
}

arrow::Compression::type
garrow_compression_type_to_raw(GArrowCompressionType type)
{
  switch (type) {
  case GARROW_COMPRESSION_TYPE_UNCOMPRESSED:
    return arrow::Compression::type::UNCOMPRESSED;
  case GARROW_COMPRESSION_TYPE_SNAPPY:
    return arrow::Compression::type::SNAPPY;
  case GARROW_COMPRESSION_TYPE_GZIP:
    return arrow::Compression::type::GZIP;
  case GARROW_COMPRESSION_TYPE_BROTLI:
    return arrow::Compression::type::BROTLI;
  case GARROW_COMPRESSION_TYPE_ZSTD:
    return arrow::Compression::type::ZSTD;
  case GARROW_COMPRESSION_TYPE_LZ4:
    return arrow::Compression::type::LZ4;
  case GARROW_COMPRESSION_TYPE_LZO:
    return arrow::Compression::type::LZO;
  case GARROW_COMPRESSION_TYPE_BZ2:
    return arrow::Compression::type::BZ2;
  default:
    return arrow::Compression::type::UNCOMPRESSED;
  }
}

GArrowCodec *
garrow_codec_new_raw(arrow::util::Codec *arrow_codec)
{
  auto codec = GARROW_CODEC(g_object_new(GARROW_TYPE_CODEC,
                                         "codec", arrow_codec,
                                         NULL));
  return codec;
}

arrow::util::Codec *
garrow_codec_get_raw(GArrowCodec *codec)
{
  auto priv = GARROW_CODEC_GET_PRIVATE(codec);
  return priv->codec;
}
