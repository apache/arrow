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

#include <arrow-glib/timestamp-parser.hpp>

G_BEGIN_DECLS

/**
 * SECTION: timestamp-parser
 * @section_id: timestamp-parser-classes
 * @title: TimestamParser classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowTimestampParser is a base class for parsing timestamp text.
 *
 * #GArrowStrptimeTimestampParser is a class for parsing timestamp
 * text used by the given stprtime(3) format.
 *
 * #GArrowISO8601TimestampParser is a class for parsing ISO 8601
 * format timestamp text.
 */

struct GArrowTimestampParserPrivate
{
  std::shared_ptr<arrow::TimestampParser> parser;
};

enum {
  PROP_PARSER = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowTimestampParser,
                                    garrow_timestamp_parser,
                                    G_TYPE_OBJECT);

#define GARROW_TIMESTAMP_PARSER_GET_PRIVATE(obj)                                         \
  static_cast<GArrowTimestampParserPrivate *>(                                           \
    garrow_timestamp_parser_get_instance_private(GARROW_TIMESTAMP_PARSER(obj)))

static void
garrow_timestamp_parser_finalize(GObject *object)
{
  auto priv = GARROW_TIMESTAMP_PARSER_GET_PRIVATE(object);

  priv->parser.~shared_ptr();

  G_OBJECT_CLASS(garrow_timestamp_parser_parent_class)->finalize(object);
}

static void
garrow_timestamp_parser_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_TIMESTAMP_PARSER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_PARSER:
    priv->parser =
      *static_cast<std::shared_ptr<arrow::TimestampParser> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_timestamp_parser_init(GArrowTimestampParser *object)
{
  auto priv = GARROW_TIMESTAMP_PARSER_GET_PRIVATE(object);
  new (&priv->parser) std::shared_ptr<arrow::TimestampParser>;
}

static void
garrow_timestamp_parser_class_init(GArrowTimestampParserClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = garrow_timestamp_parser_finalize;
  gobject_class->set_property = garrow_timestamp_parser_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "parser",
    "Parser",
    "The raw std::shared<arrow::TimestampParser> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PARSER, spec);
}

/**
 * garrow_timestamp_parser_get_kind:
 * @parser: A #GArrowTimestampParser.
 *
 * Returns: The kind of this timestamp parser.
 *
 * Since: 16.0.0
 */
const gchar *
garrow_timestamp_parser_get_kind(GArrowTimestampParser *parser)
{
  auto arrow_parser = garrow_timestamp_parser_get_raw(parser);
  return arrow_parser->kind();
}

G_DEFINE_TYPE(GArrowStrptimeTimestampParser,
              garrow_strptime_timestamp_parser,
              GARROW_TYPE_TIMESTAMP_PARSER);

static void
garrow_strptime_timestamp_parser_init(GArrowStrptimeTimestampParser *object)
{
}

static void
garrow_strptime_timestamp_parser_class_init(GArrowStrptimeTimestampParserClass *klass)
{
}

/**
 * garrow_strptime_timestamp_parser_new:
 * @format: A format used by strptime(3).
 *
 * Returns: (transfer full): A newly allocated #GArrowStrptimeTimestampParser.
 *
 * Since: 16.0.0
 */
GArrowStrptimeTimestampParser *
garrow_strptime_timestamp_parser_new(const gchar *format)
{
  auto arrow_parser = arrow::TimestampParser::MakeStrptime(format);
  return GARROW_STRPTIME_TIMESTAMP_PARSER(
    g_object_new(GARROW_TYPE_STRPTIME_TIMESTAMP_PARSER,
                 "parser",
                 &arrow_parser,
                 nullptr));
}

/**
 * garrow_strptime_timestamp_parser_get_format:
 * @parser: A #GArrowStrptimeTimestampParser.
 *
 * Returns: The format used by this parser.
 *
 * Since: 16.0.0
 */
const gchar *
garrow_strptime_timestamp_parser_get_format(GArrowStrptimeTimestampParser *parser)
{
  auto arrow_parser = garrow_timestamp_parser_get_raw(GARROW_TIMESTAMP_PARSER(parser));
  return arrow_parser->format();
}

G_DEFINE_TYPE(GArrowISO8601TimestampParser,
              garrow_iso8601_timestamp_parser,
              GARROW_TYPE_TIMESTAMP_PARSER);

static void
garrow_iso8601_timestamp_parser_init(GArrowISO8601TimestampParser *object)
{
}

static void
garrow_iso8601_timestamp_parser_class_init(GArrowISO8601TimestampParserClass *klass)
{
}

/**
 * garrow_iso8601_timestamp_parser_new:
 *
 * Returns: (transfer full): A newly allocated #GArrowISO8601TimestampParser.
 *
 * Since: 16.0.0
 */
GArrowISO8601TimestampParser *
garrow_iso8601_timestamp_parser_new(void)
{
  auto arrow_parser = arrow::TimestampParser::MakeISO8601();
  return GARROW_ISO8601_TIMESTAMP_PARSER(
    g_object_new(GARROW_TYPE_ISO8601_TIMESTAMP_PARSER, "parser", &arrow_parser, nullptr));
}

G_END_DECLS

std::shared_ptr<arrow::TimestampParser>
garrow_timestamp_parser_get_raw(GArrowTimestampParser *parser)
{
  auto priv = GARROW_TIMESTAMP_PARSER_GET_PRIVATE(parser);
  return priv->parser;
}
