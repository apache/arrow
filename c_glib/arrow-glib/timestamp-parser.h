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

#define GARROW_TYPE_TIMESTAMP_PARSER (garrow_timestamp_parser_get_type())
GARROW_AVAILABLE_IN_16_0
G_DECLARE_DERIVABLE_TYPE(
  GArrowTimestampParser, garrow_timestamp_parser, GARROW, TIMESTAMP_PARSER, GObject)
struct _GArrowTimestampParserClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_16_0
const gchar *
garrow_timestamp_parser_get_kind(GArrowTimestampParser *parser);

#define GARROW_TYPE_STRPTIME_TIMESTAMP_PARSER                                            \
  (garrow_strptime_timestamp_parser_get_type())
GARROW_AVAILABLE_IN_16_0
G_DECLARE_DERIVABLE_TYPE(GArrowStrptimeTimestampParser,
                         garrow_strptime_timestamp_parser,
                         GARROW,
                         STRPTIME_TIMESTAMP_PARSER,
                         GArrowTimestampParser)
struct _GArrowStrptimeTimestampParserClass
{
  GArrowTimestampParserClass parent_class;
};

GARROW_AVAILABLE_IN_16_0
GArrowStrptimeTimestampParser *
garrow_strptime_timestamp_parser_new(const gchar *format);

GARROW_AVAILABLE_IN_16_0
const gchar *
garrow_strptime_timestamp_parser_get_format(GArrowStrptimeTimestampParser *parser);

#define GARROW_TYPE_ISO8601_TIMESTAMP_PARSER (garrow_iso8601_timestamp_parser_get_type())
GARROW_AVAILABLE_IN_16_0
G_DECLARE_DERIVABLE_TYPE(GArrowISO8601TimestampParser,
                         garrow_iso8601_timestamp_parser,
                         GARROW,
                         ISO8601_TIMESTAMP_PARSER,
                         GArrowTimestampParser)
struct _GArrowISO8601TimestampParserClass
{
  GArrowTimestampParserClass parent_class;
};

GARROW_AVAILABLE_IN_16_0
GArrowISO8601TimestampParser *
garrow_iso8601_timestamp_parser_new();

G_END_DECLS
