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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS


#define GAFLIGHT_TYPE_CRITERIA (gaflight_criteria_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightCriteria,
                         gaflight_criteria,
                         GAFLIGHT,
                         CRITERIA,
                         GObject)
struct _GAFlightCriteriaClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightCriteria *
gaflight_criteria_new(GBytes *expression);


#define GAFLIGHT_TYPE_LOCATION (gaflight_location_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightLocation,
                         gaflight_location,
                         GAFLIGHT,
                         LOCATION,
                         GObject)
struct _GAFlightLocationClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightLocation *
gaflight_location_new(const gchar *uri,
                      GError **error);

GARROW_AVAILABLE_IN_5_0
gchar *
gaflight_location_to_string(GAFlightLocation *location);

GARROW_AVAILABLE_IN_5_0
gchar *
gaflight_location_get_scheme(GAFlightLocation *location);

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_location_equal(GAFlightLocation *location,
                        GAFlightLocation *other_location);


#define GAFLIGHT_TYPE_DESCRIPTOR (gaflight_descriptor_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightDescriptor,
                         gaflight_descriptor,
                         GAFLIGHT,
                         DESCRIPTOR,
                         GObject)
struct _GAFlightDescriptorClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
gchar *
gaflight_descriptor_to_string(GAFlightDescriptor *descriptor);

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_descriptor_equal(GAFlightDescriptor *descriptor,
                          GAFlightDescriptor *other_descriptor);


#define GAFLIGHT_TYPE_PATH_DESCRIPTOR (gaflight_path_descriptor_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightPathDescriptor,
                         gaflight_path_descriptor,
                         GAFLIGHT,
                         PATH_DESCRIPTOR,
                         GAFlightDescriptor)
struct _GAFlightPathDescriptorClass
{
  GAFlightDescriptorClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightPathDescriptor *
gaflight_path_descriptor_new(const gchar **paths,
                             gsize n_paths);

GARROW_AVAILABLE_IN_5_0
gchar **
gaflight_path_descriptor_get_paths(GAFlightPathDescriptor *descriptor);


#define GAFLIGHT_TYPE_COMMAND_DESCRIPTOR (gaflight_command_descriptor_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightCommandDescriptor,
                         gaflight_command_descriptor,
                         GAFLIGHT,
                         COMMAND_DESCRIPTOR,
                         GAFlightDescriptor)
struct _GAFlightCommandDescriptorClass
{
  GAFlightDescriptorClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightCommandDescriptor *
gaflight_command_descriptor_new(const gchar *command);

GARROW_AVAILABLE_IN_5_0
gchar *
gaflight_command_descriptor_get_command(GAFlightCommandDescriptor *descriptor);


#define GAFLIGHT_TYPE_TICKET (gaflight_ticket_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightTicket,
                         gaflight_ticket,
                         GAFLIGHT,
                         TICKET,
                         GObject)
struct _GAFlightTicketClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightTicket *
gaflight_ticket_new(GBytes *data);

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_ticket_equal(GAFlightTicket *ticket,
                      GAFlightTicket *other_ticket);


#define GAFLIGHT_TYPE_ENDPOINT (gaflight_endpoint_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightEndpoint,
                         gaflight_endpoint,
                         GAFLIGHT,
                         ENDPOINT,
                         GObject)
struct _GAFlightEndpointClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightEndpoint *
gaflight_endpoint_new(GAFlightTicket *ticket,
                      GList *locations);

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_endpoint_equal(GAFlightEndpoint *endpoint,
                        GAFlightEndpoint *other_endpoint);

GARROW_AVAILABLE_IN_5_0
GList *
gaflight_endpoint_get_locations(GAFlightEndpoint *endpoint);


#define GAFLIGHT_TYPE_INFO (gaflight_info_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightInfo,
                         gaflight_info,
                         GAFLIGHT,
                         INFO,
                         GObject)
struct _GAFlightInfoClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightInfo *
gaflight_info_new(GArrowSchema *schema,
                  GAFlightDescriptor *descriptor,
                  GList *endpoints,
                  gint64 total_records,
                  gint64 total_bytes,
                  GError **error);

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_info_equal(GAFlightInfo *info,
                    GAFlightInfo *other_info);

GARROW_AVAILABLE_IN_5_0
GArrowSchema *
gaflight_info_get_schema(GAFlightInfo *info,
                         GArrowReadOptions *options,
                         GError **error);
GARROW_AVAILABLE_IN_5_0
GAFlightDescriptor *
gaflight_info_get_descriptor(GAFlightInfo *info);
GARROW_AVAILABLE_IN_5_0
GList *
gaflight_info_get_endpoints(GAFlightInfo *info);
GARROW_AVAILABLE_IN_5_0
gint64
gaflight_info_get_total_records(GAFlightInfo *info);
GARROW_AVAILABLE_IN_5_0
gint64
gaflight_info_get_total_bytes(GAFlightInfo *info);


#define GAFLIGHT_TYPE_STREAM_CHUNK (gaflight_stream_chunk_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightStreamChunk,
                         gaflight_stream_chunk,
                         GAFLIGHT,
                         STREAM_CHUNK,
                         GObject)
struct _GAFlightStreamChunkClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowRecordBatch *
gaflight_stream_chunk_get_data(GAFlightStreamChunk *chunk);
GARROW_AVAILABLE_IN_6_0
GArrowBuffer *
gaflight_stream_chunk_get_metadata(GAFlightStreamChunk *chunk);


#define GAFLIGHT_TYPE_RECORD_BATCH_READER       \
  (gaflight_record_batch_reader_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightRecordBatchReader,
                         gaflight_record_batch_reader,
                         GAFLIGHT,
                         RECORD_BATCH_READER,
                         GObject)
struct _GAFlightRecordBatchReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GAFlightStreamChunk *
gaflight_record_batch_reader_read_next(GAFlightRecordBatchReader *reader,
                                       GError **error);

GARROW_AVAILABLE_IN_6_0
GArrowTable *
gaflight_record_batch_reader_read_all(GAFlightRecordBatchReader *reader,
                                      GError **error);


G_END_DECLS
