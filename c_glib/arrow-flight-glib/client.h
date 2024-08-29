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

#include <arrow-flight-glib/common.h>

G_BEGIN_DECLS


#define GAFLIGHT_TYPE_STREAM_READER       \
  (gaflight_stream_reader_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightStreamReader,
                         gaflight_stream_reader,
                         GAFLIGHT,
                         STREAM_READER,
                         GAFlightRecordBatchReader)
struct _GAFlightStreamReaderClass
{
  GAFlightRecordBatchReaderClass parent_class;
};


#define GAFLIGHT_TYPE_CALL_OPTIONS (gaflight_call_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightCallOptions,
                         gaflight_call_options,
                         GAFLIGHT,
                         CALL_OPTIONS,
                         GObject)
struct _GAFlightCallOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightCallOptions *
gaflight_call_options_new(void);

GARROW_AVAILABLE_IN_9_0
void
gaflight_call_options_add_header(GAFlightCallOptions *options,
                                 const gchar *name,
                                 const gchar *value);
GARROW_AVAILABLE_IN_9_0
void
gaflight_call_options_clear_headers(GAFlightCallOptions *options);
GARROW_AVAILABLE_IN_9_0
void
gaflight_call_options_foreach_header(GAFlightCallOptions *options,
                                     GAFlightHeaderFunc func,
                                     gpointer user_data);


#define GAFLIGHT_TYPE_CLIENT_OPTIONS (gaflight_client_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightClientOptions,
                         gaflight_client_options,
                         GAFLIGHT,
                         CLIENT_OPTIONS,
                         GObject)
struct _GAFlightClientOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightClientOptions *
gaflight_client_options_new(void);


#define GAFLIGHT_TYPE_CLIENT (gaflight_client_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightClient,
                         gaflight_client,
                         GAFLIGHT,
                         CLIENT,
                         GObject)
struct _GAFlightClientClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightClient *
gaflight_client_new(GAFlightLocation *location,
                    GAFlightClientOptions *options,
                    GError **error);

GARROW_AVAILABLE_IN_8_0
gboolean
gaflight_client_close(GAFlightClient *client,
                      GError **error);

GARROW_AVAILABLE_IN_12_0
gboolean
gaflight_client_authenticate_basic_token(GAFlightClient *client,
                                         const gchar *user,
                                         const gchar *password,
                                         GAFlightCallOptions *options,
                                         gchar **bearer_name,
                                         gchar **bearer_value,
                                         GError **error);

GARROW_AVAILABLE_IN_5_0
GList *
gaflight_client_list_flights(GAFlightClient *client,
                             GAFlightCriteria *criteria,
                             GAFlightCallOptions *options,
                             GError **error);

GARROW_AVAILABLE_IN_9_0
GAFlightInfo *
gaflight_client_get_flight_info(GAFlightClient *client,
                                GAFlightDescriptor *descriptor,
                                GAFlightCallOptions *options,
                                GError **error);

GARROW_AVAILABLE_IN_6_0
GAFlightStreamReader *
gaflight_client_do_get(GAFlightClient *client,
                       GAFlightTicket *ticket,
                       GAFlightCallOptions *options,
                       GError **error);


G_END_DECLS
