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


#define GAFLIGHT_TYPE_DATA_STREAM       \
  (gaflight_data_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightDataStream,
                         gaflight_data_stream,
                         GAFLIGHT,
                         DATA_STREAM,
                         GObject)
struct _GAFlightDataStreamClass
{
  GObjectClass parent_class;
};


#define GAFLIGHT_TYPE_RECORD_BATCH_STREAM       \
  (gaflight_record_batch_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightRecordBatchStream,
                         gaflight_record_batch_stream,
                         GAFLIGHT,
                         RECORD_BATCH_STREAM,
                         GAFlightDataStream)
struct _GAFlightRecordBatchStreamClass
{
  GAFlightDataStreamClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GAFlightRecordBatchStream *
gaflight_record_batch_stream_new(GArrowRecordBatchReader *reader,
                                 GArrowWriteOptions *options);


#define GAFLIGHT_TYPE_SERVER_OPTIONS (gaflight_server_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerOptions,
                         gaflight_server_options,
                         GAFLIGHT,
                         SERVER_OPTIONS,
                         GObject)
struct _GAFlightServerOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightServerOptions *
gaflight_server_options_new(GAFlightLocation *location);


#define GAFLIGHT_TYPE_SERVER_CALL_CONTEXT       \
  (gaflight_server_call_context_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerCallContext,
                         gaflight_server_call_context,
                         GAFLIGHT,
                         SERVER_CALL_CONTEXT,
                         GObject)
struct _GAFlightServerCallContextClass
{
  GObjectClass parent_class;
};


#define GAFLIGHT_TYPE_SERVER (gaflight_server_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServer,
                         gaflight_server,
                         GAFLIGHT,
                         SERVER,
                         GObject)
/**
 * GAFlightServerClass:
 * @list_flights: A virtual function to implement `ListFlights` API.
 * @do_get: A virtual function to implement `DoGet` API.
 *
 * Since: 5.0.0
 */
struct _GAFlightServerClass
{
  GObjectClass parent_class;

  GList *(*list_flights)(GAFlightServer *server,
                         GAFlightServerCallContext *context,
                         GAFlightCriteria *criteria,
                         GError **error);
  GAFlightDataStream *(*do_get)(GAFlightServer *server,
                                GAFlightServerCallContext *context,
                                GAFlightTicket *ticket,
                                GError **error);
};

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_server_listen(GAFlightServer *server,
                       GAFlightServerOptions *options,
                       GError **error);
GARROW_AVAILABLE_IN_5_0
gint
gaflight_server_get_port(GAFlightServer *server);
GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_server_shutdown(GAFlightServer *server,
                         GError **error);
GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_server_wait(GAFlightServer *server,
                     GError **error);

GARROW_AVAILABLE_IN_5_0
GList *
gaflight_server_list_flights(GAFlightServer *server,
                             GAFlightServerCallContext *context,
                             GAFlightCriteria *criteria,
                             GError **error);
GARROW_AVAILABLE_IN_6_0
GAFlightDataStream *
gaflight_server_do_get(GAFlightServer *server,
                       GAFlightServerCallContext *context,
                       GAFlightTicket *ticket,
                       GError **error);

G_END_DECLS
