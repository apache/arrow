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


#define GAFLIGHT_TYPE_SERVER_AUTH_SENDER        \
  (gaflight_server_auth_sender_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerAuthSender,
                         gaflight_server_auth_sender,
                         GAFLIGHT,
                         SERVER_AUTH_SENDER,
                         GObject)
struct _GAFlightServerAuthSenderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
gboolean
gaflight_server_auth_sender_write(GAFlightServerAuthSender *sender,
                                  GBytes *message,
                                  GError **error);


#define GAFLIGHT_TYPE_SERVER_AUTH_READER        \
  (gaflight_server_auth_reader_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerAuthReader,
                         gaflight_server_auth_reader,
                         GAFLIGHT,
                         SERVER_AUTH_READER,
                         GObject)
struct _GAFlightServerAuthReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
GBytes *
gaflight_server_auth_reader_read(GAFlightServerAuthReader *reader,
                                 GError **error);


#define GAFLIGHT_TYPE_SERVER_AUTH_HANDLER       \
  (gaflight_server_auth_handler_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerAuthHandler,
                         gaflight_server_auth_handler,
                         GAFLIGHT,
                         SERVER_AUTH_HANDLER,
                         GObject)
struct _GAFlightServerAuthHandlerClass
{
  GObjectClass parent_class;
};

#define GAFLIGHT_TYPE_SERVER_CUSTOM_AUTH_HANDLER       \
  (gaflight_server_custom_auth_handler_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerCustomAuthHandler,
                         gaflight_server_custom_auth_handler,
                         GAFLIGHT,
                         SERVER_CUSTOM_AUTH_HANDLER,
                         GAFlightServerAuthHandler)
/**
 * GAFlightServerCustomAuthHandlerClass:
 * @authenticate: Authenticates the client on initial connection. The server
 *   can send and read responses from the client at any time.
 * @is_valid: Validates a per-call client token.
 *
 * Since: 12.0.0
 */
struct _GAFlightServerCustomAuthHandlerClass
{
  GAFlightServerAuthHandlerClass parent_class;

  void (*authenticate)(GAFlightServerCustomAuthHandler *handler,
                       GAFlightServerAuthSender *sender,
                       GAFlightServerAuthReader *reader,
                       GError **error);
  void (*is_valid)(GAFlightServerCustomAuthHandler *handler,
                   GBytes *token,
                   GBytes **peer_identity,
                   GError **error);
};

GARROW_AVAILABLE_IN_12_0
void
gaflight_server_custom_auth_handler_authenticate(
  GAFlightServerCustomAuthHandler *handler,
  GAFlightServerAuthSender *sender,
  GAFlightServerAuthReader *reader,
  GError **error);

GARROW_AVAILABLE_IN_12_0
void
gaflight_server_custom_auth_handler_is_valid(
  GAFlightServerCustomAuthHandler *handler,
  GBytes *token,
  GBytes **peer_identity,
  GError **error);


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


#define GAFLIGHT_TYPE_SERVABLE (gaflight_servable_get_type())
G_DECLARE_INTERFACE(GAFlightServable,
                    gaflight_servable,
                    GAFLIGHT,
                    SERVABLE,
                    GObject)


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
  GAFlightInfo *(*get_flight_info)(GAFlightServer *server,
                                   GAFlightServerCallContext *context,
                                   GAFlightDescriptor *request,
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
GARROW_AVAILABLE_IN_9_0
GAFlightInfo *
gaflight_server_get_flight_info(GAFlightServer *server,
                                GAFlightServerCallContext *context,
                                GAFlightDescriptor *request,
                                GError **error);
GARROW_AVAILABLE_IN_6_0
GAFlightDataStream *
gaflight_server_do_get(GAFlightServer *server,
                       GAFlightServerCallContext *context,
                       GAFlightTicket *ticket,
                       GError **error);

G_END_DECLS
