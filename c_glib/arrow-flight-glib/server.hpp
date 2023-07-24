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

#include <arrow/flight/api.h>

#include <arrow-flight-glib/server.h>


arrow::flight::FlightDataStream *
gaflight_data_stream_get_raw(GAFlightDataStream *stream);

GAFlightServerCallContext *
gaflight_server_call_context_new_raw(
  const arrow::flight::ServerCallContext *flight_call_context);
const arrow::flight::ServerCallContext *
gaflight_server_call_context_get_raw(GAFlightServerCallContext *call_context);

GAFlightServerAuthSender *
gaflight_server_auth_sender_new_raw(
  arrow::flight::ServerAuthSender *flight_sender);
arrow::flight::ServerAuthSender *
gaflight_server_auth_sender_get_raw(GAFlightServerAuthSender *sender);

GAFlightServerAuthReader *
gaflight_server_auth_reader_new_raw(
  arrow::flight::ServerAuthReader *flight_reader);
arrow::flight::ServerAuthReader *
gaflight_server_auth_reader_get_raw(GAFlightServerAuthReader *reader);

std::shared_ptr<arrow::flight::ServerAuthHandler>
gaflight_server_auth_handler_get_raw(GAFlightServerAuthHandler *handler);

arrow::flight::FlightServerOptions *
gaflight_server_options_get_raw(GAFlightServerOptions *options);


struct _GAFlightServableInterface
{
  GTypeInterface parent_iface;

  arrow::flight::FlightServerBase *(*get_raw)(GAFlightServable *servable);
};

arrow::flight::FlightServerBase *
gaflight_servable_get_raw(GAFlightServable *servable);


namespace gaflight {
  class DataStream : public arrow::flight::FlightDataStream {
  public:
    explicit DataStream(GAFlightDataStream *gastream) :
      arrow::flight::FlightDataStream(),
      gastream_(gastream) {
    }

    ~DataStream() override {
      g_object_unref(gastream_);
    }

    std::shared_ptr<arrow::Schema> schema() override {
      auto stream = gaflight_data_stream_get_raw(gastream_);
      return stream->schema();
    }

    arrow::Result<arrow::flight::FlightPayload> GetSchemaPayload() override {
      auto stream = gaflight_data_stream_get_raw(gastream_);
      return stream->GetSchemaPayload();
    }

    arrow::Result<arrow::flight::FlightPayload> Next() override {
      auto stream = gaflight_data_stream_get_raw(gastream_);
      return stream->Next();
    }

  private:
    GAFlightDataStream *gastream_;
  };
};
