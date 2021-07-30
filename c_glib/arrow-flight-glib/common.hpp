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

#include <arrow-flight-glib/common.h>


GAFlightCriteria *
gaflight_criteria_new_raw(const arrow::flight::Criteria *flight_criteria);
arrow::flight::Criteria *
gaflight_criteria_get_raw(GAFlightCriteria *criteria);

arrow::flight::Location *
gaflight_location_get_raw(GAFlightLocation *location);

GAFlightDescriptor *
gaflight_descriptor_new_raw(
  const arrow::flight::FlightDescriptor *flight_descriptor);
arrow::flight::FlightDescriptor *
gaflight_descriptor_get_raw(GAFlightDescriptor *descriptor);

GAFlightTicket *
gaflight_ticket_new_raw(const arrow::flight::Ticket *flight_ticket);
arrow::flight::Ticket *
gaflight_ticket_get_raw(GAFlightTicket *ticket);

GAFlightEndpoint *
gaflight_endpoint_new_raw(const arrow::flight::FlightEndpoint *flight_endpoint,
                          GAFlightTicket *ticket);
arrow::flight::FlightEndpoint *
gaflight_endpoint_get_raw(GAFlightEndpoint *endpoint);

GAFlightInfo *
gaflight_info_new_raw(arrow::flight::FlightInfo *flight_info);
arrow::flight::FlightInfo *
gaflight_info_get_raw(GAFlightInfo *info);

GAFlightStreamChunk *
gaflight_stream_chunk_new_raw(arrow::flight::FlightStreamChunk *flight_chunk);
arrow::flight::FlightStreamChunk *
gaflight_stream_chunk_get_raw(GAFlightStreamChunk *chunk);

arrow::flight::MetadataRecordBatchReader *
gaflight_record_batch_reader_get_raw(GAFlightRecordBatchReader *reader);
