/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightGetInfo;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

public class FlightInfo {
  private Schema schema;
  private FlightDescriptor descriptor;
  private List<FlightEndpoint> endpoints;
  private final long bytes;
  private final long records;

  public FlightInfo(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints, long bytes,
      long records) {
    super();
    this.schema = schema;
    this.descriptor = descriptor;
    this.endpoints = endpoints;
    this.bytes = bytes;
    this.records = records;
  }

  FlightInfo(FlightGetInfo flightGetInfo) {
    schema = flightGetInfo.getSchema().size() > 0 ?
        Schema.deserialize(flightGetInfo.getSchema().asReadOnlyByteBuffer()) : new Schema(ImmutableList.of());
    descriptor = new FlightDescriptor(flightGetInfo.getFlightDescriptor());
    endpoints = flightGetInfo.getEndpointList().stream().map(t -> new FlightEndpoint(t)).collect(Collectors.toList());
    bytes = flightGetInfo.getTotalBytes();
    records = flightGetInfo.getTotalRecords();
  }

  public Schema getSchema() {
    return schema;
  }

  public long getBytes() {
    return bytes;
  }

  public long getRecords() {
    return records;
  }

  public FlightDescriptor getDescriptor() {
    return descriptor;
  }

  public List<FlightEndpoint> getEndpoints() {
    return endpoints;
  }

  FlightGetInfo toProtocol() {
    return Flight.FlightGetInfo.newBuilder()
        .addAllEndpoint(endpoints.stream().map(t -> t.toProtocol()).collect(Collectors.toList()))
        .setSchema(ByteString.copyFrom(schema.toByteArray()))
        .setFlightDescriptor(descriptor.toProtocol())
        .setTotalBytes(FlightInfo.this.bytes)
        .setTotalRecords(records)
        .build();

  }
}
