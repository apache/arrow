/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightGetInfo;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

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
    try {
      final ByteBuffer schemaBuf = flightGetInfo.getSchema().asReadOnlyByteBuffer();
      schema = flightGetInfo.getSchema().size() > 0 ?
          MessageSerializer.deserializeSchema(
              new ReadChannel(Channels.newChannel(new ByteBufferBackedInputStream(schemaBuf))))
          : new Schema(ImmutableList.of());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    // Encode schema in a Message payload
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(baos)), schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Flight.FlightGetInfo.newBuilder()
        .addAllEndpoint(endpoints.stream().map(t -> t.toProtocol()).collect(Collectors.toList()))
        .setSchema(ByteString.copyFrom(baos.toByteArray()))
        .setFlightDescriptor(descriptor.toProtocol())
        .setTotalBytes(FlightInfo.this.bytes)
        .setTotalRecords(records)
        .build();

  }
}
