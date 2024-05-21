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
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.protobuf.ByteString;

/**
 * A POJO representation of a FlightInfo, metadata associated with a set of data records.
 */
public class FlightInfo {
  private final Schema schema;
  private final FlightDescriptor descriptor;
  private final List<FlightEndpoint> endpoints;
  private final long bytes;
  private final long records;
  private final boolean ordered;
  private final IpcOption option;
  private final byte[] appMetadata;

  /**
   * Constructs a new instance.
   *
   * @param schema The schema of the Flight
   * @param descriptor An identifier for the Flight.
   * @param endpoints A list of endpoints that have the flight available.
   * @param bytes The number of bytes in the flight
   * @param records The number of records in the flight.
   */
  public FlightInfo(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints, long bytes,
      long records) {
    this(schema, descriptor, endpoints, bytes, records, /*ordered*/ false, IpcOption.DEFAULT);
  }

  /**
   * Constructs a new instance.
   *
   * @param schema The schema of the Flight
   * @param descriptor An identifier for the Flight.
   * @param endpoints A list of endpoints that have the flight available.
   * @param bytes The number of bytes in the flight
   * @param records The number of records in the flight.
   * @param option IPC write options.
   */
  public FlightInfo(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints, long bytes,
                    long records, IpcOption option) {
    this(schema, descriptor, endpoints, bytes, records, /*ordered*/ false, option);
  }

  /**
   * Constructs a new instance.
   *
   * @param schema The schema of the Flight
   * @param descriptor An identifier for the Flight.
   * @param endpoints A list of endpoints that have the flight available.
   * @param bytes The number of bytes in the flight
   * @param records The number of records in the flight.
   * @param ordered Whether the endpoints in this flight are ordered.
   * @param option IPC write options.
   */
  public FlightInfo(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints, long bytes,
                    long records, boolean ordered, IpcOption option) {
    this(schema, descriptor, endpoints, bytes, records, ordered, option, null);
  }

  /**
   * Constructs a new instance.
   *
   * @param schema The schema of the Flight
   * @param descriptor An identifier for the Flight.
   * @param endpoints A list of endpoints that have the flight available.
   * @param bytes The number of bytes in the flight
   * @param records The number of records in the flight.
   * @param ordered Whether the endpoints in this flight are ordered.
   * @param option IPC write options.
   * @param appMetadata Metadata to send along with the flight
   */
  public FlightInfo(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints, long bytes,
                    long records, boolean ordered, IpcOption option, byte[] appMetadata) {
    Objects.requireNonNull(descriptor);
    Objects.requireNonNull(endpoints);
    if (schema != null) {
      MetadataV4UnionChecker.checkForUnion(schema.getFields().iterator(), option.metadataVersion);
    }
    this.schema = schema;
    this.descriptor = descriptor;
    this.endpoints = endpoints;
    this.bytes = bytes;
    this.records = records;
    this.ordered = ordered;
    this.option = option;
    this.appMetadata = appMetadata;
  }

  /**
   * Constructs from the protocol buffer representation.
   */
  FlightInfo(Flight.FlightInfo pbFlightInfo) throws URISyntaxException {
    try {
      final ByteBuffer schemaBuf = pbFlightInfo.getSchema().asReadOnlyByteBuffer();
      schema = pbFlightInfo.getSchema().size() > 0 ?
          MessageSerializer.deserializeSchema(
                          new ReadChannel(
                                  Channels.newChannel(
                                          new ByteBufferBackedInputStream(schemaBuf))))
          : null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    descriptor = new FlightDescriptor(pbFlightInfo.getFlightDescriptor());
    endpoints = new ArrayList<>();
    for (final Flight.FlightEndpoint endpoint : pbFlightInfo.getEndpointList()) {
      endpoints.add(new FlightEndpoint(endpoint));
    }
    bytes = pbFlightInfo.getTotalBytes();
    records = pbFlightInfo.getTotalRecords();
    ordered = pbFlightInfo.getOrdered();
    appMetadata = (pbFlightInfo.getAppMetadata().size() == 0 ? null : pbFlightInfo.getAppMetadata().toByteArray());
    option = IpcOption.DEFAULT;
  }

  public Optional<Schema> getSchemaOptional() {
    return Optional.ofNullable(schema);
  }

  /**
   * Returns the schema, or an empty schema if no schema is present.
   * @deprecated Deprecated. Use {@link #getSchemaOptional()} instead.
   */
  @Deprecated
  public Schema getSchema() {
    return schema != null ? schema : new Schema(Collections.emptyList());
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

  public boolean getOrdered() {
    return ordered;
  }

  public byte[] getAppMetadata() {
    return appMetadata;
  }

  /**
   * Converts to the protocol buffer representation.
   */
  Flight.FlightInfo toProtocol() {
    Flight.FlightInfo.Builder builder = Flight.FlightInfo.newBuilder()
            .addAllEndpoint(endpoints.stream().map(t -> t.toProtocol()).collect(Collectors.toList()))
            .setFlightDescriptor(descriptor.toProtocol())
            .setTotalBytes(FlightInfo.this.bytes)
            .setTotalRecords(records)
            .setOrdered(ordered);
    if (schema != null) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        MessageSerializer.serialize(
                new WriteChannel(Channels.newChannel(baos)),
                schema,
                option);
        builder.setSchema(ByteString.copyFrom(baos.toByteArray()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (appMetadata != null) {
      builder.setAppMetadata(ByteString.copyFrom(appMetadata));
    }
    return builder.build();
  }

  /**
   * Get the serialized form of this protocol message.
   *
   * <p>Intended to help interoperability by allowing non-Flight services to still return Flight types.
   */
  public ByteBuffer serialize() {
    return ByteBuffer.wrap(toProtocol().toByteArray());
  }

  /**
   * Parse the serialized form of this protocol message.
   *
   * <p>Intended to help interoperability by allowing Flight clients to obtain stream info from non-Flight services.
   *
   * @param serialized The serialized form of the FlightInfo, as returned by {@link #serialize()}.
   * @return The deserialized FlightInfo.
   * @throws IOException if the serialized form is invalid.
   * @throws URISyntaxException if the serialized form contains an unsupported URI format.
   */
  public static FlightInfo deserialize(ByteBuffer serialized) throws IOException, URISyntaxException {
    return new FlightInfo(Flight.FlightInfo.parseFrom(serialized));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FlightInfo)) {
      return false;
    }
    FlightInfo that = (FlightInfo) o;
    return bytes == that.bytes &&
        records == that.records &&
        schema.equals(that.schema) &&
        descriptor.equals(that.descriptor) &&
        endpoints.equals(that.endpoints) &&
        ordered == that.ordered &&
        Arrays.equals(appMetadata, that.appMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, descriptor, endpoints, bytes, records, ordered, Arrays.hashCode(appMetadata));
  }

  @Override
  public String toString() {
    return "FlightInfo{" +
        "schema=" + schema +
        ", descriptor=" + descriptor +
        ", endpoints=" + endpoints +
        ", bytes=" + bytes +
        ", records=" + records +
        ", ordered=" + ordered +
        ", appMetadata=" + (appMetadata == null ? "(none)" : Base64.getEncoder().encodeToString(appMetadata)) +
        '}';
  }

  /**
   * Create a builder for FlightInfo.
   *
   * @param schema     The schema of the Flight
   * @param descriptor An identifier for the Flight.
   * @param endpoints  A list of endpoints that have the flight available.
   */
  public static Builder builder(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints) {
    return new Builder(schema, descriptor, endpoints);
  }

  /**
   * Builder for FlightInfo.
   */
  public static final class Builder {
    private final Schema schema;
    private final FlightDescriptor descriptor;
    private final List<FlightEndpoint> endpoints;
    private long bytes = -1;
    private long records = -1;
    private boolean ordered = false;
    private IpcOption option = IpcOption.DEFAULT;
    private byte[] appMetadata = null;

    private Builder(Schema schema, FlightDescriptor descriptor, List<FlightEndpoint> endpoints) {
      this.schema = schema;
      this.descriptor = descriptor;
      this.endpoints = endpoints;
    }

    /**
     * Set the number of bytes for the flight. Default to -1 for unknown.
     *
     * @param bytes The number of bytes in the flight
     */
    public Builder setBytes(long bytes) {
      this.bytes = bytes;
      return this;
    }

    /**
     * Set the number of records for the flight. Default to -1 for unknown.
     *
     * @param records The number of records in the flight.
     */
    public Builder setRecords(long records) {
      this.records = records;
      return this;
    }

    /**
     * Set whether the flight endpoints are ordered. Default is false.
     *
     * @param ordered Whether the endpoints in this flight are ordered.
     */
    public Builder setOrdered(boolean ordered) {
      this.ordered = ordered;
      return this;
    }

    /**
     * Set IPC write options. Default is IpcOption.DEFAULT
     *
     * @param option IPC write options.
     */
    public Builder setOption(IpcOption option) {
      this.option = option;
      return this;
    }

    /**
     * Set the app metadata to send along with the flight. Default is null.
     *
     * @param appMetadata Metadata to send along with the flight
     */
    public Builder setAppMetadata(byte[] appMetadata) {
      this.appMetadata = appMetadata;
      return this;
    }

    /**
     * Build FlightInfo object.
     */
    public FlightInfo build() {
      return new FlightInfo(schema, descriptor, endpoints, bytes, records, ordered, option, appMetadata);
    }
  }
}
