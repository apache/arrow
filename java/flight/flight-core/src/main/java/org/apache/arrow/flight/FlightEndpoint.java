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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

/**
 * POJO to convert to/from the underlying protobuf FlightEndpoint.
 */
public class FlightEndpoint {
  private final List<Location> locations;
  private final Ticket ticket;
  private final Instant expirationTime;
  private final byte[] appMetadata;

  /**
   * Constructs a new endpoint with no expiration time.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public FlightEndpoint(Ticket ticket, Location... locations) {
    this(ticket, /*expirationTime*/null, locations);
  }

  /**
   * Constructs a new endpoint with an expiration time.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param expirationTime (optional) When this endpoint expires.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public FlightEndpoint(Ticket ticket, Instant expirationTime, Location... locations) {
    this(ticket, expirationTime, null, Collections.unmodifiableList(new ArrayList<>(Arrays.asList(locations))));
  }

  /**
   * Private constructor with all parameters. Should only be called by Builder.
   */
  private FlightEndpoint(Ticket ticket, Instant expirationTime, byte[] appMetadata, List<Location> locations) {
    Objects.requireNonNull(ticket);
    this.locations = locations;
    this.expirationTime = expirationTime;
    this.ticket = ticket;
    this.appMetadata = appMetadata;
  }

  /**
   * Constructs from the protocol buffer representation.
   */
  FlightEndpoint(Flight.FlightEndpoint flt) throws URISyntaxException {
    this.locations = new ArrayList<>();
    for (final Flight.Location location : flt.getLocationList()) {
      this.locations.add(new Location(location.getUri()));
    }
    if (flt.hasExpirationTime()) {
      this.expirationTime = Instant.ofEpochSecond(
          flt.getExpirationTime().getSeconds(), Timestamps.toNanos(flt.getExpirationTime()));
    } else {
      this.expirationTime = null;
    }
    this.appMetadata = (flt.getAppMetadata().isEmpty() ? null : flt.getAppMetadata().toByteArray());
    this.ticket = new Ticket(flt.getTicket());
  }

  public List<Location> getLocations() {
    return locations;
  }

  public Ticket getTicket() {
    return ticket;
  }

  public Optional<Instant> getExpirationTime() {
    return Optional.ofNullable(expirationTime);
  }

  public byte[] getAppMetadata() {
    return appMetadata;
  }

  /**
   * Converts to the protocol buffer representation.
   */
  Flight.FlightEndpoint toProtocol() {
    Flight.FlightEndpoint.Builder b = Flight.FlightEndpoint.newBuilder()
        .setTicket(ticket.toProtocol());

    for (Location l : locations) {
      b.addLocation(l.toProtocol());
    }

    if (expirationTime != null) {
      b.setExpirationTime(
          Timestamp.newBuilder()
              .setSeconds(expirationTime.getEpochSecond())
              .setNanos(expirationTime.getNano())
              .build());
    }

    if (appMetadata != null) {
      b.setAppMetadata(ByteString.copyFrom(appMetadata));
    }

    return b.build();
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
   * @param serialized The serialized form of the message, as returned by {@link #serialize()}.
   * @return The deserialized message.
   * @throws IOException if the serialized form is invalid.
   * @throws URISyntaxException if the serialized form contains an unsupported URI format.
   */
  public static FlightEndpoint deserialize(ByteBuffer serialized) throws IOException, URISyntaxException {
    return new FlightEndpoint(Flight.FlightEndpoint.parseFrom(serialized));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FlightEndpoint)) {
      return false;
    }
    FlightEndpoint that = (FlightEndpoint) o;
    return locations.equals(that.locations) &&
        ticket.equals(that.ticket) &&
        Objects.equals(expirationTime, that.expirationTime) &&
        Arrays.equals(appMetadata, that.appMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(locations, ticket, expirationTime, Arrays.hashCode(appMetadata));
  }

  @Override
  public String toString() {
    return "FlightEndpoint{" +
        "locations=" + locations +
        ", ticket=" + ticket +
        ", expirationTime=" + (expirationTime == null ? "(none)" : expirationTime.toString()) +
        ", appMetadata=" + (appMetadata == null ? "(none)" : Base64.getEncoder().encodeToString(appMetadata)) +
        '}';
  }

  /**
   * Create a builder for FlightEndpoint.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public static Builder builder(Ticket ticket, Location... locations) {
    return new Builder(ticket, locations);
  }

  /**
   * Builder for FlightEndpoint.
   */
  public static final class Builder {
    private final Ticket ticket;
    private final List<Location> locations;
    private Instant expirationTime = null;
    private byte[] appMetadata = null;

    private Builder(Ticket ticket, Location... locations) {
      this.ticket = ticket;
      this.locations = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(locations)));
    }

    /**
     * Set expiration time for the endpoint. Default is null, which means don't expire.
     *
     * @param expirationTime (optional) When this endpoint expires.
     */
    public Builder setExpirationTime(Instant expirationTime) {
      this.expirationTime = expirationTime;
      return this;
    }

    /**
     * Set the app metadata to send along with the flight. Default is null;
     *
     * @param appMetadata Metadata to send along with the flight
     */
    public Builder setAppMetadata(byte[] appMetadata) {
      this.appMetadata = appMetadata;
      return this;
    }

    /**
     * Build FlightEndpoint object.
     */
    public FlightEndpoint build() {
      return new FlightEndpoint(ticket, expirationTime, appMetadata, locations);
    }
  }
}
