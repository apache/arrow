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
import java.util.Objects;
import java.util.Optional;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

/**
 * A POJO representation of the execution of a long-running query.
 */
public class PollInfo {
  private final FlightInfo flightInfo;
  private final FlightDescriptor flightDescriptor;
  private final Double progress;
  private final Instant expirationTime;

  /**
   * Create a new PollInfo.
   *
   * @param flightInfo The FlightInfo (must not be null).
   * @param flightDescriptor The descriptor used to poll for more information; null if and only if query is finished.
   * @param progress Optional progress info in [0.0, 1.0].
   * @param expirationTime An expiration time, after which the server may no longer recognize the descriptor.
   */
  public PollInfo(FlightInfo flightInfo, FlightDescriptor flightDescriptor, Double progress, Instant expirationTime) {
    this.flightInfo = Objects.requireNonNull(flightInfo);
    this.flightDescriptor = flightDescriptor;
    this.progress = progress;
    this.expirationTime = expirationTime;
  }

  PollInfo(Flight.PollInfo flt) throws URISyntaxException {
    this.flightInfo = new FlightInfo(flt.getInfo());
    this.flightDescriptor = flt.hasFlightDescriptor() ? new FlightDescriptor(flt.getFlightDescriptor()) : null;
    this.progress = flt.hasProgress() ? flt.getProgress() : null;
    this.expirationTime = flt.hasExpirationTime() ?
        Instant.ofEpochSecond(flt.getExpirationTime().getSeconds(), Timestamps.toNanos(flt.getExpirationTime())) :
        null;
  }

  /**
   * The FlightInfo describing the result set of the execution of a query.
   *
   * <p>This is always present and always contains all endpoints for the query execution so far,
   * not just new endpoints that completed execution since the last call to
   * {@link FlightClient#pollInfo(FlightDescriptor, CallOption...)}.
   */
  public FlightInfo getFlightInfo() {
    return flightInfo;
  }

  /**
   * The FlightDescriptor that should be used to get further updates on this query.
   *
   * <p>It is present if and only if the query is still running.  If present, it should be passed to
   * {@link FlightClient#pollInfo(FlightDescriptor, CallOption...)} to get an update.
   */
  public Optional<FlightDescriptor> getFlightDescriptor() {
    return Optional.ofNullable(flightDescriptor);
  }

  /**
   * The progress of the query.
   *
   * <p>If present, should be a value in [0.0, 1.0]. It is not necessarily monotonic or non-decreasing.
   */
  public Optional<Double> getProgress() {
    return Optional.ofNullable(progress);
  }

  /**
   * The expiration time of the query execution.
   *
   * <p>After this passes, the server may not recognize the descriptor anymore and the client will not
   * be able to track the query anymore.
   */
  public Optional<Instant> getExpirationTime() {
    return Optional.ofNullable(expirationTime);
  }

  Flight.PollInfo toProtocol() {
    Flight.PollInfo.Builder b = Flight.PollInfo.newBuilder();
    b.setInfo(flightInfo.toProtocol());
    if (flightDescriptor != null) {
      b.setFlightDescriptor(flightDescriptor.toProtocol());
    }
    if (progress != null) {
      b.setProgress(progress);
    }
    if (expirationTime != null) {
      b.setExpirationTime(
          Timestamp.newBuilder()
              .setSeconds(expirationTime.getEpochSecond())
              .setNanos(expirationTime.getNano())
              .build());
    }
    return b.build();
  }

  public ByteBuffer serialize() {
    return ByteBuffer.wrap(toProtocol().toByteArray());
  }

  public static PollInfo deserialize(ByteBuffer serialized) throws IOException, URISyntaxException {
    return new PollInfo(Flight.PollInfo.parseFrom(serialized));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PollInfo)) {
      return false;
    }
    PollInfo pollInfo = (PollInfo) o;
    return Objects.equals(getFlightInfo(), pollInfo.getFlightInfo()) &&
        Objects.equals(getFlightDescriptor(), pollInfo.getFlightDescriptor()) &&
        Objects.equals(getProgress(), pollInfo.getProgress()) &&
        Objects.equals(getExpirationTime(), pollInfo.getExpirationTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFlightInfo(), getFlightDescriptor(), getProgress(), getExpirationTime());
  }

  @Override
  public String toString() {
    return "PollInfo{" +
        "flightInfo=" + flightInfo +
        ", flightDescriptor=" + flightDescriptor +
        ", progress=" + progress +
        ", expirationTime=" + expirationTime +
        '}';
  }
}
