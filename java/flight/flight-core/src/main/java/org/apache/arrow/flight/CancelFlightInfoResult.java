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
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.arrow.flight.impl.Flight;

/**
 * The result of cancelling a FlightInfo.
 */
public class CancelFlightInfoResult {
  private final CancelStatus status;

  public CancelFlightInfoResult(CancelStatus status) {
    this.status = status;
  }

  CancelFlightInfoResult(Flight.CancelFlightInfoResult proto) {
    switch (proto.getStatus()) {
      case CANCEL_STATUS_UNSPECIFIED:
        status = CancelStatus.UNSPECIFIED;
        break;
      case CANCEL_STATUS_CANCELLED:
        status = CancelStatus.CANCELLED;
        break;
      case CANCEL_STATUS_CANCELLING:
        status = CancelStatus.CANCELLING;
        break;
      case CANCEL_STATUS_NOT_CANCELLABLE:
        status = CancelStatus.NOT_CANCELLABLE;
        break;
      default:
        throw new IllegalArgumentException("");
    }
  }

  public CancelStatus getStatus() {
    return status;
  }

  Flight.CancelFlightInfoResult toProtocol() {
    Flight.CancelFlightInfoResult.Builder b = Flight.CancelFlightInfoResult.newBuilder();
    switch (status) {
      case UNSPECIFIED:
        b.setStatus(Flight.CancelStatus.CANCEL_STATUS_UNSPECIFIED);
        break;
      case CANCELLED:
        b.setStatus(Flight.CancelStatus.CANCEL_STATUS_CANCELLED);
        break;
      case CANCELLING:
        b.setStatus(Flight.CancelStatus.CANCEL_STATUS_CANCELLING);
        break;
      case NOT_CANCELLABLE:
        b.setStatus(Flight.CancelStatus.CANCEL_STATUS_NOT_CANCELLABLE);
        break;
      default:
        // Not possible
        throw new AssertionError();
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
   */
  public static CancelFlightInfoResult deserialize(ByteBuffer serialized) throws IOException {
    return new CancelFlightInfoResult(Flight.CancelFlightInfoResult.parseFrom(serialized));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CancelFlightInfoResult)) {
      return false;
    }
    CancelFlightInfoResult that = (CancelFlightInfoResult) o;
    return status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(status);
  }

  @Override
  public String toString() {
    return "CancelFlightInfoResult{" +
        "status=" + status +
        '}';
  }
}
