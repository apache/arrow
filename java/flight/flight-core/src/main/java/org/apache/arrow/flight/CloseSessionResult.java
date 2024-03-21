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

import org.apache.arrow.flight.impl.Flight;

/** The result of attempting to close/invalidate a server session context. */
public class CloseSessionResult {
  /**
   * Close operation result status values.
   */
  public enum Status {
    /**
     * The session close status is unknown. Servers should avoid using this value
     * (send a NOT_FOUND error if the requested session is not known). Clients can
     * retry the request.
     */
    UNSPECIFIED,
    /**
     * The session close request is complete.
     */
    CLOSED,
    /**
     * The session close request is in progress. The client may retry the request.
     */
    CLOSING,
    /**
     * The session is not closeable.
     */
    NOT_CLOSABLE,
    ;

    public static Status fromProtocol(Flight.CloseSessionResult.Status proto) {
      return values()[proto.getNumber()];
    }

    public Flight.CloseSessionResult.Status toProtocol() {
      return Flight.CloseSessionResult.Status.values()[ordinal()];
    }
  }

  private final Status status;

  public CloseSessionResult(Status status) {
    this.status = status;
  }

  CloseSessionResult(Flight.CloseSessionResult proto) {
    status = Status.fromProtocol(proto.getStatus());
    if (status == null) {
      // Unreachable
      throw new IllegalArgumentException("");
    }
  }

  public Status getStatus() {
    return status;
  }

  Flight.CloseSessionResult toProtocol() {
    Flight.CloseSessionResult.Builder b = Flight.CloseSessionResult.newBuilder();
    b.setStatus(status.toProtocol());
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
  public static CloseSessionResult deserialize(ByteBuffer serialized) throws IOException {
    return new CloseSessionResult(Flight.CloseSessionResult.parseFrom(serialized));
  }

}
