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

public class CloseSessionResult {
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
  }

  private final Status status;

  public CloseSessionResult(Status status) {
    this.status = status;
  }

  CloseSessionResult(Flight.CloseSessionResult proto) {
    switch (proto.getStatus()) {
      case UNSPECIFIED:
        status = Status.UNSPECIFIED;
        break;
      case CLOSED:
        status = Status.CLOSED;
        break;
      case CLOSING:
        status = Status.CLOSING;
        break;
      case NOT_CLOSABLE:
        status = Status.NOT_CLOSABLE;
        break;
      default:
        // Unreachable
        throw new IllegalArgumentException("");
    }
  }


  Flight.CloseSessionResult toProtocol() {
    Flight.CloseSessionResult.Builder b = Flight.CloseSessionResult.newBuilder();
    switch (status) {
      case UNSPECIFIED:
        b.setStatus(Flight.CloseSessionResult.Status.UNSPECIFIED);
        break;
      case CLOSED:
        b.setStatus(Flight.CloseSessionResult.Status.CLOSED);
        break;
      case CLOSING:
        b.setStatus(Flight.CloseSessionResult.Status.CLOSING);
        break;
      case NOT_CLOSABLE:
        b.setStatus(Flight.CloseSessionResult.Status.NOT_CLOSABLE);
        break;
      default:
        // Unreachable
        throw new IllegalStateException("");
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
  public static CloseSessionResult deserialize(ByteBuffer serialized) throws IOException {
    return new CloseSessionResult(Flight.CloseSessionResult.parseFrom(serialized));
  }

}
