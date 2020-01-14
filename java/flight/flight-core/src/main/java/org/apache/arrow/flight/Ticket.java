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
import java.util.Arrays;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.ByteString;

/**
 * Endpoint for a particular stream.
 */
public class Ticket {
  private final byte[] bytes;

  public Ticket(byte[] bytes) {
    super();
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }

  Ticket(org.apache.arrow.flight.impl.Flight.Ticket ticket) {
    this.bytes = ticket.getTicket().toByteArray();
  }

  Flight.Ticket toProtocol() {
    return Flight.Ticket.newBuilder()
        .setTicket(ByteString.copyFrom(bytes))
        .build();
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
   * @param serialized The serialized form of the Ticket, as returned by {@link #serialize()}.
   * @return The deserialized Ticket.
   * @throws IOException if the serialized form is invalid.
   */
  public static Ticket deserialize(ByteBuffer serialized) throws IOException {
    return new Ticket(Flight.Ticket.parseFrom(serialized));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Ticket other = (Ticket) obj;
    if (!Arrays.equals(bytes, other.bytes)) {
      return false;
    }
    return true;
  }


}
