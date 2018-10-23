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
