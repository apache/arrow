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
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class SetSessionOptionsResult {
  public enum Status {
    /**
     * The status of setting the option is unknown. Servers should avoid using this value
     * (send a NOT_FOUND error if the requested session is not known). Clients can retry
     * the request.
      */
    UNSPECIFIED(Flight.SetSessionOptionsResult.Status.UNSPECIFIED),
    /**
     * The session option setting completed successfully.
     */
    OK(Flight.SetSessionOptionsResult.Status.OK),
    /**
     * The given session option name was an alias for another option name.
     */
    OK_MAPPED(Flight.SetSessionOptionsResult.Status.OK_MAPPED),
    /**
     * The given session option name is invalid.
     */
    INVALID_NAME(Flight.SetSessionOptionsResult.Status.INVALID_NAME),
    /**
     * The session option value is invalid.
     */
    INVALID_VALUE(Flight.SetSessionOptionsResult.Status.INVALID_VALUE),
    /**
     * The session option cannot be set.
     */
    ERROR(Flight.SetSessionOptionsResult.Status.ERROR),
    ;

    private static final Map<Flight.SetSessionOptionsResult.Status, SetSessionOptionsResult.Status> mapFromProto;

    static {
      for (Status s : values()) mapFromProto.put(s.proto, s);
    }

    private final Flight.SetSessionOptionsResult.Status proto;

    private Status(Flight.SetSessionOptionsResult.Status s) {
      proto = s;
    }

    public static Status fromProtocol(Flight.SetSessionOptionsResult.Status s) {
      return mapFromProto.get(s);
    }

    public Flight.SetSessionOptionsResult.Status toProtocol() {
      return proto;
    }
  }

  private final Map<String, Status> results;

  public SetSessionOptionsResult(Map<String, Status> results) {
    this.results = Collections.unmodifiableMap(new HashMap<String, Status>(results));
  }

  SetSessionOptionsResult(Flight.SetSessionOptionsResult proto) {
    results = Collections.unmodifiableMap(proto.getResults().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, (e) -> Status.fromProtocol(e.getValue()))));
  }

  /**
   *
   * @return An immutable view of the result status map.
   */
  Map<String, Status> getResults() {
    return results;
  }

  Flight.SetSessionOptionsResult toProtocol() {
    Flight.SetSessionOptionsResult.Builder b = Flight.SetSessionOptionsResult.newBuilder();
    b.putAllResults(results.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getValue().toProtocol())));
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
  public static SetSessionOptionsResult deserialize(ByteBuffer serialized) throws IOException {
    return new SetSessionOptionsResult(Flight.SetSessionOptionsResult.parseFrom(serialized));
  }
}
