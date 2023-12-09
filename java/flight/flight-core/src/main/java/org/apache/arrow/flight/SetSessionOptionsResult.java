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

import org.apache.arrow.flight.impl.Flight;

/** The result of attempting to set a set of session options. */
public class SetSessionOptionsResult {
  public enum ErrorValue {
    /**
     * The status of setting the option is unknown. Servers should avoid using this value
     * (send a NOT_FOUND error if the requested session is not known). Clients can retry
     * the request.
      */
    UNSPECIFIED(Flight.SetSessionOptionsResult.ErrorValue.UNSPECIFIED),
    /**
     * The given session option name is invalid.
     */
    INVALID_NAME(Flight.SetSessionOptionsResult.ErrorValue.INVALID_NAME),
    /**
     * The session option value is invalid.
     */
    INVALID_VALUE(Flight.SetSessionOptionsResult.ErrorValue.INVALID_VALUE),
    /**
     * The session option cannot be set.
     */
    ERROR(Flight.SetSessionOptionsResult.ErrorValue.ERROR),
    ;

    static ErrorValue fromProtocol(Flight.SetSessionOptionsResult.ErrorValue s) {
      return values()[s.ordinal()];
    }

    Flight.SetSessionOptionsResult.Status toProtocol() {
      return Flight.SetSessionOptionsResult.ErrorValue.values()[ordinal()];
    }
  }

  public class Error {
    public ErrorValue value;

    static Error fromProtocol(Flight.SetSessionOptionsResult.Error e) {
      return Error(ErrorValue.fromProtocol(e.getValue()));
    }

    Flight.SetSessionOptionsResult.Error toProtocol() {
      Flight.SetSessionOptionsResult.Error b = Flight.SetSessionOptionsResult.newBuilder();
      b.setValue(value.toProtocol());
      return b.build();
    }
  }

  private final Map<String, ErrorValue> errors;

  public SetSessionOptionsResult(Map<String, ErrorValue> errors) {
    this.errors = Collections.unmodifiableMap(new HashMap<String, ErrorValue>(errors));
  }

  SetSessionOptionsResult(Flight.SetSessionOptionsResult proto) {
    errors = Collections.unmodifiableMap(proto.getErrors().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, (e) -> Error.fromProtocol(e.getValue()))));
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  /**
   *
   * @return An immutable view of the error status map.
   */
  public Map<String, ErrorValue> getErrors() {
    return errors;
  }

  Flight.SetSessionOptionsResult toProtocol() {
    Flight.SetSessionOptionsResult.Builder b = Flight.SetSessionOptionsResult.newBuilder();
    b.putAllResults(errors.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        (e) -> {
          Flight.SetSessionOptionsResult.Error.builder b = Flight.SetSessionOptionsResult.Error.newBuilder();
          b.setValue(Error.fromProtocol(e.getValue()));
          return b.build(); } )));
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
