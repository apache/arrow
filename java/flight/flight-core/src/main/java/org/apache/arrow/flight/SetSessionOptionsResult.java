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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;

/** The result of attempting to set a set of session options. */
public class SetSessionOptionsResult {
  /** Error status value for per-option errors. */
  public enum ErrorValue {
    /**
     * The status of setting the option is unknown. Servers should avoid using this value
     * (send a NOT_FOUND error if the requested session is not known). Clients can retry
     * the request.
      */
    UNSPECIFIED,
    /**
     * The given session option name is invalid.
     */
    INVALID_NAME,
    /**
     * The session option value or type is invalid.
     */
    INVALID_VALUE,
    /**
     * The session option cannot be set.
     */
    ERROR,
    ;

    static ErrorValue fromProtocol(Flight.SetSessionOptionsResult.ErrorValue s) {
      return values()[s.getNumber()];
    }

    Flight.SetSessionOptionsResult.ErrorValue toProtocol() {
      return Flight.SetSessionOptionsResult.ErrorValue.values()[ordinal()];
    }
  }

  /** Per-option extensible error response container. */
  public static class Error {
    public ErrorValue value;

    public Error(ErrorValue value) {
      this.value = value;
    }

    Error(Flight.SetSessionOptionsResult.Error e) {
      value = ErrorValue.fromProtocol(e.getValue());
    }

    Flight.SetSessionOptionsResult.Error toProtocol() {
      Flight.SetSessionOptionsResult.Error.Builder b = Flight.SetSessionOptionsResult.Error.newBuilder();
      b.setValue(value.toProtocol());
      return b.build();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Error that = (Error) o;
      return value == that.value;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  private final Map<String, Error> errors;

  public SetSessionOptionsResult(Map<String, Error> errors) {
    this.errors = Collections.unmodifiableMap(new HashMap<String, Error>(errors));
  }

  SetSessionOptionsResult(Flight.SetSessionOptionsResult proto) {
    errors = Collections.unmodifiableMap(proto.getErrors().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, (e) -> new Error(e.getValue()))));
  }

  /** Report whether the error map has nonzero length. */
  public boolean hasErrors() {
    return errors.size() > 0;
  }

  /**
   * Get the error status map from the result object.
   *
   * @return An immutable view of the error status map.
   */
  public Map<String, Error> getErrors() {
    return errors;
  }

  Flight.SetSessionOptionsResult toProtocol() {
    Flight.SetSessionOptionsResult.Builder b = Flight.SetSessionOptionsResult.newBuilder();
    b.putAllErrors(errors.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        (e) -> e.getValue().toProtocol())));
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
