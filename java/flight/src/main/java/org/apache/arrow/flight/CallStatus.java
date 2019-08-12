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

import java.util.Objects;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;

/**
 * The result of a Flight RPC, consisting of a status code with an optional description and/or exception that led
 * to the status.
 *
 * <p>If raised or sent through {@link StreamListener#onError(Throwable)} or
 * {@link ServerStreamListener#error(Throwable)}, the client call will raise the same error (a
 * {@link FlightRuntimeException} with the same {@link FlightStatusCode} and description). The exception within, if
 * present, will not be sent to the client.
 */
public class CallStatus {

  private final FlightStatusCode code;
  private final Throwable cause;
  private final String description;

  public static final CallStatus UNKNOWN = FlightStatusCode.UNKNOWN.toStatus();
  public static final CallStatus INTERNAL = FlightStatusCode.INTERNAL.toStatus();
  public static final CallStatus INVALID_ARGUMENT = FlightStatusCode.INVALID_ARGUMENT.toStatus();
  public static final CallStatus TIMED_OUT = FlightStatusCode.TIMED_OUT.toStatus();
  public static final CallStatus NOT_FOUND = FlightStatusCode.NOT_FOUND.toStatus();
  public static final CallStatus ALREADY_EXISTS = FlightStatusCode.ALREADY_EXISTS.toStatus();
  public static final CallStatus CANCELLED = FlightStatusCode.CANCELLED.toStatus();
  public static final CallStatus UNAUTHENTICATED = FlightStatusCode.UNAUTHENTICATED.toStatus();
  public static final CallStatus UNAUTHORIZED = FlightStatusCode.UNAUTHORIZED.toStatus();
  public static final CallStatus UNIMPLEMENTED = FlightStatusCode.UNIMPLEMENTED.toStatus();
  public static final CallStatus UNAVAILABLE = FlightStatusCode.UNAVAILABLE.toStatus();

  /**
   * Create a new status.
   *
   * @param code The status code.
   * @param cause An exception that resulted in this status (or null).
   * @param description A description of the status (or null).
   */
  public CallStatus(FlightStatusCode code, Throwable cause, String description) {
    this.code = Objects.requireNonNull(code);
    this.cause = cause;
    this.description = description == null ? "" : description;
  }

  /**
   * Create a new status with no cause or description.
   *
   * @param code The status code.
   */
  public CallStatus(FlightStatusCode code) {
    this(code, /* no cause */ null, /* no description */ null);
  }

  /**
   * The status code describing the result of the RPC.
   */
  public FlightStatusCode code() {
    return code;
  }

  /**
   * The exception that led to this result. May be null.
   */
  public Throwable cause() {
    return cause;
  }

  /**
   * A description of the result.
   */
  public String description() {
    return description;
  }

  /**
   * Return a copy of this status with an error message.
   */
  public CallStatus withDescription(String message) {
    return new CallStatus(code, cause, message);
  }

  /**
   * Return a copy of this status with the given exception as the cause. This will not be sent over the wire.
   */
  public CallStatus withCause(Throwable t) {
    return new CallStatus(code, t, description);
  }

  /**
   * Convert the status to an equivalent exception.
   */
  public FlightRuntimeException toRuntimeException() {
    return new FlightRuntimeException(this);
  }
}
