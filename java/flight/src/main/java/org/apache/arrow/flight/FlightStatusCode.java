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

/**
 * A status code describing the result of a Flight call.
 */
public enum FlightStatusCode {
  /**
   * An unknown error occurred. This may also be the result of an implementation error on the server-side; by default,
   * unhandled server exceptions result in this code.
   */
  UNKNOWN,
  /**
   * An internal/implementation error occurred.
   */
  INTERNAL,
  /**
   * One or more of the given arguments was invalid.
   */
  INVALID_ARGUMENT,
  /**
   * The operation timed out.
   */
  TIMED_OUT,
  /**
   * The operation describes a resource that does not exist.
   */
  NOT_FOUND,
  /**
   * The operation creates a resource that already exists.
   */
  ALREADY_EXISTS,
  /**
   * The operation was cancelled.
   */
  CANCELLED,
  /**
   * The client was not authenticated.
   */
  UNAUTHENTICATED,
  /**
   * The client did not have permission to make the call.
   */
  UNAUTHORIZED,
  /**
   * The requested operation is not implemented.
   */
  UNIMPLEMENTED,
  /**
   * The server cannot currently handle the request. This should be used for retriable requests, i.e. the server
   * should send this code only if it has not done any work.
   */
  UNAVAILABLE,
  ;

  /**
   * Create a blank {@link CallStatus} with this code.
   */
  public CallStatus toStatus() {
    return new CallStatus(this);
  }
}
