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

/** The result of cancelling a FlightInfo. */
public enum CancelStatus {
  /**
   * The cancellation status is unknown. Servers should avoid using
   * this value (send a NOT_FOUND error if the requested query is
   * not known). Clients can retry the request.
   */
  UNSPECIFIED,
  /**
   * The cancellation request is complete. Subsequent requests with
   * the same payload may return CANCELLED or a NOT_FOUND error.
   */
  CANCELLED,
  /**
   * The cancellation request is in progress. The client may retry
   * the cancellation request.
   */
  CANCELLING,
  /**
   * The query is not cancellable. The client should not retry the
   * cancellation request.
   */
  NOT_CANCELLABLE,
  ;
}
