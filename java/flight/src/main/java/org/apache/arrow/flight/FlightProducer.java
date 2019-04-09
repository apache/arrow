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

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * API to Implement an Arrow Flight producer.
 */
public interface FlightProducer {

  /**
   * Return data for the given stream.
   */
  void getStream(CallContext context, Ticket ticket,
      ServerStreamListener listener);

  /** List available flights given some criteria. */
  void listFlights(CallContext context, Criteria criteria,
      StreamListener<FlightInfo> listener);

  /** Get information on a specific flight. */
  FlightInfo getFlightInfo(CallContext context,
      FlightDescriptor descriptor);

  /** Accept uploaded data for a flight. */
  Runnable acceptPut(CallContext context,
      FlightStream flightStream, StreamListener<PutResult> ackStream);

  void doAction(CallContext context, Action action,
      StreamListener<Result> listener);

  void listActions(CallContext context,
      StreamListener<ActionType> listener);

  /**
   * Listener for creating a stream on the server side.
   */
  interface ServerStreamListener {

    /**
     * Check if the client has cancelled their request.
     */
    boolean isCancelled();

    /**
     * Check if the client is ready to receive further data.
     */
    boolean isReady();

    /**
     * Begin sending data from the specified root.
     */
    void start(VectorSchemaRoot root);

    /** Send the current contents of the root to the client. */
    void putNext();

    /**
     * Send the current contents of the root to the client, along with application-defined metadata on the side.
     */
    void putNext(byte[] metadata);

    /**
     *  Indicate an error to the client. Do not call {@link #completed} afterwards.
     */
    void error(Throwable ex);

    /** Indicate that the stream is finished. */
    void completed();

  }

  /**
   * Callbacks for pushing objects to a receiver.
   *
   * @param <T> Type of the values in the stream.
   */
  interface StreamListener<T> {

    void onNext(T val);

    void onError(Throwable t);

    void onCompleted();

  }

  /**
   * Call-specific context.
   */
  interface CallContext {
    /** The identity of the authenticated peer. May be the empty string if unknown. */
    String peerIdentity();

    /** Whether the call has been cancelled by the client. */
    boolean isCancelled();
  }
}
