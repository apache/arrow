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

import java.util.Map;

/** API to Implement an Arrow Flight producer. */
public interface FlightProducer {

  /**
   * Return data for a stream.
   *
   * @param context Per-call context.
   * @param ticket The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  void getStream(CallContext context, Ticket ticket, ServerStreamListener listener);

  /**
   * List available data streams on this service.
   *
   * @param context Per-call context.
   * @param criteria Application-defined criteria for filtering streams.
   * @param listener An interface for sending data back to the client.
   */
  void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener);

  /**
   * Get information about a particular data stream.
   *
   * @param context Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor);

  /**
   * Begin or get an update on execution of a long-running query.
   *
   * <p>If the descriptor would begin a query, the server should return a response immediately to
   * not block the client. Otherwise, the server should not return an update until progress is made
   * to not spam the client with inactionable updates.
   *
   * @param context Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about execution.
   */
  default PollInfo pollFlightInfo(CallContext context, FlightDescriptor descriptor) {
    FlightInfo info = getFlightInfo(context, descriptor);
    return new PollInfo(info, null, null, null);
  }

  /**
   * Get schema for a particular data stream.
   *
   * @param context Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Schema for the stream.
   */
  default SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    FlightInfo info = getFlightInfo(context, descriptor);
    return new SchemaResult(
        info.getSchemaOptional()
            .orElseThrow(
                () ->
                    CallStatus.INVALID_ARGUMENT
                        .withDescription("No schema is present in FlightInfo")
                        .toRuntimeException()));
  }

  /**
   * Accept uploaded data for a particular stream.
   *
   * @param context Per-call context.
   * @param flightStream The data stream being uploaded.
   */
  Runnable acceptPut(
      CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream);

  /**
   * This method is used to perform a bidirectional data exchange between a client and a server.
   *
   * @param context Per-call context.
   * @param reader The FlightStream from which data is read.
   * @param writer The ServerStreamListener to which data is written.
   * @throws RuntimeException if the method is not implemented.
   */
  default void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
    throw CallStatus.UNIMPLEMENTED
        .withDescription("DoExchange is unimplemented")
        .toRuntimeException();
  }

  /**
   * Generic handler for application-defined RPCs.
   *
   * @param context Per-call context.
   * @param action Client-supplied parameters.
   * @param listener A stream of responses.
   */
  void doAction(CallContext context, Action action, StreamListener<Result> listener);

  /**
   * List available application-defined RPCs.
   *
   * @param context Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void listActions(CallContext context, StreamListener<ActionType> listener);

  /** An interface for sending Arrow data back to a client. */
  interface ServerStreamListener extends OutboundStreamListener {

    /** Check whether the call has been cancelled. If so, stop sending data. */
    boolean isCancelled();

    /**
     * Set a callback for when the client cancels a call, i.e. {@link #isCancelled()} has become
     * true.
     *
     * <p>Note that this callback may only be called some time after {@link #isCancelled()} becomes
     * true, and may never be called if all executor threads on the server are busy, or the RPC
     * method body is implemented in a blocking fashion.
     */
    void setOnCancelHandler(Runnable handler);
  }

  /**
   * Callbacks for pushing objects to a receiver.
   *
   * @param <T> Type of the values in the stream.
   */
  interface StreamListener<T> {

    /** Send the next value to the client. */
    void onNext(T val);

    /**
     * Indicate an error to the client.
     *
     * <p>Terminates the stream; do not call {@link #onCompleted()}.
     */
    void onError(Throwable t);

    /** Indicate that the transmission is finished. */
    void onCompleted();
  }

  /** Call-specific context. */
  interface CallContext {
    /** The identity of the authenticated peer. May be the empty string if unknown. */
    String peerIdentity();

    /** Whether the call has been cancelled by the client. */
    boolean isCancelled();

    /**
     * Get the middleware instance of the given type for this call.
     *
     * <p>Returns null if not found.
     */
    <T extends FlightServerMiddleware> T getMiddleware(FlightServerMiddleware.Key<T> key);

    /** Get an immutable map of middleware for this call. */
    Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> getMiddleware();
  }
}
