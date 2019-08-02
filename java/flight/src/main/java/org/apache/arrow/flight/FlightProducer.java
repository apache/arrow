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
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import io.netty.buffer.ArrowBuf;

/**
 * API to Implement an Arrow Flight producer.
 */
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
  void listFlights(CallContext context, Criteria criteria,
      StreamListener<FlightInfo> listener);

  /**
   * Get information about a particular data stream.
   *
   * @param context Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor);

  /**
   * Get schema for a particular data stream.
   *
   * @param context Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Schema for the stream.
   */
  default SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    FlightInfo info = getFlightInfo(context, descriptor);
    return new SchemaResult(info.getSchema());
  }


  /**
   * Accept uploaded data for a particular stream.
   *
   * @param context Per-call context.
   * @param flightStream The data stream being uploaded.
   */
  Runnable acceptPut(CallContext context,
      FlightStream flightStream, StreamListener<PutResult> ackStream);

  /**
   * Generic handler for application-defined RPCs.
   *
   * @param context Per-call context.
   * @param action Client-supplied parameters.
   * @param listener A stream of responses.
   */
  void doAction(CallContext context, Action action,
      StreamListener<Result> listener);

  /**
   * List available application-defined RPCs.
   * @param context Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void listActions(CallContext context, StreamListener<ActionType> listener);

  /**
   * An interface for sending Arrow data back to a client.
   */
  interface ServerStreamListener {

    /**
     * Check whether the call has been cancelled. If so, stop sending data.
     */
    boolean isCancelled();

    /**
     * A hint indicating whether the client is ready to receive data without excessive buffering.
     */
    boolean isReady();

    /**
     * Start sending data, using the schema of the given {@link VectorSchemaRoot}.
     *
     * <p>This method must be called before all others.
     */
    void start(VectorSchemaRoot root);

    /**
     * Start sending data, using the schema of the given {@link VectorSchemaRoot}.
     *
     * <p>This method must be called before all others.
     */
    void start(VectorSchemaRoot root, DictionaryProvider dictionaries);

    /**
     * Send the current contents of the associated {@link VectorSchemaRoot}.
     */
    void putNext();

    /**
     * Send the current contents of the associated {@link VectorSchemaRoot} alongside application-defined metadata.
     * @param metadata The metadata to send. Ownership of the buffer is transferred to the Flight implementation.
     */
    void putNext(ArrowBuf metadata);

    /**
     * Indicate an error to the client. Terminates the stream; do not call {@link #completed()} afterwards.
     */
    void error(Throwable ex);

    /**
     * Indicate that transmission is finished.
     */
    void completed();

  }

  /**
   * Callbacks for pushing objects to a receiver.
   *
   * @param <T> Type of the values in the stream.
   */
  interface StreamListener<T> {

    /**
     * Send the next value to the client.
     */
    void onNext(T val);

    /**
     * Indicate an error to the client.
     *
     * <p>Terminates the stream; do not call {@link #onCompleted()}.
     */
    void onError(Throwable t);

    /**
     * Indicate that the transmission is finished.
     */
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
