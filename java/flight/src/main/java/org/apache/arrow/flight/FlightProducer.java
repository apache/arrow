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

import java.util.concurrent.Callable;

import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * API to Implement an Arrow Flight producer.
 */
public interface FlightProducer {

  public void getStream(CallContext context, Ticket ticket,
      ServerStreamListener listener);

  public void listFlights(CallContext context, Criteria criteria,
      StreamListener<FlightInfo> listener);

  public FlightInfo getFlightInfo(CallContext context,
      FlightDescriptor descriptor);

  public Callable<PutResult> acceptPut(CallContext context,
      FlightStream flightStream);

  public Result doAction(CallContext context, Action action);

  public void listActions(CallContext context,
      StreamListener<ActionType> listener);

  public interface ServerStreamListener {

    boolean isCancelled();

    boolean isReady();

    void start(VectorSchemaRoot root);

    void putNext();

    void error(Throwable ex);

    void completed();

  }

  public interface StreamListener<T> {

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
