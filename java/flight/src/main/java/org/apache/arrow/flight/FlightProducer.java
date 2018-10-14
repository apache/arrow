/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
 * API to Implement an Arrow Flight server.
 */
public interface FlightProducer {

  public void getStream(Ticket ticket, ServerStreamListener listener);

  public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener);

  public FlightInfo getFlightInfo(FlightDescriptor descriptor);

  public Callable<PutResult> acceptPut(FlightStream flightStream);

  public Result doAction(Action action);

  public void listActions(StreamListener<ActionType> listener);

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


}
