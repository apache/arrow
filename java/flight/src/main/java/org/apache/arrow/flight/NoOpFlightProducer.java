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
 * A {@link FlightProducer} that throws on all operations.
 */
public class NoOpFlightProducer implements FlightProducer {

  @Override
  public void getStream(CallContext context, Ticket ticket,
      ServerStreamListener listener) {
    listener.error(new UnsupportedOperationException("NYI"));
  }

  @Override
  public void listFlights(CallContext context, Criteria criteria,
      StreamListener<FlightInfo> listener) {
    listener.onError(new UnsupportedOperationException("NYI"));
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context,
      FlightDescriptor descriptor) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public Runnable acceptPut(CallContext context,
      FlightStream flightStream, StreamListener<PutResult> ackStream) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void doAction(CallContext context, Action action,
      StreamListener<Result> listener) {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public void listActions(CallContext context,
      StreamListener<ActionType> listener) {
    listener.onError(new UnsupportedOperationException("NYI"));
  }

}
