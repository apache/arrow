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

package org.apache.arrow.flight.sql;

import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Result;

/** Typed StreamListener for cancelFlightInfo. */
class CancelStatusListener implements FlightProducer.StreamListener<CancelStatus> {
  private final FlightProducer.StreamListener<Result> listener;

  CancelStatusListener(FlightProducer.StreamListener<Result> listener) {
    this.listener = listener;
  }

  @Override
  public void onNext(CancelStatus val) {
    listener.onNext(new Result(new CancelFlightInfoResult(val).serialize().array()));
  }

  @Override
  public void onError(Throwable t) {
    listener.onError(t);
  }

  @Override
  public void onCompleted() {
    listener.onCompleted();
  }
}
