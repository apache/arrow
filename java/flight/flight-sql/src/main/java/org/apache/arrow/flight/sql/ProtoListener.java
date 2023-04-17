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

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Result;

import com.google.protobuf.Any;
import com.google.protobuf.Message;

/**
 * A StreamListener that accepts a particular type.
 *
 * @param <T> The message type to accept.
 */
class ProtoListener<T extends Message> implements FlightProducer.StreamListener<T> {
  private final FlightProducer.StreamListener<Result> listener;

  ProtoListener(FlightProducer.StreamListener<Result> listener) {
    this.listener = listener;
  }

  @Override
  public void onNext(T val) {
    listener.onNext(new Result(Any.pack(val).toByteArray()));
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
