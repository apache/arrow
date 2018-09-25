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

import java.util.function.Function;

import org.apache.arrow.flight.FlightProducer.StreamListener;

import io.grpc.stub.StreamObserver;

/**
 * Shim listener to avoid exposing GRPC internals.

 * @param <T>
 */
class StreamPipe<FROM, TO> implements StreamListener<FROM> {

  private StreamObserver<TO> delegate;
  private Function<FROM, TO> mapFunction;

  public static <FROM, TO> StreamListener<FROM> wrap(StreamObserver<TO> delegate, Function<FROM, TO> func) {
    return new StreamPipe<>(delegate, func);
  }

  public StreamPipe(StreamObserver<TO> delegate, Function<FROM, TO> func) {
    super();
    this.delegate = delegate;
    this.mapFunction = func;
  }

  @Override
  public void onNext(FROM val) {
    delegate.onNext(mapFunction.apply(val));
  }

  @Override
  public void onError(Throwable t) {
    delegate.onError(t);
  }

  @Override
  public void onCompleted() {
    delegate.onCompleted();
  }

}
