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

import org.apache.arrow.flight.FlightProducer.StreamListener;

/**
 * A {@link StreamListener} that does nothing for all callbacks.
 * @param <T> The type of the callback object.
 */
public class NoOpStreamListener<T> implements StreamListener<T> {
  private static NoOpStreamListener INSTANCE = new NoOpStreamListener();

  /** Ignores the value received. */
  @Override
  public void onNext(T val) {
  }

  /** Ignores the error received. */
  @Override
  public void onError(Throwable t) {
  }

  /** Ignores the stream completion event. */
  @Override
  public void onCompleted() {
  }

  @SuppressWarnings("unchecked")
  public static <T> StreamListener<T> getInstance() {
    // Safe because we never use T
    return (StreamListener<T>) INSTANCE;
  }
}
