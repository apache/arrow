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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.arrow.flight.grpc.StatusUtils;

import io.grpc.stub.StreamObserver;

/**
 * Utility class for executing an API calls on the FlightServer.
 */
public class ClientApiCallWrapper {

  /**
   * Execute client call using the Stream Observer ApiCallObserver.
   * @param apiCall Function call to execute.
   * @param <T> Type of the values in the stream.
   * @return An iterable of type T.
   */
  private static <T> Iterable<T> doClientCall(Consumer<ApiCallObserver<T>> apiCall) {
    final ApiCallObserver<T> observer = new ApiCallObserver<>();
    try {
      apiCall.accept(observer);
      while (!observer.completed.get()) {/* Wait for results */}
      return observer.result;
    } catch (ExecutionException e) {
      throw StatusUtils.fromThrowable(e.getCause());
    } catch (InterruptedException e) {
      throw StatusUtils.fromThrowable(e);
    }
  }

  /**
   * This method calls the method doClientCall to get the results iterator and use that to get the
   * results.
   * @param apiCall The underlying Flight API call to execute.
   * @param result Functional interface that returns the results.
   * @param retryOnUnauthorized Should the API call be retried if it fails during the initial call.
   * @param <T> Type of the values in the stream.
   * @param <R> Type of the return value from this method.
   * @return An iterable of type R.
   */
  static <T, R> Iterable<R> callFlightApi(Consumer<ApiCallObserver<T>> apiCall,
                                          Function<Iterator<T>, Iterator<R>> result,
                                          boolean retryOnUnauthorized) {
    try {
      final Iterator<T> iterator = doClientCall(apiCall).iterator();
      return () -> result.apply(iterator);
    } catch (FlightRuntimeException ex) {
      if (retryOnUnauthorized) {
        final Iterator<T> iterator = doClientCall(apiCall).iterator();
        return () -> result.apply(iterator);
      }
      throw ex;
    }
  }

  /**
   * Call observer that implements a StreamObserver and stores the values in the stream into
   * and iterable as it encounters them.
   * @param <T> Type of the values in the stream.
   */
  static class ApiCallObserver<T> implements StreamObserver<T> {
    private final CompletableFuture<Boolean> completed;
    private final List<T> result = new ArrayList<>();

    public ApiCallObserver() {
      super();
      completed = new CompletableFuture<>();
    }

    @Override
    public void onNext(T value) {
      result.add(value);
    }

    @Override
    public void onError(Throwable t) {
      completed.completeExceptionally(StatusUtils.fromThrowable(t));
    }

    @Override
    public void onCompleted() {
      completed.complete(true);
    }
  }
}
