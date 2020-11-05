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

import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.util.AutoCloseables;

import io.grpc.stub.StreamObserver;

/**
 * Shim listener to avoid exposing GRPC internals.

 * @param <FROM> From Type
 * @param <TO> To Type
 */
class StreamPipe<FROM, TO> implements StreamListener<FROM> {

  private final StreamObserver<TO> delegate;
  private final Function<FROM, TO> mapFunction;
  private final Consumer<Throwable> errorHandler;
  private AutoCloseable resource;
  private boolean closed = false;

  /**
   * Wrap the given gRPC StreamObserver with a transformation function.
   *
   * @param delegate The {@link StreamObserver} to wrap.
   * @param func The transformation function.
   * @param errorHandler A handler for uncaught exceptions (e.g. if something tries to double-close this stream).
   * @param <FROM> The source type.
   * @param <TO> The output type.
   * @return A wrapped listener.
   */
  public static <FROM, TO> StreamPipe<FROM, TO> wrap(StreamObserver<TO> delegate, Function<FROM, TO> func,
      Consumer<Throwable> errorHandler) {
    return new StreamPipe<>(delegate, func, errorHandler);
  }

  public StreamPipe(StreamObserver<TO> delegate, Function<FROM, TO> func, Consumer<Throwable> errorHandler) {
    super();
    this.delegate = delegate;
    this.mapFunction = func;
    this.errorHandler = errorHandler;
    this.resource = null;
  }

  /** Set an AutoCloseable resource to be cleaned up when the gRPC observer is to be completed. */
  void setAutoCloseable(AutoCloseable ac) {
    resource = ac;
  }

  @Override
  public void onNext(FROM val) {
    delegate.onNext(mapFunction.apply(val));
  }

  @Override
  public void onError(Throwable t) {
    if (closed) {
      errorHandler.accept(t);
      return;
    }
    try {
      AutoCloseables.close(resource);
    } catch (Exception e) {
      errorHandler.accept(e);
    } finally {
      // Set closed to true in case onError throws, so that we don't try to close again
      closed = true;
      delegate.onError(StatusUtils.toGrpcException(t));
    }
  }

  @Override
  public void onCompleted() {
    if (closed) {
      errorHandler.accept(new IllegalStateException("Tried to complete already-completed call"));
      return;
    }
    try {
      AutoCloseables.close(resource);
    } catch (Exception e) {
      errorHandler.accept(e);
    } finally {
      // Set closed to true in case onCompleted throws, so that we don't try to close again
      closed = true;
      delegate.onCompleted();
    }
  }

  /**
   * Ensure this stream has been completed.
   */
  void ensureCompleted() {
    if (!closed) {
      onCompleted();
    }
  }
}
