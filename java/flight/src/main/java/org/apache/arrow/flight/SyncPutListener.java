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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.grpc.StatusUtils;

import io.netty.buffer.ArrowBuf;

/**
 * A listener for server-sent application metadata messages during a Flight DoPut. This class wraps the messages in a
 * synchronous interface.
 */
public final class SyncPutListener implements FlightClient.PutListener, AutoCloseable {

  private final LinkedBlockingQueue<Object> queue;
  private final CompletableFuture<Void> completed;
  private static final Object DONE = new Object();
  private static final Object DONE_WITH_EXCEPTION = new Object();

  public SyncPutListener() {
    queue = new LinkedBlockingQueue<>();
    completed = new CompletableFuture<>();
  }

  private PutResult unwrap(Object queueItem) throws InterruptedException, ExecutionException {
    if (queueItem == DONE) {
      queue.put(queueItem);
      return null;
    } else if (queueItem == DONE_WITH_EXCEPTION) {
      queue.put(queueItem);
      completed.get();
    }
    return (PutResult) queueItem;
  }

  /**
   * Get the next message from the server, blocking until it is available.
   *
   * @return The next message, or null if the server is done sending messages. The caller assumes ownership of the
   *     metadata and must remember to close it.
   * @throws InterruptedException if interrupted while waiting.
   * @throws ExecutionException if the server sent an error, or if there was an internal error.
   */
  public PutResult read() throws InterruptedException, ExecutionException {
    return unwrap(queue.take());
  }

  /**
   * Get the next message from the server, blocking for the specified amount of time until it is available.
   *
   * @return The next message, or null if the server is done sending messages or no message arrived before the timeout.
   *     The caller assumes ownership of the metadata and must remember to close it.
   * @throws InterruptedException if interrupted while waiting.
   * @throws ExecutionException if the server sent an error, or if there was an internal error.
   */
  public PutResult poll(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
    return unwrap(queue.poll(timeout, unit));
  }

  @Override
  public void getResult() {
    try {
      completed.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onNext(PutResult val) {
    final ArrowBuf metadata = val.getApplicationMetadata();
    metadata.getReferenceManager().retain();
    queue.add(PutResult.metadata(metadata));
  }

  @Override
  public void onError(Throwable t) {
    completed.completeExceptionally(StatusUtils.fromThrowable(t));
    queue.add(DONE_WITH_EXCEPTION);
  }

  @Override
  public void onCompleted() {
    completed.complete(null);
    queue.add(DONE);
  }

  @Override
  public void close() {
    queue.forEach(o -> {
      if (o instanceof PutResult) {
        ((PutResult) o).close();
      }
    });
  }
}
