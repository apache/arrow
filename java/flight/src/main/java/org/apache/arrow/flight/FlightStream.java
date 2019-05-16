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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.stub.StreamObserver;

/**
 * An adaptor between protobuf streams and flight data streams.
 */
public class FlightStream {


  private final Object DONE = new Object();
  private final Object DONE_EX = new Object();


  private final BufferAllocator allocator;
  private final Cancellable cancellable;
  private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
  private final SettableFuture<VectorSchemaRoot> root = SettableFuture.create();
  private final int pendingTarget;
  private final Requestor requestor;

  private volatile int pending = 1;
  private boolean completed = false;
  private volatile VectorSchemaRoot fulfilledRoot;
  private volatile VectorLoader loader;
  private volatile Throwable ex;
  private volatile FlightDescriptor descriptor;
  private volatile Schema schema;

  /**
   * Constructs a new instance.
   *
   * @param allocator  The allocator to use for creating/reallocating buffers for Vectors.
   * @param pendingTarget Target number of messages to receive.
   * @param cancellable Only provided for streams from server to client, used to cancel mid-stream requests.
   * @param requestor A callback do determine how many pending items there are.
   */
  public FlightStream(BufferAllocator allocator, int pendingTarget, Cancellable cancellable, Requestor requestor) {
    this.allocator = allocator;
    this.pendingTarget = pendingTarget;
    this.cancellable = cancellable;
    this.requestor = requestor;
  }

  public Schema getSchema() {
    return schema;
  }

  public FlightDescriptor getDescriptor() {
    return descriptor;
  }

  /**
   * Closes the stream (freeing any existing resources).
   *
   * <p>If the stream is isn't complete and is cancellable this method will cancel the stream first.</p>
   */
  public void close() throws Exception {
    if (!completed && cancellable != null) {
      cancel("Stream closed before end.", null);
    }
    List<AutoCloseable> closeables = ImmutableList.copyOf(queue.toArray()).stream()
        .filter(t -> AutoCloseable.class.isAssignableFrom(t.getClass()))
        .map(t -> ((AutoCloseable) t))
        .collect(Collectors.toList());

    AutoCloseables.close(Iterables.concat(closeables, ImmutableList.of(root.get())));
  }

  /**
   * Blocking request to load next item into list.
   * @return Whether or not more data was found.
   */
  public boolean next() {
    try {
      // make sure we have the root
      root.get().clear();

      if (completed && queue.isEmpty()) {
        return false;
      }


      pending--;
      requestOutstanding();

      Object data = queue.take();
      if (DONE == data) {
        queue.put(DONE);
        completed = true;
        return false;
      } else if (DONE_EX == data) {
        queue.put(DONE_EX);
        if (ex instanceof Exception) {
          throw (Exception) ex;
        } else {
          throw new Exception(ex);
        }
      } else {
        ArrowMessage msg = ((ArrowMessage) data);
        try (ArrowRecordBatch arb = msg.asRecordBatch()) {
          loader.load(arb);
        }
        return true;
      }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /** Get the current vector data from the stream. */
  public VectorSchemaRoot getRoot() {
    try {
      return root.get();
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private synchronized void requestOutstanding() {
    if (pending < pendingTarget) {
      requestor.request(pendingTarget - pending);
      pending = pendingTarget;
    }
  }

  private class Observer implements StreamObserver<ArrowMessage> {

    public Observer() {
      super();
    }

    @Override
    public void onNext(ArrowMessage msg) {
      requestOutstanding();
      switch (msg.getMessageType()) {
        case SCHEMA:
          schema = msg.asSchema();
          fulfilledRoot = VectorSchemaRoot.create(schema, allocator);
          loader = new VectorLoader(fulfilledRoot);
          descriptor = msg.getDescriptor() != null ? new FlightDescriptor(msg.getDescriptor()) : null;
          root.set(fulfilledRoot);

          break;
        case RECORD_BATCH:
          queue.add(msg);
          break;
        case NONE:
        case DICTIONARY_BATCH:
        case TENSOR:
        default:
          queue.add(DONE_EX);
          ex = new UnsupportedOperationException("Unable to handle message of type: " + msg);

      }

    }

    @Override
    public void onError(Throwable t) {
      ex = t;
      queue.add(DONE_EX);
      root.setException(t);
    }

    @Override
    public void onCompleted() {
      queue.add(DONE);
    }
  }

  /**
   * Cancels sending the stream to a client.
   *
   * @throws UnsupportedOperationException on a stream being uploaded from the client.
   */
  public void cancel(String message, Throwable exception) {
    if (cancellable != null) {
      cancellable.cancel(message, exception);
    } else {
      throw new UnsupportedOperationException("Streams cannot be cancelled that are produced by client. " +
          "Instead, server should reject incoming messages.");
    }
  }

  StreamObserver<ArrowMessage> asObserver() {
    return new Observer();
  }

  public interface Cancellable {
    void cancel(String message, Throwable exception);
  }

  public interface Requestor {
    /**
     * Requests <code>count</code> more messages from the reuqestor.
     */
    void request(int count);
  }
}
