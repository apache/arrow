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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.arrow.flight.ArrowMessage.HeaderType;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ArrowBuf;

/**
 * An adaptor between protobuf streams and flight data streams.
 */
public class FlightStream implements AutoCloseable {


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
  private DictionaryProvider.MapDictionaryProvider dictionaries;
  private volatile VectorLoader loader;
  private volatile Throwable ex;
  private volatile FlightDescriptor descriptor;
  private volatile Schema schema;
  private volatile ArrowBuf applicationMetadata = null;

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
    this.dictionaries = new DictionaryProvider.MapDictionaryProvider();
  }

  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the provider for dictionaries in this stream.
   *
   * <p>Does NOT retain a reference to the underlying dictionaries. Dictionaries may be updated as the stream is read.
   * This method is intended for stream processing, where the application code will not retain references to values
   * after the stream is closed.
   *
   * @throws IllegalStateException if {@link #takeDictionaryOwnership()} was called
   * @see #takeDictionaryOwnership()
   */
  public DictionaryProvider getDictionaryProvider() {
    if (dictionaries == null) {
      throw new IllegalStateException("Dictionary ownership was claimed by the application.");
    }
    return dictionaries;
  }

  /**
   * Get an owned reference to the dictionaries in this stream. Should be called after finishing reading the stream,
   * but before closing.
   *
   * <p>If called, the client is responsible for closing the dictionaries in this provider. Can only be called once.
   *
   * @return The dictionary provider for the stream.
   * @throws IllegalStateException if called more than once.
   */
  public DictionaryProvider takeDictionaryOwnership() {
    if (dictionaries == null) {
      throw new IllegalStateException("Dictionary ownership was claimed by the application.");
    }
    // Swap out the provider so it is not closed
    final DictionaryProvider provider = dictionaries;
    dictionaries = null;
    return provider;
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

    final List<FieldVector> dictionaryVectors =
        dictionaries == null ? Collections.emptyList() : dictionaries.getDictionaryIds().stream()
        .map(id -> dictionaries.lookup(id).getVector()).collect(Collectors.toList());

    // Must check for null since ImmutableList doesn't accept nulls
    AutoCloseables.close(Iterables.concat(closeables,
        dictionaryVectors,
        applicationMetadata != null ? ImmutableList.of(root.get(), applicationMetadata)
            : ImmutableList.of(root.get())));
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
        try (ArrowMessage msg = ((ArrowMessage) data)) {
          if (msg.getMessageType() == HeaderType.RECORD_BATCH) {
            try (ArrowRecordBatch arb = msg.asRecordBatch()) {
              loader.load(arb);
            }
            if (this.applicationMetadata != null) {
              this.applicationMetadata.close();
            }
            this.applicationMetadata = msg.getApplicationMetadata();
            if (this.applicationMetadata != null) {
              this.applicationMetadata.getReferenceManager().retain();
            }
          } else if (msg.getMessageType() == HeaderType.DICTIONARY_BATCH) {
            try (ArrowDictionaryBatch arb = msg.asDictionaryBatch()) {
              final long id = arb.getDictionaryId();
              if (dictionaries == null) {
                throw new IllegalStateException("Dictionary ownership was claimed by the application.");
              }
              final Dictionary dictionary = dictionaries.lookup(id);
              if (dictionary == null) {
                throw new IllegalArgumentException("Dictionary not defined in schema: ID " + id);
              }

              final FieldVector vector = dictionary.getVector();
              final VectorSchemaRoot dictionaryRoot = new VectorSchemaRoot(Collections.singletonList(vector.getField()),
                  Collections.singletonList(vector), 0);
              final VectorLoader dictionaryLoader = new VectorLoader(dictionaryRoot);
              dictionaryLoader.load(arb.getDictionary());
            }
            return next();
          } else {
            throw new UnsupportedOperationException("Message type is unsupported: " + msg.getMessageType());
          }
          return true;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Get the current vector data from the stream. */
  public VectorSchemaRoot getRoot() {
    try {
      return root.get();
    } catch (InterruptedException e) {
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    } catch (ExecutionException e) {
      throw StatusUtils.fromThrowable(e.getCause());
    }
  }

  /**
   * Get the most recent metadata sent from the server. This may be cleared by calls to {@link #next()} if the server
   * sends a message without metadata. This does NOT take ownership of the buffer - call retain() to create a reference
   * if you need the buffer after a call to {@link #next()}.
   *
   * @return the application metadata. May be null.
   */
  public ArrowBuf getLatestMetadata() {
    return applicationMetadata;
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
        case SCHEMA: {
          schema = msg.asSchema();
          final List<Field> fields = new ArrayList<>();
          final Map<Long, Dictionary> dictionaryMap = new HashMap<>();
          for (final Field originalField : schema.getFields()) {
            final Field updatedField = DictionaryUtility.toMemoryFormat(originalField, allocator, dictionaryMap);
            fields.add(updatedField);
          }
          for (final Map.Entry<Long, Dictionary> entry : dictionaryMap.entrySet()) {
            dictionaries.put(entry.getValue());
          }
          schema = new Schema(fields, schema.getCustomMetadata());
          fulfilledRoot = VectorSchemaRoot.create(schema, allocator);
          loader = new VectorLoader(fulfilledRoot);
          descriptor = msg.getDescriptor() != null ? new FlightDescriptor(msg.getDescriptor()) : null;
          root.set(fulfilledRoot);

          break;
        }
        case RECORD_BATCH:
          queue.add(msg);
          break;
        case DICTIONARY_BATCH:
          queue.add(msg);
          break;
        case NONE:
        case TENSOR:
        default:
          queue.add(DONE_EX);
          ex = new UnsupportedOperationException("Unable to handle message of type: " + msg.getMessageType());

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

  /**
   * Provides a callback to cancel a process that is in progress.
   */
  public interface Cancellable {
    void cancel(String message, Throwable exception);
  }

  /**
   * Provides a interface to request more items from a stream producer.
   */
  public interface Requestor {
    /**
     * Requests <code>count</code> more messages from the instance of this object.
     */
    void request(int count);
  }
}
