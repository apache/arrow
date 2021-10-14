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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.flight.ArrowMessage.HeaderType;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

import com.google.common.util.concurrent.SettableFuture;

import io.grpc.stub.StreamObserver;

/**
 * An adaptor between protobuf streams and flight data streams.
 */
public class FlightStream implements AutoCloseable {
  // Use AutoCloseable sentinel objects to simplify logic in #close
  private final AutoCloseable DONE = () -> {
  };
  private final AutoCloseable DONE_EX = () -> {
  };

  private final BufferAllocator allocator;
  private final Cancellable cancellable;
  private final LinkedBlockingQueue<AutoCloseable> queue = new LinkedBlockingQueue<>();
  private final SettableFuture<VectorSchemaRoot> root = SettableFuture.create();
  private final SettableFuture<FlightDescriptor> descriptor = SettableFuture.create();
  private final int pendingTarget;
  private final Requestor requestor;
  // The completion flags.
  // This flag is only updated as the user iterates through the data, i.e. it tracks whether the user has read all the
  // data and closed the stream
  final CompletableFuture<Void> completed;
  // This flag is immediately updated when gRPC signals that the server has ended the call. This is used to make sure
  // we don't block forever trying to write to a server that has rejected a call.
  final CompletableFuture<Void> cancelled;

  private volatile int pending = 1;
  private volatile VectorSchemaRoot fulfilledRoot;
  private DictionaryProvider.MapDictionaryProvider dictionaries;
  private volatile VectorLoader loader;
  private volatile Throwable ex;
  private volatile ArrowBuf applicationMetadata = null;
  @VisibleForTesting
  volatile MetadataVersion metadataVersion = null;

  /**
   * Constructs a new instance.
   *
   * @param allocator  The allocator to use for creating/reallocating buffers for Vectors.
   * @param pendingTarget Target number of messages to receive.
   * @param cancellable Used to cancel mid-stream requests.
   * @param requestor A callback to determine how many pending items there are.
   */
  public FlightStream(BufferAllocator allocator, int pendingTarget, Cancellable cancellable, Requestor requestor) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(requestor);
    this.allocator = allocator;
    this.pendingTarget = pendingTarget;
    this.cancellable = cancellable;
    this.requestor = requestor;
    this.dictionaries = new DictionaryProvider.MapDictionaryProvider();
    this.completed = new CompletableFuture<>();
    this.cancelled = new CompletableFuture<>();
  }

  /**
   * Get the schema for this stream. Blocks until the schema is available.
   */
  public Schema getSchema() {
    return getRoot().getSchema();
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

  /**
   * Get the descriptor for this stream. Only applicable on the server side of a DoPut operation. Will block until the
   * client sends the descriptor.
   */
  public FlightDescriptor getDescriptor() {
    // This blocks until the first message from the client is received.
    try {
      return descriptor.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw CallStatus.INTERNAL.withCause(e).withDescription("Interrupted").toRuntimeException();
    } catch (ExecutionException e) {
      throw CallStatus.INTERNAL.withCause(e).withDescription("Error getting descriptor").toRuntimeException();
    }
  }

  /**
   * Closes the stream (freeing any existing resources).
   *
   * <p>If the stream isn't complete and is cancellable, this method will cancel and drain the stream first.
   */
  public void close() throws Exception {
    final List<AutoCloseable> closeables = new ArrayList<>();
    Throwable suppressor = null;
    if (cancellable != null) {
      // Client-side stream. Cancel the call, to help ensure gRPC doesn't deliver a message after close() ends.
      // On the server side, we can't rely on draining the stream , because this gRPC bug means the completion callback
      // may never run https://github.com/grpc/grpc-java/issues/5882
      try {
        synchronized (cancellable) {
          if (!cancelled.isDone()) {
            // Only cancel if the call is not done on the gRPC side
            cancellable.cancel("Stream closed before end", /* no exception to report */null);
          }
        }
        // Drain the stream without the lock (as next() implicitly needs the lock)
        while (next()) { }
      } catch (FlightRuntimeException e) {
        suppressor = e;
      }
    }
    // Perform these operations under a lock. This way the observer can't enqueue new messages while we're in the
    // middle of cleanup. This should only be a concern for server-side streams since client-side streams are drained
    // by the lambda above.
    synchronized (completed) {
      try {
        if (fulfilledRoot != null) {
          closeables.add(fulfilledRoot);
        }
        closeables.add(applicationMetadata);
        closeables.addAll(queue);
        if (dictionaries != null) {
          dictionaries.getDictionaryIds().forEach(id -> closeables.add(dictionaries.lookup(id).getVector()));
        }
        if (suppressor != null) {
          AutoCloseables.close(suppressor, closeables);
        } else {
          AutoCloseables.close(closeables);
        }
      } finally {
        // The value of this CompletableFuture is meaningless, only whether it's completed (or has an exception)
        // No-op if already complete
        completed.complete(null);
      }
    }
  }

  /**
   * Blocking request to load next item into list.
   * @return Whether or not more data was found.
   */
  public boolean next() {
    try {
      if (completed.isDone() && queue.isEmpty()) {
        return false;
      }

      pending--;
      requestOutstanding();

      Object data = queue.take();
      if (DONE == data) {
        queue.put(DONE);
        // Other code ignores the value of this CompletableFuture, only whether it's completed (or has an exception)
        completed.complete(null);
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
          if (msg.getMessageType() == HeaderType.NONE) {
            updateMetadata(msg);
            // We received a message without data, so erase any leftover data
            if (fulfilledRoot != null) {
              fulfilledRoot.clear();
            }
          } else if (msg.getMessageType() == HeaderType.RECORD_BATCH) {
            checkMetadataVersion(msg);
            // Ensure we have the root
            root.get().clear();
            try (ArrowRecordBatch arb = msg.asRecordBatch()) {
              loader.load(arb);
            }
            updateMetadata(msg);
          } else if (msg.getMessageType() == HeaderType.DICTIONARY_BATCH) {
            checkMetadataVersion(msg);
            // Ensure we have the root
            root.get().clear();
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
    } catch (RuntimeException e) {
      throw e;
    } catch (ExecutionException e) {
      throw StatusUtils.fromThrowable(e.getCause());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Update our metadata reference with a new one from this message. */
  private void updateMetadata(ArrowMessage msg) {
    if (this.applicationMetadata != null) {
      this.applicationMetadata.close();
    }
    this.applicationMetadata = msg.getApplicationMetadata();
    if (this.applicationMetadata != null) {
      this.applicationMetadata.getReferenceManager().retain();
    }
  }

  /** Ensure the Arrow metadata version doesn't change mid-stream. */
  private void checkMetadataVersion(ArrowMessage msg) {
    if (msg.asSchemaMessage() == null) {
      return;
    }
    MetadataVersion receivedVersion = MetadataVersion.fromFlatbufID(msg.asSchemaMessage().getMessage().version());
    if (this.metadataVersion != receivedVersion) {
      throw new IllegalStateException("Metadata version mismatch: stream started as " +
          this.metadataVersion + " but got message with version " + receivedVersion);
    }
  }

  /**
   * Get the current vector data from the stream.
   *
   * <p>The data in the root may change at any time. Clients should NOT modify the root, but instead unload the data
   * into their own root.
   *
   * @throws FlightRuntimeException if there was an error reading the schema from the stream.
   */
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
   * Check if there is a root (i.e. whether the other end has started sending data).
   *
   * Updated by calls to {@link #next()}.
   *
   * @return true if and only if the other end has started sending data.
   */
  public boolean hasRoot() {
    return root.isDone();
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

    Observer() {
      super();
    }

    /** Helper to add an item to the queue under the appropriate lock. */
    private void enqueue(AutoCloseable message) {
      synchronized (completed) {
        if (completed.isDone()) {
          // The stream is already closed (RPC ended), discard the message
          AutoCloseables.closeNoChecked(message);
        } else {
          queue.add(message);
        }
      }
    }

    @Override
    public void onNext(ArrowMessage msg) {
      // Operations here have to be under a lock so that we don't add a message to the queue while in the middle of
      // close().
      requestOutstanding();
      switch (msg.getMessageType()) {
        case NONE: {
          // No IPC message - pure metadata or descriptor
          if (msg.getDescriptor() != null) {
            descriptor.set(new FlightDescriptor(msg.getDescriptor()));
          }
          if (msg.getApplicationMetadata() != null) {
            enqueue(msg);
          }
          break;
        }
        case SCHEMA: {
          Schema schema = msg.asSchema();
          
          // if there is app metadata in the schema message, make sure
          // that we don't leak it.
          ArrowBuf meta = msg.getApplicationMetadata();
          if (meta != null) {
            meta.close();
          }

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
          metadataVersion = MetadataVersion.fromFlatbufID(msg.asSchemaMessage().getMessage().version());
          try {
            MetadataV4UnionChecker.checkRead(schema, metadataVersion);
          } catch (IOException e) {
            ex = e;
            enqueue(DONE_EX);
            break;
          }

          synchronized (completed) {
            if (!completed.isDone()) {              
              fulfilledRoot = VectorSchemaRoot.create(schema, allocator);
              loader = new VectorLoader(fulfilledRoot);
              if (msg.getDescriptor() != null) {
                descriptor.set(new FlightDescriptor(msg.getDescriptor()));
              }
              root.set(fulfilledRoot);
            }
          }
          break;
        }
        case RECORD_BATCH:
        case DICTIONARY_BATCH:
          enqueue(msg);
          break;
        case TENSOR:
        default:
          ex = new UnsupportedOperationException("Unable to handle message of type: " + msg.getMessageType());
          enqueue(DONE_EX);
      }
    }

    @Override
    public void onError(Throwable t) {
      ex = StatusUtils.fromThrowable(t);
      queue.add(DONE_EX);
      cancelled.complete(null);
      root.setException(ex);
    }

    @Override
    public void onCompleted() {
      // Depends on gRPC calling onNext and onCompleted non-concurrently
      cancelled.complete(null);
      queue.add(DONE);
    }
  }

  /**
   * Cancels sending the stream to a client.
   *
   * <p>Callers should drain the stream (with {@link #next()}) to ensure all messages sent before cancellation are
   * received and to wait for the underlying transport to acknowledge cancellation.
   */
  public void cancel(String message, Throwable exception) {
    if (cancellable == null) {
      throw new UnsupportedOperationException("Streams cannot be cancelled that are produced by client. " +
          "Instead, server should reject incoming messages.");
    }
    cancellable.cancel(message, exception);
    // Do not mark the stream as completed, as gRPC may still be delivering messages.
  }

  StreamObserver<ArrowMessage> asObserver() {
    return new Observer();
  }

  /**
   * Provides a callback to cancel a process that is in progress.
   */
  @FunctionalInterface
  public interface Cancellable {
    void cancel(String message, Throwable exception);
  }

  /**
   * Provides a interface to request more items from a stream producer.
   */
  @FunctionalInterface
  public interface Requestor {
    /**
     * Requests <code>count</code> more messages from the instance of this object.
     */
    void request(int count);
  }
}
