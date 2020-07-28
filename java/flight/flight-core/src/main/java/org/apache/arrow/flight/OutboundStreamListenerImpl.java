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

import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;

import io.grpc.stub.CallStreamObserver;

/**
 * A base class for writing Arrow data to a Flight stream.
 */
abstract class OutboundStreamListenerImpl implements OutboundStreamListener {
  private final FlightDescriptor descriptor; // nullable
  protected final CallStreamObserver<ArrowMessage> responseObserver;
  protected volatile VectorUnloader unloader; // null until stream started
  protected IpcOption option; // null until stream started

  OutboundStreamListenerImpl(FlightDescriptor descriptor, CallStreamObserver<ArrowMessage> responseObserver) {
    Preconditions.checkNotNull(responseObserver, "responseObserver must be provided");
    this.descriptor = descriptor;
    this.responseObserver = responseObserver;
    this.unloader = null;
  }

  @Override
  public boolean isReady() {
    return responseObserver.isReady();
  }

  @Override
  public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
    this.option = option;
    try {
      DictionaryUtils.generateSchemaMessages(root.getSchema(), descriptor, dictionaries, option,
          responseObserver::onNext);
    } catch (RuntimeException e) {
      // Propagate runtime exceptions, like those raised when trying to write unions with V4 metadata
      throw e;
    } catch (Exception e) {
      // Only happens if closing buffers somehow fails - indicates application is an unknown state so propagate
      // the exception
      throw new RuntimeException("Could not generate and send all schema messages", e);
    }
    // We include the null count and align buffers to be compatible with Flight/C++
    unloader = new VectorUnloader(root, /* includeNullCount */ true, /* alignBuffers */ true);
  }

  @Override
  public void putNext() {
    putNext(null);
  }

  /**
   * Busy-wait until the stream is ready.
   *
   * <p>This is overridable as client/server have different behavior.
   */
  protected abstract void waitUntilStreamReady();

  @Override
  public void putNext(ArrowBuf metadata) {
    if (unloader == null) {
      throw CallStatus.INTERNAL.withDescription("Stream was not started, call start()").toRuntimeException();
    }

    waitUntilStreamReady();
    // close is a no-op if the message has been written to gRPC, otherwise frees the associated buffers
    // in some code paths (e.g. if the call is cancelled), gRPC does not write the message, so we need to clean up
    // ourselves. Normally, writing the ArrowMessage will transfer ownership of the data to gRPC/Netty.
    try (final ArrowMessage message = new ArrowMessage(unloader.getRecordBatch(), metadata, option)) {
      responseObserver.onNext(message);
    } catch (Exception e) {
      // This exception comes from ArrowMessage#close, not responseObserver#onNext.
      // Generally this should not happen - ArrowMessage's implementation only closes non-throwing things.
      // The user can't reasonably do anything about this, but if something does throw, we shouldn't let
      // execution continue since other state (e.g. allocators) may be in an odd state.
      throw new RuntimeException("Could not free ArrowMessage", e);
    }
  }

  @Override
  public void putMetadata(ArrowBuf metadata) {
    waitUntilStreamReady();
    try (final ArrowMessage message = new ArrowMessage(metadata)) {
      responseObserver.onNext(message);
    } catch (Exception e) {
      throw StatusUtils.fromThrowable(e);
    }
  }

  @Override
  public void error(Throwable ex) {
    responseObserver.onError(StatusUtils.toGrpcException(ex));
  }

  @Override
  public void completed() {
    responseObserver.onCompleted();
  }
}
