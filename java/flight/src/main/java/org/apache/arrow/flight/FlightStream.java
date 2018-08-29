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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.stub.StreamObserver;

public class FlightStream {


  private final Object DONE = new Object();
  private final Object DONE_EX = new Object();

  private final BufferAllocator allocator;
  private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
  private final SettableFuture<VectorSchemaRoot> root = SettableFuture.create();

  private boolean completed = false;
  private volatile VectorSchemaRoot fulfilledRoot;
  private volatile VectorLoader loader;
  private volatile Throwable ex;
  private volatile FlightDescriptor descriptor;
  private volatile Schema schema;

  public FlightStream(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public Schema getSchema() {
    return schema;
  }

  public FlightDescriptor getDescriptor() {
    return descriptor;
  }

  public void close() throws Exception {
    AutoCloseables.close(fulfilledRoot);
  }

  /**
   * Blocking request to load next item into list.
   * @return Whether or not more data was found.
   */
  public boolean next() {
    try {
    // make sure we have the root
    root.get().clear();

    if(completed && queue.isEmpty()) {
      return false;
    }

    Object data = queue.take();
    if(DONE == data) {
      queue.put(DONE);
      return false;
    } else if(DONE_EX == data) {
      queue.put(DONE_EX);
      if(ex instanceof Exception) {
        throw (Exception) ex;
      }else {
        throw new Exception(ex);
      }
    } else {
      //loader.load(((ArrowMessage) data).asRecordBatch());
      ArrowMessage msg = ((ArrowMessage) data);
      try(ArrowRecordBatch arb = msg.asRecordBatch()){
        loader.load(arb);
      }
      return true;
    }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public VectorSchemaRoot getRoot() {
    try {
      return root.get();
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  StreamObserver<ArrowMessage> asObserver(){
    return new StreamObserver<ArrowMessage>() {

      @Override
      public void onNext(ArrowMessage msg) {
        switch(msg.getMessageType()) {
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
          ex = new UnsupportedOperationException("Unable to handle message of type." + msg);

        }
      }

      @Override
      public void onError(Throwable t) {
        ex = t;
        queue.add(DONE_EX);
      }

      @Override
      public void onCompleted() {
        queue.add(DONE);
      }};

  }

}
