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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.ActionType;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.FlightGetInfo;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.flight.impl.Flight.Result;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceImplBase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;

import com.google.common.base.Preconditions;

import io.grpc.stub.StreamObserver;

class FlightService extends FlightServiceImplBase {
  private final BufferAllocator allocator;
  private final FlightProducer producer;
  private final ExecutorService executors = Executors.newCachedThreadPool();

  public FlightService(BufferAllocator allocator, FlightProducer producer) {
    this.allocator = allocator;
    this.producer = producer;
  }

  @Override
  public void listFlights(Flight.Criteria criteria, StreamObserver<FlightGetInfo> responseObserver) {
    producer.listFlights(new Criteria(criteria), StreamPipe.wrap(responseObserver, t -> t.toProtocol()));
  }

  public void doGetCustom(Flight.Ticket ticket, StreamObserver<ArrowMessage> responseObserver) {
    producer.getStream(new Ticket(ticket), new GetListener(responseObserver));
  }

  @Override
  public void doAction(Flight.Action request, StreamObserver<Result> responseObserver) {
    try {
      responseObserver.onNext(producer.doAction(new Action(request)).toProtocol());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      responseObserver.onError(ex);
    }
  }

  @Override
  public void listActions(Empty request, StreamObserver<ActionType> responseObserver) {
    producer.listActions(StreamPipe.wrap(responseObserver, t -> t.toProtocol()));
  }

  private static class GetListener implements ServerStreamListener {
    private StreamObserver<ArrowMessage> responseObserver;
    private volatile VectorUnloader unloader;

    public GetListener(StreamObserver<ArrowMessage> responseObserver) {
      super();
      this.responseObserver = responseObserver;
    }

    @Override
    public void start(VectorSchemaRoot root) {
      responseObserver.onNext(new ArrowMessage(null, root.getSchema()));
      unloader = new VectorUnloader(root, true, false);
    }

    @Override
    public void putNext() {
      Preconditions.checkNotNull(unloader);
      responseObserver.onNext(new ArrowMessage(unloader.getRecordBatch()));
    }

    @Override
    public void error(Throwable ex) {
      responseObserver.onError(ex);
    }

    @Override
    public void completed() {
      responseObserver.onCompleted();
    }

  }

  public StreamObserver<ArrowMessage> doPutCustom(final StreamObserver<PutResult> responseObserver){
    FlightStream fs = new FlightStream(allocator);
    executors.submit(() -> {
      try {
        responseObserver.onNext(producer.acceptPut(fs).call());
        responseObserver.onCompleted();
      } catch (Exception ex) {
        responseObserver.onError(ex);
      }
    });

    return fs.asObserver();
  }

  @Override
  public void getFlightInfo(Flight.FlightDescriptor request, StreamObserver<FlightGetInfo> responseObserver) {
    try {
      FlightInfo info = producer.getFlightInfo(new FlightDescriptor(request));
      responseObserver.onNext(info.toProtocol());
      responseObserver.onCompleted();
    } catch(Exception ex) {
      responseObserver.onError(ex);
    }
  }


}