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

import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Empty;
import org.apache.arrow.flight.impl.Flight.PutResult;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceBlockingStub;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

public class FlightClient implements AutoCloseable {
  private final BufferAllocator allocator;
  private final ManagedChannel channel;
  private final FlightServiceBlockingStub blockingStub;
  private final FlightServiceStub asyncStub;
  private final MethodDescriptor<Flight.Ticket, ArrowMessage> doGetDescriptor;
  private final MethodDescriptor<ArrowMessage, Flight.PutResult> doPutDescriptor;

  /** Construct client for accessing RouteGuide server using the existing channel. */
  public FlightClient(BufferAllocator allocator, Location location) {
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(location.getHost(), location.getPort())
        .usePlaintext();
    this.allocator = allocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
    channel = channelBuilder.build();
    blockingStub = FlightServiceGrpc.newBlockingStub(channel);
    asyncStub = FlightServiceGrpc.newStub(channel);
    doGetDescriptor = FlightBindingService.getDoGetDescriptor(allocator);
    doPutDescriptor = FlightBindingService.getDoPutDescriptor(allocator);
  }

  /**
   * Get a list of available flights
   * @param criteria
   * @return
   */
  public Iterable<FlightInfo> listFlights(Criteria criteria){
    return ImmutableList.copyOf(blockingStub.listFlights(criteria.asCriteria()))
        .stream()
        .map(t -> new FlightInfo(t))
        .collect(Collectors.toList());
  }

  public Iterable<ActionType> listActions(){
    return ImmutableList.copyOf(blockingStub.listActions(Empty.getDefaultInstance()))
        .stream()
        .map(t -> new ActionType(t))
        .collect(Collectors.toList());
  }

  public Result doAction(Action action) {
    return new Result(blockingStub.doAction(action.toProtocol()));
  }

  /**
   * Create or append a descriptor with another stream.
   * @param descriptor
   * @param root
   * @return
   */
  public ClientStreamListener startPut(FlightDescriptor descriptor, VectorSchemaRoot root) {
    Preconditions.checkNotNull(descriptor);
    Preconditions.checkNotNull(root);

    SetStreamObserver<PutResult> resultObserver = new SetStreamObserver<>();
    StreamObserver<ArrowMessage> observer = asyncClientStreamingCall(channel.newCall(doPutDescriptor, asyncStub.getCallOptions()), resultObserver);
    // send the schema to start.
    ArrowMessage message = new ArrowMessage(descriptor.toProtocol(), root.getSchema());
    observer.onNext(message);
    return new PutObserver(new VectorUnloader(root, true, false), observer, resultObserver.getFuture());
  }

  public FlightInfo getInfo(FlightDescriptor descriptor) {
    return new FlightInfo(blockingStub.getFlightInfo(descriptor.toProtocol()));
  }

  public FlightStream getStream(Ticket ticket) {
    FlightStream stream = new FlightStream(allocator);
    asyncServerStreamingCall(channel.newCall(doGetDescriptor, asyncStub.getCallOptions()), ticket.toProtocol(), stream.asObserver());
    return stream;
  }

  private static class SetStreamObserver<T> implements StreamObserver<T>{
    private final SettableFuture<T> result = SettableFuture.create();
    private volatile T resultLocal;

    @Override
    public void onNext(T value) {
      resultLocal = value;
    }

    @Override
    public void onError(Throwable t) {
      result.setException(t);
    }

    @Override
    public void onCompleted() {
      result.set(Preconditions.checkNotNull(resultLocal));
    }

    public ListenableFuture<T> getFuture(){
      return result;
    }
  }

  private static class PutObserver implements ClientStreamListener {
    private final StreamObserver<ArrowMessage> observer;
    private final VectorUnloader unloader;
    private final ListenableFuture<PutResult> futureResult;

    public PutObserver(VectorUnloader unloader, StreamObserver<ArrowMessage> observer, ListenableFuture<PutResult> futureResult) {
      this.observer = observer;
      this.unloader = unloader;
      this.futureResult = futureResult;
    }

    @Override
    public void putNext() {
      ArrowRecordBatch batch = unloader.getRecordBatch();
      observer.onNext(new ArrowMessage(batch));
    }

    @Override
    public void error(Throwable ex) {
      observer.onError(ex);
    }

    @Override
    public void completed() {
      observer.onCompleted();
    }

    @Override
    public PutResult getResult(){
      try {
        return futureResult.get();
      }catch(Exception ex) {
        throw Throwables.propagate(ex);
      }
    }
  }

  public interface ClientStreamListener {
    public void putNext();
    public void error(Throwable ex);
    public void completed();
    public PutResult getResult();
  }


  public void close() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    allocator.close();
  }


}
