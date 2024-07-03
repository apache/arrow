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
package org.apache.arrow.flight.perf;

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.perf.impl.PerfOuterClass.Perf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class TestPerf {

  public static final boolean VALIDATE = false;

  public static FlightDescriptor getPerfFlightDescriptor(
      long recordCount, int recordsPerBatch, int streamCount) {
    final Schema pojoSchema =
        new Schema(
            ImmutableList.of(
                Field.nullable("a", MinorType.BIGINT.getType()),
                Field.nullable("b", MinorType.BIGINT.getType()),
                Field.nullable("c", MinorType.BIGINT.getType()),
                Field.nullable("d", MinorType.BIGINT.getType())));

    byte[] bytes = pojoSchema.serializeAsMessage();
    ByteString serializedSchema = ByteString.copyFrom(bytes);

    return FlightDescriptor.command(
        Perf.newBuilder()
            .setRecordsPerStream(recordCount)
            .setRecordsPerBatch(recordsPerBatch)
            .setSchema(serializedSchema)
            .setStreamCount(streamCount)
            .build()
            .toByteArray());
  }

  public static void main(String[] args) throws Exception {
    new TestPerf().throughput();
  }

  @Test
  public void throughput() throws Exception {
    final int numRuns = 10;
    ListeningExecutorService pool =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));
    double[] throughPuts = new double[numRuns];

    for (int i = 0; i < numRuns; i++) {
      try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
          final PerformanceTestServer server =
              new PerformanceTestServer(a, forGrpcInsecure(LOCALHOST, 0)).start();
          final FlightClient client = FlightClient.builder(a, server.getLocation()).build(); ) {
        final FlightInfo info = client.getInfo(getPerfFlightDescriptor(50_000_000L, 4095, 2));
        List<ListenableFuture<Result>> results =
            info.getEndpoints().stream()
                .map(t -> new Consumer(client, t.getTicket()))
                .map(t -> pool.submit(t))
                .collect(Collectors.toList());

        final Result r =
            Futures.whenAllSucceed(results)
                .call(
                    () -> {
                      Result res = new Result();
                      for (ListenableFuture<Result> f : results) {
                        res.add(f.get());
                      }
                      return res;
                    },
                    pool)
                .get();

        double seconds = r.nanos * 1.0d / 1000 / 1000 / 1000;
        throughPuts[i] = (r.bytes * 1.0d / 1024 / 1024) / seconds;
        System.out.printf(
            "Transferred %d records totaling %s bytes at %f MiB/s. %f record/s. %f batch/s.%n",
            r.rows,
            r.bytes,
            throughPuts[i],
            (r.rows * 1.0d) / seconds,
            (r.batches * 1.0d) / seconds);
      }
    }
    pool.shutdown();

    System.out.println("Summary: ");
    double average = Arrays.stream(throughPuts).sum() / numRuns;
    double sqrSum =
        Arrays.stream(throughPuts).map(val -> val - average).map(val -> val * val).sum();
    double stddev = Math.sqrt(sqrSum / numRuns);
    System.out.printf(
        "Average throughput: %f MiB/s, standard deviation: %f MiB/s%n", average, stddev);
  }

  private static final class Consumer implements Callable<Result> {

    private final FlightClient client;
    private final Ticket ticket;

    public Consumer(FlightClient client, Ticket ticket) {
      super();
      this.client = client;
      this.ticket = ticket;
    }

    @Override
    public Result call() throws Exception {
      final Result r = new Result();
      Stopwatch watch = Stopwatch.createStarted();
      try (final FlightStream stream = client.getStream(ticket)) {
        final VectorSchemaRoot root = stream.getRoot();
        try {
          BigIntVector a = (BigIntVector) root.getVector("a");
          while (stream.next()) {
            int rows = root.getRowCount();
            long aSum = r.aSum;
            for (int i = 0; i < rows; i++) {
              if (VALIDATE) {
                aSum += a.get(i);
              }
            }
            r.bytes += rows * 32L;
            r.rows += rows;
            r.aSum = aSum;
            r.batches++;
          }

          r.nanos = watch.elapsed(TimeUnit.NANOSECONDS);
          return r;
        } finally {
          root.clear();
        }
      }
    }
  }

  private static final class Result {
    private long rows;
    private long aSum;
    private long bytes;
    private long nanos;
    private long batches;

    public void add(Result r) {
      rows += r.rows;
      aSum += r.aSum;
      bytes += r.bytes;
      batches += r.batches;
      nanos = Math.max(nanos, r.nanos);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("rows", rows)
          .add("aSum", aSum)
          .add("batches", batches)
          .add("bytes", bytes)
          .add("nanos", nanos)
          .toString();
    }
  }
}
