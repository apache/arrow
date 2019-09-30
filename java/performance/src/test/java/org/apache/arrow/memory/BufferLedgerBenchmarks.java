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

package org.apache.arrow.memory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.netty.buffer.ArrowBuf;

/**
 * Benchmarks for {@link org.apache.arrow.memory.BufferLedger}.
 */
public class BufferLedgerBenchmarks {

  public static final int BUFFER_SIZE = 1024;

  public static final int NUM_BUFFERS_PER_TASK = 60;

  public static final int NUM_CONCURRENT_TASKS = 40;

  @State(Scope.Benchmark)
  public static class MultiThreadingCloseState {

    private ExecutorService threadPool;

    private BaseAllocator allocator;

    private CloseTask[] tasks = new CloseTask[NUM_CONCURRENT_TASKS];

    private CompletableFuture<Boolean>[] futures = new CompletableFuture[NUM_CONCURRENT_TASKS];

    @Setup(Level.Trial)
    public void prepareState() throws Exception {
      threadPool = Executors.newFixedThreadPool(NUM_CONCURRENT_TASKS);
      allocator = new RootAllocator(Integer.MAX_VALUE);
      for (int i = 0; i < NUM_CONCURRENT_TASKS; i++) {
        tasks[i] = new CloseTask(allocator);
      }
    }

    @Setup(Level.Invocation)
    public void prepareInvoke() throws Exception {
      for (int i = 0; i < NUM_CONCURRENT_TASKS; i++) {
        tasks[i].allocate();
        futures[i] = new CompletableFuture<>();
      }
    }

    @TearDown(Level.Trial)
    public void tearDownState() throws Exception {
      allocator.close();
      threadPool.shutdown();
    }
  }

  public static class CloseTask {

    private BufferAllocator allocator;

    private ArrowBuf[] buffers = new ArrowBuf[NUM_BUFFERS_PER_TASK];

    public CloseTask(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    public void allocate() {
      for (int i = 0; i < NUM_BUFFERS_PER_TASK; i++) {
        buffers[i] = allocator.buffer(BUFFER_SIZE);
      }
    }

    public void deallocate() {
      for (int i = 0; i < NUM_BUFFERS_PER_TASK; i++) {
        buffers[i].close();
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void concurrentCloseBenchmark(MultiThreadingCloseState state) throws Exception {
    for (int i = 0; i < NUM_CONCURRENT_TASKS; i++) {
      int tid = i;
      state.threadPool.submit(() -> {
        state.tasks[tid].deallocate();
        state.futures[tid].complete(true);
      });
    }
    CompletableFuture.allOf(state.futures).get();
  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(BufferLedgerBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
