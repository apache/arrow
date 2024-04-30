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

package org.apache.arrow.algorithm.search;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for {@link ParallelSearcher}.
 */
public class ParallelSearcherBenchmarks {

  private static final int VECTOR_LENGTH = 1024 * 1024;

  /**
   * State object for the benchmarks.
   */
  @State(Scope.Benchmark)
  public static class SearchState {

    @Param({"1", "2", "5", "10", "20", "50", "100"})
    int numThreads;

    BufferAllocator allocator;

    ExecutorService threadPool;

    IntVector targetVector;

    IntVector keyVector;

    ParallelSearcher<IntVector> searcher;

    @Setup(Level.Trial)
    public void prepare() {
      allocator = new RootAllocator(Integer.MAX_VALUE);
      targetVector = new IntVector("target vector", allocator);
      targetVector.allocateNew(VECTOR_LENGTH);
      keyVector = new IntVector("key vector", allocator);
      keyVector.allocateNew(1);
      threadPool = Executors.newFixedThreadPool(numThreads);

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.set(i, i);
      }
      targetVector.setValueCount(VECTOR_LENGTH);

      keyVector.set(0, VECTOR_LENGTH / 3);
      keyVector.setValueCount(1);
    }

    @Setup(Level.Invocation)
    public void prepareInvoke() {
      searcher = new ParallelSearcher<>(targetVector, threadPool, numThreads);
    }

    @TearDown(Level.Trial)
    public void tearDownState() {
      targetVector.close();
      keyVector.close();
      allocator.close();
      threadPool.shutdown();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void searchBenchmark(SearchState state) throws Exception {
    state.searcher.search(state.keyVector, 0);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(ParallelSearcherBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
