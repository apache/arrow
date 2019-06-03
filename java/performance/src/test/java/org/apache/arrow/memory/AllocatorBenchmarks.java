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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.memory.rounding.SegmentRoundingPolicy;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.netty.buffer.ArrowBuf;

/**
 * Benchmarks for allocators.
 */
public class AllocatorBenchmarks {

  /**
   * Benchmark for the default allocator.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void defaultAllocatorBenchmark() {
    final int bufferSize = 1024;
    final int numBuffers = 1024;

    try (RootAllocator allocator = new RootAllocator(numBuffers * bufferSize)) {
      ArrowBuf[] buffers = new ArrowBuf[numBuffers];

      for (int i = 0; i < numBuffers; i++) {
        buffers[i] = allocator.buffer(bufferSize);
      }

      for (int i = 0; i < numBuffers; i++) {
        buffers[i].close();
      }
    }
  }

  /**
   * Benchmark for allocator with segment rounding policy.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void segmentRoundingPolicyBenchmark() {
    final int bufferSize = 1024;
    final int numBuffers = 1024;
    final int segmentSize = 1024;

    RoundingPolicy policy = new SegmentRoundingPolicy(segmentSize);
    try (RootAllocator allocator = new RootAllocator(AllocationListener.NOOP, bufferSize * numBuffers, policy)) {
      ArrowBuf[] buffers = new ArrowBuf[numBuffers];

      for (int i = 0; i < numBuffers; i++) {
        buffers[i] = allocator.buffer(bufferSize);
      }

      for (int i = 0; i < numBuffers; i++) {
        buffers[i].close();
      }
    }
  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(AllocatorBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
