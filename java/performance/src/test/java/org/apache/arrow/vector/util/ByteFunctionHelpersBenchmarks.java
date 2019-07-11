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

package org.apache.arrow.vector.util;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
 * Benchmarks for {@link ByteFunctionHelpers}.
 */
@State(Scope.Benchmark)
public class ByteFunctionHelpersBenchmarks {

  private static final int BUFFER_CAPACITY = 7;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private BufferAllocator allocator;

  private ArrowBuf buffer1;

  private ArrowBuf buffer2;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {
    allocator = new RootAllocator(ALLOCATOR_CAPACITY);
    buffer1 = allocator.buffer(BUFFER_CAPACITY);
    buffer2 = allocator.buffer(BUFFER_CAPACITY);

    for (int i = 0; i < BUFFER_CAPACITY; i++) {
      buffer1.setByte(i, i);
      buffer2.setByte(i, i);
    }
  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() {
    buffer1.close();
    buffer2.close();
    allocator.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void equals() {
    ByteFunctionHelpers.equal(buffer1, 0, BUFFER_CAPACITY - 1, buffer2, 0, BUFFER_CAPACITY - 1);

  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ByteFunctionHelpersBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
