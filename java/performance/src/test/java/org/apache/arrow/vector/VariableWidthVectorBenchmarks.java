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

package org.apache.arrow.vector;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
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

/**
 * Benchmarks for {@link BaseVariableWidthVector}.
 */
@State(Scope.Benchmark)
public class VariableWidthVectorBenchmarks {

  private static final int VECTOR_CAPACITY = 16 * 1024;

  private static final int VECTOR_LENGTH = 1024;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private static byte[] bytes = VariableWidthVectorBenchmarks.class.getName().getBytes();
  private ArrowBuf arrowBuff;

  private BufferAllocator allocator;

  private VarCharVector vector;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {
    allocator = new RootAllocator(ALLOCATOR_CAPACITY);
    vector = new VarCharVector("vector", allocator);
    vector.allocateNew(VECTOR_CAPACITY, VECTOR_LENGTH);
    arrowBuff = allocator.buffer(VECTOR_LENGTH);
    arrowBuff.setBytes(0, bytes, 0, bytes.length);
  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() {
    arrowBuff.close();
    vector.close();
    allocator.close();
  }

  /**
   * Test {@link BaseVariableWidthVector#getValueCapacity()}.
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int setSafeFromArray() {
    for (int i = 0; i < 500; ++i) {
      vector.setSafe(i * 40, bytes);
    }
    return vector.getBufferSize();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int setSafeFromNullableVarcharHolder() {
    NullableVarCharHolder nvch = new NullableVarCharHolder();
    nvch.buffer = arrowBuff;
    nvch.start = 0;
    nvch.end = bytes.length;
    for (int i = 0; i < 50; ++i) {
      nvch.isSet = 0;
      for (int j = 0; j < 9; ++j) {
        int idx = 10 * i + j;
        vector.setSafe(idx, nvch);
      }
      nvch.isSet = 1;
      vector.setSafe(10 * (i + 1), nvch);
    }
    return vector.getBufferSize();
  }


  public static void main(String [] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(VariableWidthVectorBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
