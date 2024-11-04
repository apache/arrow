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
import org.apache.arrow.vector.holders.NullableViewVarCharHolder;
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

/** Benchmarks for {@link BaseVariableWidthVector}. */
@State(Scope.Benchmark)
public class VariableWidthViewVectorBenchmarks {
  // checkstyle:off: MissingJavadocMethod

  private static final int VECTOR_CAPACITY = 16 * 1024;

  private static final int VECTOR_LENGTH = 1024;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private static byte[] bytes = VariableWidthVectorBenchmarks.class.getName().getBytes();
  private ArrowBuf arrowBuff;

  private BufferAllocator allocator;

  private ViewVarCharVector vector;

  @Param({"1", "2", "10", "40"})
  private int step;

  /** Setup benchmarks. */
  @Setup(Level.Iteration)
  public void prepare() {
    allocator = new RootAllocator();
    vector = new ViewVarCharVector("vector", allocator);
    vector.allocateNew(VECTOR_CAPACITY, VECTOR_LENGTH);
    arrowBuff = allocator.buffer(VECTOR_LENGTH);
    arrowBuff.setBytes(0, bytes, 0, bytes.length);
  }

  /** Tear down benchmarks. */
  @TearDown(Level.Iteration)
  public void tearDown() {
    arrowBuff.close();
    vector.close();
    allocator.close();
  }

  /**
   * Test {@link BaseVariableWidthVector#getValueCapacity()}.
   *
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
      vector.setSafe(i * step, bytes);
    }
    return vector.getBufferSize();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int setSafeFromNullableVarcharHolder() {
    NullableViewVarCharHolder nvch = new NullableViewVarCharHolder();
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

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(VariableWidthViewVectorBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
  // checkstyle:on: MissingJavadocMethod
}
