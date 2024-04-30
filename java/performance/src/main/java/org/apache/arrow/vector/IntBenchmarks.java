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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.impl.IntWriterImpl;
import org.apache.arrow.vector.holders.NullableIntHolder;
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
 * Benchmarks for {@link IntVector}.
 */
@State(Scope.Benchmark)
public class IntBenchmarks {

  private static final int VECTOR_LENGTH = 1024;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private BufferAllocator allocator;

  private IntVector vector;

  @Setup
  public void prepare() {
    allocator = new RootAllocator(ALLOCATOR_CAPACITY);
    vector = new IntVector("vector", allocator);
    vector.allocateNew(VECTOR_LENGTH);
    vector.setValueCount(VECTOR_LENGTH);
  }

  @TearDown
  public void tearDown() {
    vector.close();
    allocator.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setWithValueHolder() {
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      NullableIntHolder holder = new NullableIntHolder();
      holder.isSet = i % 3 == 0 ? 0 : 1;
      if (holder.isSet == 1) {
        holder.value = i;
      }
      vector.setSafe(i, holder);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setIntDirectly() {
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      vector.setSafe(i, i % 3 == 0 ? 0 : 1, i);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setWithWriter() {
    IntWriterImpl writer = new IntWriterImpl(vector);
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      if (i % 3 != 0) {
        writer.writeInt(i);
      }
    }
  }

  public static void main(String [] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(IntBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
