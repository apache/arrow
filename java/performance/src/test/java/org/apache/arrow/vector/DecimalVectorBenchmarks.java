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

import java.math.BigDecimal;
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
 * Benchmarks for {@link DecimalVector}.
 */
@State(Scope.Benchmark)
public class DecimalVectorBenchmarks {

  private static final int VECTOR_LENGTH = 1024;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private BufferAllocator allocator;

  private DecimalVector vector;

  private ArrowBuf fromBuf;

  byte[] fromByteArray;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {
    allocator = new RootAllocator(ALLOCATOR_CAPACITY);
    vector = new DecimalVector("vector", allocator, 38, 16);
    vector.allocateNew(VECTOR_LENGTH);

    fromBuf = allocator.buffer(VECTOR_LENGTH * DecimalVector.TYPE_WIDTH);
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      byte[] bytes = BigDecimal.valueOf(i).unscaledValue().toByteArray();
      fromBuf.setBytes(i * DecimalVector.TYPE_WIDTH, bytes);
    }

    fromByteArray = new byte[DecimalVector.TYPE_WIDTH];
    fromBuf.getBytes(0, fromByteArray);
  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() {
    fromBuf.close();
    vector.close();
    allocator.close();
  }

  /**
   * Test writing on {@link DecimalVector} from arrow buf.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setBigEndianArrowBufBenchmark() {
    int offset = 0;

    for (int i = 0; i < VECTOR_LENGTH; i++) {
      vector.setBigEndianSafe(i, offset, fromBuf, DecimalVector.TYPE_WIDTH);
      offset += 8;
    }
  }

  /**
   * Test writing on {@link DecimalVector} from byte array.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setBigEndianByteArrayBenchmark() {
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      vector.setBigEndian(i, fromByteArray);
    }
  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(DecimalVectorBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
