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
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
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

/**
 * Benchmarks for {@link BitVectorHelper}.
 */
public class BitVectorHelperBenchmarks {

  /**
   * State object for general benchmarks.
   */
  @State(Scope.Benchmark)
  public static class BenchmarkState {

    private static final int VALIDITY_BUFFER_CAPACITY = 1024;

    private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

    private BufferAllocator allocator;

    private ArrowBuf validityBuffer;

    private ArrowBuf oneBitValidityBuffer;

    /**
     * Setup benchmarks.
     */
    @Setup(Level.Trial)
    public void prepare() {
      allocator = new RootAllocator(ALLOCATOR_CAPACITY);
      validityBuffer = allocator.buffer(VALIDITY_BUFFER_CAPACITY / 8);

      for (int i = 0; i < VALIDITY_BUFFER_CAPACITY; i++) {
        if (i % 7 == 0) {
          BitVectorHelper.setBit(validityBuffer, i);
        } else {
          BitVectorHelper.unsetBit(validityBuffer, i);
        }
      }

      // only one 1 bit in the middle of the buffer
      oneBitValidityBuffer = allocator.buffer(VALIDITY_BUFFER_CAPACITY / 8);
      oneBitValidityBuffer.setZero(0, VALIDITY_BUFFER_CAPACITY / 8);
      BitVectorHelper.setBit(oneBitValidityBuffer, VALIDITY_BUFFER_CAPACITY / 2);
    }

    /**
     * Tear down benchmarks.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
      validityBuffer.close();
      oneBitValidityBuffer.close();
      allocator.close();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int getNullCountBenchmark(BenchmarkState state) {
    return BitVectorHelper.getNullCount(state.validityBuffer, BenchmarkState.VALIDITY_BUFFER_CAPACITY);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public boolean allBitsNullBenchmark(BenchmarkState state) {
    return BitVectorHelper.checkAllBitsEqualTo(
            state.oneBitValidityBuffer, BenchmarkState.VALIDITY_BUFFER_CAPACITY, true);
  }

  /**
   * State object for {@link #loadValidityBufferAllOne(NonNullableValidityBufferState)}..
   */
  @State(Scope.Benchmark)
  public static class NonNullableValidityBufferState {

    private static final int VALIDITY_BUFFER_CAPACITY = 1024;

    private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

    private BufferAllocator allocator;

    private ArrowBuf validityBuffer;

    private ArrowBuf loadResult;

    private ArrowFieldNode fieldNode;

    /**
     * Setup benchmarks.
     */
    @Setup(Level.Trial)
    public void prepare() {
      allocator = new RootAllocator(ALLOCATOR_CAPACITY);
      validityBuffer = allocator.buffer(VALIDITY_BUFFER_CAPACITY / 8);

      for (int i = 0; i < VALIDITY_BUFFER_CAPACITY; i++) {
        BitVectorHelper.setBit(validityBuffer, i);
      }

      fieldNode = new ArrowFieldNode(VALIDITY_BUFFER_CAPACITY, 0);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvoke() {
      loadResult.close();
    }

    /**
     * Tear down benchmarks.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
      validityBuffer.close();
      allocator.close();
    }
  }

  /**
   * Benchmark for {@link BitVectorHelper#loadValidityBuffer(ArrowFieldNode, ArrowBuf, BufferAllocator)}
   * when all elements are not null.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void loadValidityBufferAllOne(NonNullableValidityBufferState state) {
    state.loadResult = BitVectorHelper.loadValidityBuffer(state.fieldNode, state.validityBuffer, state.allocator);
  }

  /**
   * State object for {@link #setValidityBitBenchmark(ClearBitStateState)}.
   */
  @State(Scope.Benchmark)
  public static class ClearBitStateState {

    private static final int VALIDITY_BUFFER_CAPACITY = 1024;

    private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

    private BufferAllocator allocator;

    private ArrowBuf validityBuffer;

    private int bitToSet = 0;

    /**
     * Setup benchmarks.
     */
    @Setup(Level.Trial)
    public void prepare() {
      allocator = new RootAllocator(ALLOCATOR_CAPACITY);
      validityBuffer = allocator.buffer(VALIDITY_BUFFER_CAPACITY / 8);
    }

    /**
     * Tear down benchmarks.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
      validityBuffer.close();
      allocator.close();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setValidityBitBenchmark(ClearBitStateState state) {
    for (int i = 0; i < ClearBitStateState.VALIDITY_BUFFER_CAPACITY; i++) {
      BitVectorHelper.setValidityBit(state.validityBuffer, i, state.bitToSet);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setValidityBitToZeroBenchmark(ClearBitStateState state) {
    for (int i = 0; i < ClearBitStateState.VALIDITY_BUFFER_CAPACITY; i++) {
      BitVectorHelper.unsetBit(state.validityBuffer, i);
    }
  }

  public static void main(String [] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(BitVectorHelperBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
