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
import org.apache.arrow.vector.compare.ApproxEqualsVisitor;
import org.apache.arrow.vector.compare.Range;
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
 * Benchmarks for floating point vectors.
 */
@State(Scope.Benchmark)
public class FloatingPointBenchmarks {

  private static final int VECTOR_LENGTH = 1024;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private BufferAllocator allocator;

  private Float4Vector floatVector1;

  private Float4Vector floatVector2;

  private Float8Vector doubleVector1;

  private Float8Vector doubleVector2;

  private ApproxEqualsVisitor floatVisitor;

  private ApproxEqualsVisitor doubleVisitor;

  private Range range;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {
    allocator = new RootAllocator(ALLOCATOR_CAPACITY);
    floatVector1 = new Float4Vector("vector", allocator);
    floatVector2 = new Float4Vector("vector", allocator);
    doubleVector1 = new Float8Vector("vector", allocator);
    doubleVector2 = new Float8Vector("vector", allocator);

    floatVector1.allocateNew(VECTOR_LENGTH);
    floatVector2.allocateNew(VECTOR_LENGTH);
    doubleVector1.allocateNew(VECTOR_LENGTH);
    doubleVector2.allocateNew(VECTOR_LENGTH);

    for (int i = 0; i < VECTOR_LENGTH; i++) {
      if (i % 3 == 0) {
        floatVector1.setNull(i);
        floatVector2.setNull(i);
        doubleVector1.setNull(i);
        doubleVector2.setNull(i);
      } else {
        floatVector1.set(i, i * i);
        floatVector2.set(i, i * i);
        doubleVector1.set(i, i * i);
        doubleVector2.set(i, i * i);
      }
    }
    floatVector1.setValueCount(VECTOR_LENGTH);
    floatVector2.setValueCount(VECTOR_LENGTH);
    doubleVector1.setValueCount(VECTOR_LENGTH);
    doubleVector2.setValueCount(VECTOR_LENGTH);

    floatVisitor = new ApproxEqualsVisitor(floatVector1, floatVector2, 0.01f, 0.01);
    doubleVisitor = new ApproxEqualsVisitor(doubleVector1, doubleVector2, 0.01f, 0.01);
    range = new Range(0, 0, VECTOR_LENGTH);
  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() {
    floatVector1.close();
    floatVector2.close();
    doubleVector1.close();
    doubleVector2.close();
    allocator.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int approxEqualsBenchmark() {
    boolean floatResult = floatVisitor.visit(floatVector1, range);
    boolean doubleResult = doubleVisitor.visit(doubleVector1, range);
    return (floatResult ? 1 : 0) + (doubleResult ? 1 : 0);
  }

  public static void main(String [] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(FloatingPointBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}

