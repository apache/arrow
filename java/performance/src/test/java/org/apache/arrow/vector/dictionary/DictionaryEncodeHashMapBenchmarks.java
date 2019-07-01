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

package org.apache.arrow.vector.dictionary;

import org.apache.arrow.memory.DictionaryEncodeHashMap;
import org.apache.arrow.vector.BaseValueVectorBenchmarks;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link org.apache.arrow.memory.DictionaryEncodeHashMap}.
 */
@State(Scope.Benchmark)
public class DictionaryEncodeHashMapBenchmarks {
  private static final int SIZE = 1000;

  private HashMap hashMap;
  private DictionaryEncodeHashMap dictionaryEncodeHashMap;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {
    hashMap = new HashMap();
    dictionaryEncodeHashMap = new DictionaryEncodeHashMap();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testHashMap() {
    for (int i = 0; i < SIZE; i++) {
      hashMap.put("test" + i, i);
    }
    for (int i = 0; i < SIZE; i++) {
      hashMap.get("test" + i);
    }
    return 0;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testDictionaryEncodeHashMap() {
    for (int i = 0; i < SIZE; i++) {
      dictionaryEncodeHashMap.put("test" + i, i);
    }
    for (int i = 0; i < SIZE; i++) {
      dictionaryEncodeHashMap.get("test" + i);
    }
    return 0;
  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(DictionaryEncodeHashMapBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
