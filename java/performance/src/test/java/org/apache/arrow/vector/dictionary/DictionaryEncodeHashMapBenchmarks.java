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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for {@link DictionaryEncodeHashMap}.
 */
@State(Scope.Benchmark)
public class DictionaryEncodeHashMapBenchmarks {
  private static final int SIZE = 1000;

  private static final int KEY_LENGTH = 10;

  private List<String> testData = new ArrayList<>();

  private HashMap<String, Integer> hashMap =  new HashMap();
  private DictionaryEncodeHashMap<String> dictionaryEncodeHashMap = new DictionaryEncodeHashMap();

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {
    for (int i = 0; i < SIZE; i++) {
      testData.add(getRandomString(KEY_LENGTH));
    }
  }

  private String getRandomString(int length) {
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(62);
      sb.append(str.charAt(number));
    }
    return sb.toString();
  }

  /**
   * Test set/get int values for {@link HashMap}.
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testHashMap() {
    for (int i = 0; i < SIZE; i++) {
      hashMap.put(testData.get(i), i);
    }
    for (int i = 0; i < SIZE; i++) {
      hashMap.get(testData.get(i));
    }
    return 0;
  }

  /**
   * Test set/get int values for {@link DictionaryEncodeHashMap}.
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testDictionaryEncodeHashMap() {
    for (int i = 0; i < SIZE; i++) {
      dictionaryEncodeHashMap.put(testData.get(i), i);
    }
    for (int i = 0; i < SIZE; i++) {
      dictionaryEncodeHashMap.get(testData.get(i));
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
