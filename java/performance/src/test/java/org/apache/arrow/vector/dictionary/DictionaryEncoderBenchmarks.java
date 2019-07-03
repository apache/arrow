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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
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

/**
 * Benchmarks for {@link DictionaryEncoder}.
 */
@State(Scope.Benchmark)
public class DictionaryEncoderBenchmarks {

  private BufferAllocator allocator;

  private static final int DATA_SIZE = 1000;
  private static final int KEY_SIZE = 100;


  private static final int KEY_LENGTH = 10;

  private List<String> keys = new ArrayList<>();

  private VarCharVector vector;

  private VarCharVector dictionaryVector;

  /**
   * Setup benchmarks.
   */
  @Setup
  public void prepare() {

    for (int i = 0; i < KEY_SIZE; i++) {
      keys.add(generateUniqueKey(KEY_LENGTH));
    }

    allocator = new RootAllocator(10 * 1024 * 1024);

    vector = new VarCharVector("vector", allocator);
    dictionaryVector = new VarCharVector("dict", allocator);

    vector.allocateNew(10240, DATA_SIZE);
    vector.setValueCount(DATA_SIZE);
    for (int i = 0; i < DATA_SIZE; i++) {
      byte[] value = keys.get(generateRandomIndex(KEY_SIZE)).getBytes(StandardCharsets.UTF_8);
      vector.setSafe(i, value, 0, value.length);
    }

    dictionaryVector.allocateNew(1024, 100);
    dictionaryVector.setValueCount(100);
    for (int i = 0; i < KEY_SIZE; i++) {
      byte[] value = keys.get(i).getBytes(StandardCharsets.UTF_8);
      dictionaryVector.setSafe(i, value, 0, value.length);
    }

  }

  /**
   * Tear down benchmarks.
   */
  @TearDown
  public void tearDown() {
    vector.close();
    dictionaryVector.close();
    keys.clear();
    allocator.close();
  }

  /**
   * Test encode for {@link DictionaryEncoder}.
   * @return useless. To avoid DCE by JIT.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public int testEncode() {
    Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
    final ValueVector encoded = DictionaryEncoder.encode(vector, dictionary);
    encoded.close();
    return 0;
  }

  private int generateRandomIndex(int max) {
    Random random = new Random();
    return random.nextInt(max);
  }

  private String generateUniqueKey(int length) {
    String str = "abcdefghijklmnopqrstuvwxyz";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int number = random.nextInt(26);
      sb.append(str.charAt(number));
    }
    if (keys.contains(sb.toString())) {
      return generateUniqueKey(length);
    }
    return sb.toString();
  }

  @Test
  public void evaluate() throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(DictionaryEncoderBenchmarks.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
