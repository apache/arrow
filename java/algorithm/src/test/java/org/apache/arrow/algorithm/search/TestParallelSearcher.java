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
package org.apache.arrow.algorithm.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test cases for {@link ParallelSearcher}. */
@RunWith(Parameterized.class)
public class TestParallelSearcher {

  private enum ComparatorType {
    EqualityComparator,
    OrderingComparator;
  }

  private static final int VECTOR_LENGTH = 10000;

  private final int threadCount;

  private BufferAllocator allocator;

  private ExecutorService threadPool;

  private final ComparatorType comparatorType;

  public TestParallelSearcher(ComparatorType comparatorType, int threadCount) {
    this.comparatorType = comparatorType;
    this.threadCount = threadCount;
  }

  @Parameterized.Parameters(name = "comparator type = {0}, thread count = {1}")
  public static Collection<Object[]> getComparatorName() {
    List<Object[]> params = new ArrayList<>();
    int[] threadCounts = {1, 2, 5, 10, 20, 50};
    for (ComparatorType type : ComparatorType.values()) {
      for (int count : threadCounts) {
        params.add(new Object[] {type, count});
      }
    }
    return params;
  }

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
    threadPool = Executors.newFixedThreadPool(threadCount);
  }

  @After
  public void shutdown() {
    allocator.close();
    threadPool.shutdown();
  }

  @Test
  public void testParallelIntSearch() throws ExecutionException, InterruptedException {
    try (IntVector targetVector = new IntVector("targetVector", allocator);
        IntVector keyVector = new IntVector("keyVector", allocator)) {
      targetVector.allocateNew(VECTOR_LENGTH);
      keyVector.allocateNew(VECTOR_LENGTH);

      // if we are comparing elements using equality semantics, we do not need a comparator here.
      VectorValueComparator<IntVector> comparator =
          comparatorType == ComparatorType.EqualityComparator
              ? null
              : DefaultVectorComparators.createDefaultComparator(targetVector);

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.set(i, i);
        keyVector.set(i, i * 2);
      }
      targetVector.setValueCount(VECTOR_LENGTH);
      keyVector.setValueCount(VECTOR_LENGTH);

      ParallelSearcher<IntVector> searcher =
          new ParallelSearcher<>(targetVector, threadPool, threadCount);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int pos =
            comparator == null
                ? searcher.search(keyVector, i)
                : searcher.search(keyVector, i, comparator);
        if (i * 2 < VECTOR_LENGTH) {
          assertEquals(i * 2, pos);
        } else {
          assertEquals(-1, pos);
        }
      }
    }
  }

  @Test
  public void testParallelStringSearch() throws ExecutionException, InterruptedException {
    try (VarCharVector targetVector = new VarCharVector("targetVector", allocator);
        VarCharVector keyVector = new VarCharVector("keyVector", allocator)) {
      targetVector.allocateNew(VECTOR_LENGTH);
      keyVector.allocateNew(VECTOR_LENGTH);

      // if we are comparing elements using equality semantics, we do not need a comparator here.
      VectorValueComparator<VarCharVector> comparator =
          comparatorType == ComparatorType.EqualityComparator
              ? null
              : DefaultVectorComparators.createDefaultComparator(targetVector);

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        keyVector.setSafe(i, String.valueOf(i * 2).getBytes(StandardCharsets.UTF_8));
      }
      targetVector.setValueCount(VECTOR_LENGTH);
      keyVector.setValueCount(VECTOR_LENGTH);

      ParallelSearcher<VarCharVector> searcher =
          new ParallelSearcher<>(targetVector, threadPool, threadCount);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int pos =
            comparator == null
                ? searcher.search(keyVector, i)
                : searcher.search(keyVector, i, comparator);
        if (i * 2 < VECTOR_LENGTH) {
          assertEquals(i * 2, pos);
        } else {
          assertEquals(-1, pos);
        }
      }
    }
  }
}
