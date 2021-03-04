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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link ParallelSearcher}.
 */
public class TestParallelSearcher {

  private static final int THREAD_COUNT = 10;

  private static final int VECTOR_LENGTH = 10000;

  private BufferAllocator allocator;

  private ExecutorService threadPool;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
    threadPool = Executors.newFixedThreadPool(THREAD_COUNT);
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

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.set(i, i);
        keyVector.set(i, i * 2);
      }
      targetVector.setValueCount(VECTOR_LENGTH);
      keyVector.setValueCount(VECTOR_LENGTH);

      ParallelSearcher<IntVector> searcher = new ParallelSearcher<>(targetVector, threadPool, THREAD_COUNT);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int pos = searcher.search(keyVector, i);
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

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.setSafe(i, String.valueOf(i).getBytes());
        keyVector.setSafe(i, String.valueOf(i * 2).getBytes());
      }
      targetVector.setValueCount(VECTOR_LENGTH);
      keyVector.setValueCount(VECTOR_LENGTH);

      ParallelSearcher<VarCharVector> searcher = new ParallelSearcher<>(targetVector, threadPool, THREAD_COUNT);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int pos = searcher.search(keyVector, i);
        if (i * 2 < VECTOR_LENGTH) {
          assertEquals(i * 2, pos);
        } else {
          assertEquals(-1, pos);
        }
      }
    }
  }
}
