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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;

/**
 * Search for a value in the vector by multiple threads. This is often used in scenarios where the
 * vector is large or low response time is required.
 *
 * @param <V> the vector type.
 */
public class ParallelSearcher<V extends ValueVector> {

  /** The target vector to search. */
  private final V vector;

  /** The thread pool. */
  private final ExecutorService threadPool;

  /** The number of threads to use. */
  private final int numThreads;

  /** The position of the key in the target vector, if any. */
  private volatile int keyPosition = -1;

  /**
   * Constructs a parallel searcher.
   *
   * @param vector the vector to search.
   * @param threadPool the thread pool to use.
   * @param numThreads the number of threads to use.
   */
  public ParallelSearcher(V vector, ExecutorService threadPool, int numThreads) {
    this.vector = vector;
    this.threadPool = threadPool;
    this.numThreads = numThreads;
  }

  private CompletableFuture<Boolean>[] initSearch() {
    keyPosition = -1;
    final CompletableFuture<Boolean>[] futures = new CompletableFuture[numThreads];
    for (int i = 0; i < futures.length; i++) {
      futures[i] = new CompletableFuture<>();
    }
    return futures;
  }

  /**
   * Search for the key in the target vector. The element-wise comparison is based on {@link
   * RangeEqualsVisitor}, so there are two possible results for each element-wise comparison: equal
   * and un-equal.
   *
   * @param keyVector the vector containing the search key.
   * @param keyIndex the index of the search key in the key vector.
   * @return the position of a matched value in the target vector, or -1 if none is found. Please
   *     note that if there are multiple matches of the key in the target vector, this method makes
   *     no guarantees about which instance is returned. For an alternative search implementation
   *     that always finds the first match of the key, see {@link
   *     VectorSearcher#linearSearch(ValueVector, VectorValueComparator, ValueVector, int)}.
   * @throws ExecutionException if an exception occurs in a thread.
   * @throws InterruptedException if a thread is interrupted.
   */
  public int search(V keyVector, int keyIndex) throws ExecutionException, InterruptedException {
    final CompletableFuture<Boolean>[] futures = initSearch();
    final int valueCount = vector.getValueCount();
    for (int i = 0; i < numThreads; i++) {
      final int tid = i;
      Future<?> unused =
          threadPool.submit(
              () -> {
                // convert to long to avoid overflow
                int start = (int) (((long) valueCount) * tid / numThreads);
                int end = (int) ((long) valueCount) * (tid + 1) / numThreads;

                if (start >= end) {
                  // no data assigned to this task.
                  futures[tid].complete(false);
                  return;
                }

                RangeEqualsVisitor visitor = new RangeEqualsVisitor(vector, keyVector, null);
                Range range = new Range(0, 0, 1);
                for (int pos = start; pos < end; pos++) {
                  if (keyPosition != -1) {
                    // the key has been found by another task
                    futures[tid].complete(false);
                    return;
                  }
                  range.setLeftStart(pos).setRightStart(keyIndex);
                  if (visitor.rangeEquals(range)) {
                    keyPosition = pos;
                    futures[tid].complete(true);
                    return;
                  }
                }

                // no match value is found.
                futures[tid].complete(false);
              });
    }

    CompletableFuture.allOf(futures).get();
    return keyPosition;
  }

  /**
   * Search for the key in the target vector. The element-wise comparison is based on {@link
   * VectorValueComparator}, so there are three possible results for each element-wise comparison:
   * less than, equal to and greater than.
   *
   * @param keyVector the vector containing the search key.
   * @param keyIndex the index of the search key in the key vector.
   * @param comparator the comparator for comparing the key against vector elements.
   * @return the position of a matched value in the target vector, or -1 if none is found. Please
   *     note that if there are multiple matches of the key in the target vector, this method makes
   *     no guarantees about which instance is returned. For an alternative search implementation
   *     that always finds the first match of the key, see {@link
   *     VectorSearcher#linearSearch(ValueVector, VectorValueComparator, ValueVector, int)}.
   * @throws ExecutionException if an exception occurs in a thread.
   * @throws InterruptedException if a thread is interrupted.
   */
  public int search(V keyVector, int keyIndex, VectorValueComparator<V> comparator)
      throws ExecutionException, InterruptedException {
    final CompletableFuture<Boolean>[] futures = initSearch();
    final int valueCount = vector.getValueCount();
    for (int i = 0; i < numThreads; i++) {
      final int tid = i;
      Future<?> unused =
          threadPool.submit(
              () -> {
                // convert to long to avoid overflow
                int start = (int) (((long) valueCount) * tid / numThreads);
                int end = (int) ((long) valueCount) * (tid + 1) / numThreads;

                if (start >= end) {
                  // no data assigned to this task.
                  futures[tid].complete(false);
                  return;
                }

                VectorValueComparator<V> localComparator = comparator.createNew();
                localComparator.attachVectors(vector, keyVector);
                for (int pos = start; pos < end; pos++) {
                  if (keyPosition != -1) {
                    // the key has been found by another task
                    futures[tid].complete(false);
                    return;
                  }
                  if (localComparator.compare(pos, keyIndex) == 0) {
                    keyPosition = pos;
                    futures[tid].complete(true);
                    return;
                  }
                }

                // no match value is found.
                futures[tid].complete(false);
              });
    }

    CompletableFuture.allOf(futures).get();
    return keyPosition;
  }
}
