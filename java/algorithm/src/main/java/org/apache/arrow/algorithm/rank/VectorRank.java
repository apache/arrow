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

package org.apache.arrow.algorithm.rank;

import java.util.stream.IntStream;

import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Utility for calculating ranks of vector elements.
 * @param <V> the vector type
 */
public class VectorRank<V extends ValueVector> {

  private VectorValueComparator<V> comparator;

  /**
   * Vector indices.
   */
  private IntVector indices;

  private final BufferAllocator allocator;

  /**
   * Constructs a vector rank utility.
   * @param allocator the allocator to use.
   */
  public VectorRank(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Given a rank r, gets the index of the element that is the rth smallest in the vector.
   * The operation is performed without changing the vector, and takes O(n) time,
   * where n is the length of the vector.
   * @param vector the vector from which to get the element index.
   * @param comparator the criteria for vector element comparison.
   * @param rank the rank to determine.
   * @return the element index with the given rank.
   */
  public int indexAtRank(V vector, VectorValueComparator<V> comparator, int rank) {
    Preconditions.checkArgument(rank >= 0 && rank < vector.getValueCount());
    try {
      indices = new IntVector("index vector", allocator);
      indices.allocateNew(vector.getValueCount());
      IntStream.range(0, vector.getValueCount()).forEach(i -> indices.set(i, i));

      comparator.attachVector(vector);
      this.comparator = comparator;

      int pos = getRank(0, vector.getValueCount() - 1, rank);
      return indices.get(pos);
    } finally {
      indices.close();
    }
  }

  private int getRank(int low, int high, int rank) {
    int mid = IndexSorter.partition(low, high, indices, comparator);
    if (mid < rank) {
      return getRank(mid + 1, high, rank);
    } else if (mid > rank) {
      return getRank(low, mid - 1, rank);
    } else {
      // mid == rank
      return mid;
    }
  }
}
