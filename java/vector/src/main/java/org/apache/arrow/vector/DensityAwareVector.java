/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

/**
 * Vector that support density aware initial capacity settings.
 * We use this for ListVector and VarCharVector as of now to
 * control the memory allocated.
 *
 * For ListVector, we have been using a multiplier of 5
 * to compute the initial capacity of the inner data vector.
 * For deeply nested lists and lists with lots of NULL values,
 * this is over-allocation upfront. So density helps to be
 * conservative when computing the value capacity of the
 * inner vector.
 *
 * For example, a density value of 10 implies each position in the
 * list vector has a list of 10 values. So we will provision
 * an initial capacity of (valuecount * 10) for the inner vector.
 * A density value of 0.1 implies out of 10 positions in the list vector,
 * 1 position has a list of size 1 and remaining positions are
 * null (no lists) or empty lists. This helps in tightly controlling
 * the memory we provision for inner data vector.
 *
 * Similar analogy is applicable for VarCharVector where the capacity
 * of the data buffer can be controlled using density multiplier
 * instead of default multiplier of 8 (default size of average
 * varchar length).
 *
 * Also from container vectors, we propagate the density down
 * the inner vectors so that they can use it appropriately.
 */
public interface DensityAwareVector {

  /**
   * Set value with density
   * @param valueCount
   * @param density
   */
  void setInitialCapacity(int valueCount, double density);
}
