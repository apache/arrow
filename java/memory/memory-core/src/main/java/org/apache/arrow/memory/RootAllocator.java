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
package org.apache.arrow.memory;

import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.util.VisibleForTesting;

/**
 * A root allocator for using direct memory for Arrow Vectors/Arrays. Supports creating a tree of
 * descendant child allocators to facilitate better instrumentation of memory allocations.
 */
public class RootAllocator extends BaseAllocator {

  public RootAllocator() {
    this(AllocationListener.NOOP, Long.MAX_VALUE);
  }

  public RootAllocator(final long limit) {
    this(AllocationListener.NOOP, limit);
  }

  public RootAllocator(final AllocationListener listener, final long limit) {
    // todo fix DefaultRoundingPolicy when using Netty
    this(listener, limit, DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY);
  }

  /**
   * Constructor.
   *
   * @param listener the allocation listener
   * @param limit max allocation size in bytes
   * @param roundingPolicy the policy for rounding the buffer size
   */
  public RootAllocator(
      final AllocationListener listener, final long limit, RoundingPolicy roundingPolicy) {
    this(
        configBuilder()
            .listener(listener)
            .maxAllocation(limit)
            .roundingPolicy(roundingPolicy)
            .build());
  }

  public RootAllocator(Config config) {
    super(null, "ROOT", config);
  }

  /** Verify the accounting state of the allocation system. */
  @VisibleForTesting
  public void verify() {
    verifyAllocator();
  }
}
