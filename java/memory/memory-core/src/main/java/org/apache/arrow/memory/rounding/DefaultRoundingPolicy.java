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

package org.apache.arrow.memory.rounding;

import org.apache.arrow.memory.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default rounding policy. That is, if the requested size is within the chunk size,
 * the rounded size will be the next power of two. Otherwise, the rounded size
 * will be identical to the requested size.
 */
public class DefaultRoundingPolicy implements RoundingPolicy {
  private static final Logger logger = LoggerFactory.getLogger(DefaultRoundingPolicy.class);
  public final long chunkSize;

  /**
   * The variables here and the static block calculates the DEFAULT_CHUNK_SIZE.
   *
   * <p>
   *   It was copied from {@link io.netty.buffer.PooledByteBufAllocator}.
   * </p>
   */
  private static final int MIN_PAGE_SIZE = 4096;
  private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);
  private static final long DEFAULT_CHUNK_SIZE;


  static {
    int defaultPageSize = Integer.getInteger("org.apache.memory.allocator.pageSize", 8192);
    Throwable pageSizeFallbackCause = null;
    try {
      validateAndCalculatePageShifts(defaultPageSize);
    } catch (Throwable t) {
      pageSizeFallbackCause = t;
      defaultPageSize = 8192;
    }

    int defaultMaxOrder = Integer.getInteger("org.apache.memory.allocator.maxOrder", 11);
    Throwable maxOrderFallbackCause = null;
    try {
      validateAndCalculateChunkSize(defaultPageSize, defaultMaxOrder);
    } catch (Throwable t) {
      maxOrderFallbackCause = t;
      defaultMaxOrder = 11;
    }
    DEFAULT_CHUNK_SIZE = validateAndCalculateChunkSize(defaultPageSize, defaultMaxOrder);
    if (logger.isDebugEnabled()) {
      logger.debug("-Dorg.apache.memory.allocator.pageSize: {}", defaultPageSize);
      logger.debug("-Dorg.apache.memory.allocator.maxOrder: {}", defaultMaxOrder);
    }
  }

  private static int validateAndCalculatePageShifts(int pageSize) {
    if (pageSize < MIN_PAGE_SIZE) {
      throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ")");
    }

    if ((pageSize & pageSize - 1) != 0) {
      throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
    }

    // Logarithm base 2. At this point we know that pageSize is a power of two.
    return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
  }

  private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
    if (maxOrder > 14) {
      throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
    }

    // Ensure the resulting chunkSize does not overflow.
    int chunkSize = pageSize;
    for (int i = maxOrder; i > 0; i --) {
      if (chunkSize > MAX_CHUNK_SIZE / 2) {
        throw new IllegalArgumentException(String.format(
            "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
      }
      chunkSize <<= 1;
    }
    return chunkSize;
  }

  /**
   * The singleton instance.
   */
  public static final DefaultRoundingPolicy DEFAULT_ROUNDING_POLICY = new DefaultRoundingPolicy(DEFAULT_CHUNK_SIZE);

  private DefaultRoundingPolicy(long chunkSize) {
    this.chunkSize = chunkSize;
  }

  @Override
  public long getRoundedSize(long requestSize) {
    return requestSize < chunkSize ?
            CommonUtil.nextPowerOfTwo(requestSize) : requestSize;
  }
}
