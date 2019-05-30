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

import org.apache.arrow.util.Preconditions;

/**
 * The rounding policy that each buffer size must a multiple of the segment size.
 */
public class SegmentRoundingPolicy implements  RoundingPolicy {

  /**
   * The minimal segment size.
   */
  public static final long MIN_SEGMENT_SIZE = 1024L;

  /**
   * The segment size. It must be at least {@link SegmentRoundingPolicy#MIN_SEGMENT_SIZE},
   * and be a power of 2.
   */
  private int segmentSize;

  /**
   * Constructor for the segment rounding policy.
   * @param segmentSize the segment size.
   * @throws IllegalArgumentException if the segment size is smaller than
   * {@link SegmentRoundingPolicy#MIN_SEGMENT_SIZE}, or is not a power of 2.
   */
  public SegmentRoundingPolicy(int segmentSize) {
    Preconditions.checkArgument(segmentSize >= MIN_SEGMENT_SIZE,
            "The segment size cannot be smaller than " + MIN_SEGMENT_SIZE);
    Preconditions.checkArgument((segmentSize & (segmentSize - 1)) == 0,
            "The segment size must be a power of 2");
    this.segmentSize = segmentSize;
  }

  @Override
  public int getRoundedSize(int requestSize) {
    return (requestSize + (segmentSize - 1)) / segmentSize * segmentSize;
  }

  public int getSegmentSize() {
    return segmentSize;
  }
}
