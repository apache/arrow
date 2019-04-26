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

package org.apache.arrow.vector.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;

/**
 * Factory methods for vectors..
 */
public class VectorFactory {

  /**
   * The type of the vector.
   */
  public enum VectorType {
    /**
     * This type of vectors will do all checks, so segmentation faults can be avoided,
     * but the performance may not be good.
     */
    SAFE,

    /**
     * This type of vectors will try to avoid checks, so segmentation faults can happen,
     * but the performance can be be good.
     */
    UNSAFE
  }

  public static Float8Vector createFloat8Vector(VectorType vectorType, String name, BufferAllocator allocator) {
    switch (vectorType) {
      case SAFE:
        return new org.apache.arrow.vector.Float8Vector(name, allocator);
      case UNSAFE:
        return new org.apache.arrow.vector.unsafe.Float8Vector(name, allocator);
      default:
        throw new IllegalArgumentException("Unknown vector type for Float8Vector");
    }
  }
}
