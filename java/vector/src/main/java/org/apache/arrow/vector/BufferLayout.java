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

package org.apache.arrow.vector;

import org.apache.arrow.util.Preconditions;

/**
 * Metadata class that captures the "type" of an Arrow buffer.
 * (e.g. data buffers, offset buffers for variable width types and validity
 * buffers).
 */
public class BufferLayout {

  /**
   * Enumeration of the different logical types a buffer can have.
   * Data buffer is common to most of the layouts.
   * Offset buffer is used for variable width types.
   * Validity buffer is used for nullable types.
   * Type buffer is used for Union types.
   * Size buffer is used for ListView and LargeListView types.
   */
  public enum BufferType {
    DATA("DATA"),
    OFFSET("OFFSET"),
    VALIDITY("VALIDITY"),
    TYPE("TYPE_ID"),
    SIZE("SIZE");

    private final String name;

    BufferType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private static final BufferLayout VALIDITY_BUFFER = new BufferLayout(BufferType.VALIDITY, 1);
  private static final BufferLayout OFFSET_BUFFER = new BufferLayout(BufferType.OFFSET, 32);
  private static final BufferLayout LARGE_OFFSET_BUFFER = new BufferLayout(BufferType.OFFSET, 64);
  private static final BufferLayout TYPE_BUFFER = new BufferLayout(BufferType.TYPE, 32);
  private static final BufferLayout BIT_BUFFER = new BufferLayout(BufferType.DATA, 1);
  private static final BufferLayout VALUES_256 = new BufferLayout(BufferType.DATA, 256);
  private static final BufferLayout VALUES_128 = new BufferLayout(BufferType.DATA, 128);
  private static final BufferLayout VALUES_64 = new BufferLayout(BufferType.DATA, 64);
  private static final BufferLayout VALUES_32 = new BufferLayout(BufferType.DATA, 32);
  private static final BufferLayout VALUES_16 = new BufferLayout(BufferType.DATA, 16);
  private static final BufferLayout VALUES_8 = new BufferLayout(BufferType.DATA, 8);
  private static final BufferLayout SIZE_BUFFER = new BufferLayout(BufferType.SIZE, 32);

  public static BufferLayout typeBuffer() {
    return TYPE_BUFFER;
  }

  public static BufferLayout offsetBuffer() {
    return OFFSET_BUFFER;
  }

  public static BufferLayout largeOffsetBuffer() {
    return LARGE_OFFSET_BUFFER;
  }

  public static BufferLayout sizeBuffer() {
    return SIZE_BUFFER;
  }

  /**
   * Returns a databuffer for the given bitwidth.  Only supports powers of two between 8 and 128
   * inclusive.
   */
  public static BufferLayout dataBuffer(int typeBitWidth) {
    switch (typeBitWidth) {
      case 8:
        return VALUES_8;
      case 16:
        return VALUES_16;
      case 32:
        return VALUES_32;
      case 64:
        return VALUES_64;
      case 128:
        return VALUES_128;
      case 256:
        return VALUES_256;
      default:
        throw new IllegalArgumentException("only 8, 16, 32, 64, 128, or 256 bits supported");
    }
  }

  public static BufferLayout booleanVector() {
    return BIT_BUFFER;
  }

  public static BufferLayout validityVector() {
    return VALIDITY_BUFFER;
  }

  public static BufferLayout byteVector() {
    return dataBuffer(8);
  }

  private final short typeBitWidth;

  private final BufferType type;

  BufferLayout(BufferType type, int typeBitWidth) {
    super();
    this.type = Preconditions.checkNotNull(type);
    this.typeBitWidth = (short) typeBitWidth;
    if (typeBitWidth <= 0) {
      throw new IllegalArgumentException("bitWidth invalid: " + typeBitWidth);
    }
  }

  public int getTypeBitWidth() {
    return typeBitWidth;
  }

  public BufferType getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", type, typeBitWidth);
  }

  @Override
  public int hashCode() {
    return 31 * (31 + type.hashCode()) + typeBitWidth;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BufferLayout)) {
      return false;
    }
    BufferLayout other = (BufferLayout) obj;
    return type.equals(other.type) && (typeBitWidth == other.typeBitWidth);
  }
}
