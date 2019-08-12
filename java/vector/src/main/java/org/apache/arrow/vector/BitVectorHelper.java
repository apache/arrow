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

import static io.netty.util.internal.PlatformDependent.getByte;
import static io.netty.util.internal.PlatformDependent.getInt;
import static io.netty.util.internal.PlatformDependent.getLong;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.DataSizeRoundingUtil;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import io.netty.buffer.ArrowBuf;

/**
 * Helper class for performing generic operations on a bit vector buffer.
 * External use of this class is not recommended.
 */
public class BitVectorHelper {

  private BitVectorHelper() {}

  /**
   * Get the index of byte corresponding to bit index in validity buffer.
   */
  public static int byteIndex(int absoluteBitIndex) {
    return absoluteBitIndex >> 3;
  }

  /**
   * Get the relative index of bit within the byte in validity buffer.
   */
  public static int bitIndex(int absoluteBitIndex) {
    return absoluteBitIndex & 7;
  }

  /**
   * Set the bit at provided index to 1.
   *
   * @param validityBuffer validity buffer of the vector
   * @param index index to be set
   */
  public static void setValidityBitToOne(ArrowBuf validityBuffer, int index) {
    final int byteIndex = byteIndex(index);
    final int bitIndex = bitIndex(index);
    byte currentByte = validityBuffer.getByte(byteIndex);
    final byte bitMask = (byte) (1L << bitIndex);
    currentByte |= bitMask;
    validityBuffer.setByte(byteIndex, currentByte);
  }

  /**
   * Set the bit at a given index to provided value (1 or 0).
   *
   * @param validityBuffer validity buffer of the vector
   * @param index index to be set
   * @param value value to set
   */
  public static void setValidityBit(ArrowBuf validityBuffer, int index, int value) {
    final int byteIndex = byteIndex(index);
    final int bitIndex = bitIndex(index);
    byte currentByte = validityBuffer.getByte(byteIndex);
    final byte bitMask = (byte) (1L << bitIndex);
    if (value != 0) {
      currentByte |= bitMask;
    } else {
      currentByte -= (bitMask & currentByte);
    }
    validityBuffer.setByte(byteIndex, currentByte);
  }

  /**
   * Set the bit at a given index to provided value (1 or 0). Internally
   * takes care of allocating the buffer if the caller didn't do so.
   *
   * @param validityBuffer validity buffer of the vector
   * @param allocator allocator for the buffer
   * @param valueCount number of values to allocate/set
   * @param index index to be set
   * @param value value to set
   * @return ArrowBuf
   */
  public static ArrowBuf setValidityBit(ArrowBuf validityBuffer, BufferAllocator allocator,
                                        int valueCount, int index, int value) {
    if (validityBuffer == null) {
      validityBuffer = allocator.buffer(getValidityBufferSize(valueCount));
    }
    setValidityBit(validityBuffer, index, value);
    if (index == (valueCount - 1)) {
      validityBuffer.writerIndex(getValidityBufferSize(valueCount));
    }

    return validityBuffer;
  }

  /**
   * Check if a bit at a given index is set or not.
   *
   * @param buffer buffer to check
   * @param index index of the buffer
   * @return 1 if bit is set, 0 otherwise.
   */
  public static int get(final ArrowBuf buffer, int index) {
    final int byteIndex = index >> 3;
    final byte b = buffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  /**
   * Compute the size of validity buffer required to manage a given number
   * of elements in a vector.
   *
   * @param valueCount number of elements in the vector
   * @return buffer size
   */
  public static int getValidityBufferSize(int valueCount) {
    return DataSizeRoundingUtil.divideBy8Ceil(valueCount);
  }

  /**
   * Given a validity buffer, find the number of bits that are not set.
   * This is used to compute the number of null elements in a nullable vector.
   *
   * @param validityBuffer validity buffer of the vector
   * @param valueCount number of values in the vector
   * @return number of bits not set.
   */
  public static int getNullCount(final ArrowBuf validityBuffer, final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    int count = 0;
    final int sizeInBytes = getValidityBufferSize(valueCount);
    // If value count is not a multiple of 8, then calculate number of used bits in the last byte
    final int remainder = valueCount % 8;
    final int fullBytesCount = remainder == 0 ? sizeInBytes : sizeInBytes - 1;

    int index = 0;
    while (index + 8 <= fullBytesCount) {
      long longValue = validityBuffer.getLong(index);
      count += Long.bitCount(longValue);
      index += 8;
    }

    while (index + 4 <= fullBytesCount) {
      int intValue = validityBuffer.getInt(index);
      count += Integer.bitCount(intValue);
      index += 4;
    }

    while (index < fullBytesCount) {
      byte byteValue = validityBuffer.getByte(index);
      count += Integer.bitCount(byteValue & 0xFF);
      index += 1;
    }

    // handling with the last bits
    if (remainder != 0) {
      byte byteValue = validityBuffer.getByte(sizeInBytes - 1);

      // making the remaining bits all 1s if it is not fully filled
      byte mask = (byte) (0xFF << remainder);
      byteValue = (byte) (byteValue | mask);
      count += Integer.bitCount(byteValue & 0xFF);
    }

    return 8 * sizeInBytes - count;
  }

  /**
   * Tests if all bits in a validity buffer are equal 0 or 1, according to the specified parameter.
   * @param validityBuffer the validity buffer.
   * @param valueCount the bit count.
   * @param  checkOneBits if set to true, the method checks if all bits are equal to 1;
   *                      otherwise, it checks if all bits are equal to 0.
   * @return true if all bits are 0 or 1 according to the parameter, and false otherwise.
   */
  public static boolean checkAllBitsEqualTo(
          final ArrowBuf validityBuffer, final int valueCount, final boolean checkOneBits) {
    if (valueCount == 0) {
      return true;
    }
    final int sizeInBytes = getValidityBufferSize(valueCount);

    // boundary check
    validityBuffer.checkBytes(0, sizeInBytes);

    // If value count is not a multiple of 8, then calculate number of used bits in the last byte
    final int remainder = valueCount % 8;
    final int fullBytesCount = remainder == 0 ? sizeInBytes : sizeInBytes - 1;

    // the integer number to compare against
    final int intToCompare = checkOneBits ? -1 : 0;

    int index = 0;
    while (index + 8 <= fullBytesCount) {
      long longValue = getLong(validityBuffer.memoryAddress() + index);
      if (longValue != (long) intToCompare) {
        return false;
      }
      index += 8;
    }

    while (index + 4 <= fullBytesCount) {
      int intValue = getInt(validityBuffer.memoryAddress() + index);
      if (intValue != intToCompare) {
        return false;
      }
      index += 4;
    }

    while (index < fullBytesCount) {
      byte byteValue = getByte(validityBuffer.memoryAddress() + index);
      if (byteValue != (byte) intToCompare) {
        return false;
      }
      index += 1;
    }

    // handling with the last bits
    if (remainder != 0) {
      byte byteValue = getByte(validityBuffer.memoryAddress() + sizeInBytes - 1);
      byte mask = (byte) ((1 << remainder) - 1);
      byteValue = (byte) (byteValue & mask);
      if (checkOneBits) {
        if ((mask & byteValue) != mask) {
          return false;
        }
      } else {
        if (byteValue != (byte) 0) {
          return false;
        }
      }
    }
    return true;
  }

  /** Returns the byte at index from data right-shifted by offset. */
  public static byte getBitsFromCurrentByte(final ArrowBuf data, final int index, final int offset) {
    return (byte) ((data.getByte(index) & 0xFF) >>> offset);
  }

  /**
   * Returns the byte at <code>index</code> from left-shifted by (8 - <code>offset</code>).
   */
  public static byte getBitsFromNextByte(ArrowBuf data, int index, int offset) {
    return (byte) ((data.getByte(index) << (8 - offset)));
  }

  /**
   * Returns a new buffer if the source validity buffer is either all null or all
   * not-null, otherwise returns a buffer pointing to the same memory as source.
   *
   * @param fieldNode The fieldNode containing the null count
   * @param sourceValidityBuffer The source validity buffer that will have its
   *     position copied if there is a mix of null and non-null values
   * @param allocator The allocator to use for creating a new buffer if necessary.
   * @return A new buffer that is either allocated or points to the same memory as sourceValidityBuffer.
   */
  public static ArrowBuf loadValidityBuffer(final ArrowFieldNode fieldNode,
                                            final ArrowBuf sourceValidityBuffer,
                                            final BufferAllocator allocator) {
    final int valueCount = fieldNode.getLength();
    ArrowBuf newBuffer = null;
    /* either all NULLs or all non-NULLs */
    if (fieldNode.getNullCount() == 0 || fieldNode.getNullCount() == valueCount) {
      newBuffer = allocator.buffer(getValidityBufferSize(valueCount));
      newBuffer.setZero(0, newBuffer.capacity());
      if (fieldNode.getNullCount() != 0) {
        /* all NULLs */
        return newBuffer;
      }
      /* all non-NULLs */
      int fullBytesCount = valueCount / 8;
      for (int i = 0; i < fullBytesCount; ++i) {
        newBuffer.setByte(i, 0xFF);
      }
      int remainder = valueCount % 8;
      if (remainder > 0) {
        byte bitMask = (byte) (0xFFL >>> ((8 - remainder) & 7));
        newBuffer.setByte(fullBytesCount, bitMask);
      }
    } else {
      /* mixed byte pattern -- create another ArrowBuf associated with the
       * target allocator
       */
      newBuffer = sourceValidityBuffer.getReferenceManager().retain(sourceValidityBuffer, allocator);
    }

    return newBuffer;
  }

  /**
   * Set the byte of the given index in the data buffer by applying a bit mask to
   * the current byte at that index.
   *
   * @param data buffer to set
   * @param byteIndex byteIndex within the buffer
   * @param bitMask bit mask to be set
   */
  static void setBitMaskedByte(ArrowBuf data, int byteIndex, byte bitMask) {
    byte currentByte = data.getByte(byteIndex);
    currentByte |= bitMask;
    data.setByte(byteIndex, currentByte);
  }
}
