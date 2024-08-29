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

import java.util.Arrays;
import java.util.Base64;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.ReusableBuffer;

/**
 * A wrapper around byte arrays for repeated writing.
 */
public class ReusableByteArray implements ReusableBuffer<byte[]> {

  protected static final byte[] EMPTY_BYTES = new byte[0];

  protected byte[] bytes;
  protected int length;

  public ReusableByteArray() {
    bytes = EMPTY_BYTES;
  }

  public ReusableByteArray(byte[] data) {
    bytes = Arrays.copyOfRange(data, 0, data.length);
    length = data.length;
  }

  /**
   * Get the number of bytes in the byte array.
   *
   * @return the number of bytes in the byte array
   */
  @Override
  public long getLength() {
    return length;
  }

  @Override
  public byte[] getBuffer() {
    return bytes;
  }

  @Override
  public void set(ArrowBuf srcBytes, long start, long len) {
    setCapacity((int) len, false);
    srcBytes.getBytes(start, bytes, 0, (int) len);
    length = (int) len;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o == null) {
      return false;
    }
    if (!(o instanceof ReusableByteArray)) {
      return false;
    }

    final ReusableByteArray that = (ReusableByteArray) o;
    if (this.getLength() != that.getLength()) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      if (bytes[i] != that.bytes[i]) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    if (bytes == null) {
      return 0;
    }

    int result = 1;
    for (int i = 0; i < length; i++) {
      result = 31 * result + bytes[i];
    }

    return result;
  }

  @Override
  public String toString() {
    return Base64.getEncoder().encodeToString(Arrays.copyOfRange(bytes, 0, length));
  }

  /**
   * Sets the capacity of this object to <em>at least</em> <code>len</code> bytes. If the
   * current buffer is longer, then the capacity and existing content of the buffer are unchanged.
   * If <code>len</code> is larger than the current capacity, the Text object's capacity is
   * increased to match.
   *
   * @param len      the number of bytes we need
   * @param keepData should the old data be kept
   */
  protected void setCapacity(int len, boolean keepData) {
    if (bytes == null || bytes.length < len) {
      if (bytes != null && keepData) {
        bytes = Arrays.copyOf(bytes, Math.max(len, length << 1));
      } else {
        bytes = new byte[len];
      }
    }
  }
}
