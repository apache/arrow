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

import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.DataSizeRoundingUtil;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Vector that contains pointers to a set of binary data.
 */
public class BinaryPointerVector extends FixedSizeBinaryVector {

  public static final int LENGTH_WIDTH = 4;

  public static final int ADDRESS_WIDTH = 8;

  /**
   * Type width for the pointer.
   * 8 bytes for the address, plus 4 bytes for the length.
   */
  public static final int TYPE_WIDTH = ADDRESS_WIDTH + LENGTH_WIDTH;

  /**
   * The key in the field type meta-data to indicate that the {@link FixedSizeBinaryVector} vector is a
   * {@link BinaryPointerVector}.
   */
  public static final String BINARY_POINTER_TYPE = "binary.pointer.type";

  public static Map<String, String> META_DATA = new HashMap<>();

  static {
    META_DATA.put(BINARY_POINTER_TYPE, "true");
  }

  /**
   * Check if the given field type is the type for binary pointer vector.
   * @param fieldType the field type to check.
   * @return true if the field type is for binary pointer vector, and false otherwise.
   */
  public static boolean isBinaryPointerType(FieldType fieldType) {
    if (!(fieldType.getType() instanceof ArrowType.FixedSizeBinary)) {
      return false;
    }
    if (((ArrowType.FixedSizeBinary) fieldType.getType()).getByteWidth() != TYPE_WIDTH) {
      return false;
    }
    Map<String, String> metaData = fieldType.getMetadata();
    if (metaData == null) {
      return false;
    }
    if (metaData.get(BINARY_POINTER_TYPE) == null || !metaData.get(BINARY_POINTER_TYPE).equals("true")) {
      return false;
    }
    return true;
  }

  /**
   * Constructs a new binary pointer vector.
   * @param field field of the vector.
   * @param allocator allocator for the vector.
   */
  public BinaryPointerVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
  }

  /**
   * Constructs a new binary pointer vector.
   * @param name name of the vector.
   * @param allocator allocator for the vector.
   */
  public BinaryPointerVector(String name, BufferAllocator allocator) {
    super(name, new FieldType(true, new ArrowType.FixedSizeBinary(TYPE_WIDTH), null, META_DATA), allocator);
  }

  /**
   * Convert this vector to a {@link VarBinaryVector} by consolidating
   * the data to a continuous memory region.
   * Note this method may have performance overhead, so please use it with caution.
   * @param varBinaryVector the converted {@link VarBinaryVector}.
   */
  public void toVarBinaryVector(VarBinaryVector varBinaryVector) {
    // copy validity buffer
    varBinaryVector.handleSafe(valueCount, getTotalLength());

    int validityBufferSize = DataSizeRoundingUtil.divideBy8Ceil(valueCount);
    PlatformDependent.copyMemory(
            validityBuffer.memoryAddress(),
            varBinaryVector.validityBuffer.memoryAddress(),
            validityBufferSize);

    int offset = 0;
    varBinaryVector.offsetBuffer.setInt(0, offset);

    for (int i = 0; i < valueCount; i++) {
      if (!isNull(i)) {
        int length = getLength(i);
        PlatformDependent.copyMemory(
                getAddress(i),
                varBinaryVector.valueBuffer.memoryAddress() + offset,
                length);
        offset += length;
      }
      varBinaryVector.offsetBuffer.setInt(i + 1, offset);
    }
    varBinaryVector.setLastSet(valueCount - 1);
    varBinaryVector.setValueCount(valueCount);
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    throw new IllegalStateException(
            "BinaryPointerVector should not be sent through IPC, " +
                    "please convert it to a VarBinaryVector before sending");
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets the address of the data at the given index from the vector.
   *
   * @param index position of element
   * @return address of the data at given index
   */
  public long getAddress(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getLong(index * TYPE_WIDTH);
  }

  /**
   * Gets the length of the data at the given index from the vector.
   *
   * @param index position of element
   * @return length of the data at given index
   */
  public int getLength(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getInt(index * TYPE_WIDTH + ADDRESS_WIDTH);
  }

  /**
   * Sets the address at the given index.
   * @param index position of element.
   * @param address address to set.
   */
  public void setAddress(int index, long address) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setLong(index * TYPE_WIDTH, address);
  }

  /**
   * Sets the length of the element at the given index.
   * @param index position of element
   * @param length length of element
   */
  public void setLength(int index, int length) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setInt(index * TYPE_WIDTH + ADDRESS_WIDTH, length);
  }

  /**
   * Same as {@link #setAddress(int, long)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param address address of element
   */
  public void setAddressSafe(int index, long address) {
    handleSafe(index);
    setAddress(index, address);
  }

  /**
   * Same as {@link #setLength(int, int)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param length length of element
   */
  public void setLengthSafe(int index, int length) {
    handleSafe(index);
    setLength(index, length);
  }

  /**
   * Gets the data pointed to by the pointer at the given index.
   *
   * @param index   position of element to get
   * @return array of bytes for non-null data, or null otherwise.
   */
  public byte[] get(int index) {
    assert index >= 0;
    if (isSet(index) == 0) {
      return null;
    }
    final long address = getAddress(index);
    final int length = getLength(index);
    final byte[] result = new byte[length];

    PlatformDependent.copyMemory(address, result, 0, length);
    return result;
  }

  /**
   * Get the object pointed to by the pointer at the given index.
   *
   * @param index   position of element to get
   * @return byte array object for non-null element, null otherwise
   */
  public byte[] getObject(int index) {
    return get(index);
  }

  /**
   * Get the variable length element pointed to..
   *
   * @param index   position of element to get
   * @param holder  data holder to be populated by this function
   */
  public void get(int index, NullableBinaryPointerHolder holder) {
    assert index >= 0;
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.address = getAddress(index);
    holder.length = getLength(index);
  }

  /**
   * Sets the element pointer at the given index.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, NullableBinaryPointerHolder holder) {
    assert index >= 0;
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    if (holder.isSet != 0) {
      setAddress(index, holder.address);
      setLength(index, holder.length);
    }
  }

  /**
   * Sets the element pointer at the given index.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, BinaryPointerHolder holder) {
    assert index >= 0;
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setAddress(index, holder.address);
    setLength(index, holder.length);
  }

  /**
   * Same as {@link #set(int, NullableBinaryPointerHolder)} except that it handles the
   * case where index of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, NullableBinaryPointerHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, BinaryPointerHolder)} except that it handles the
   * case where index of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, BinaryPointerHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Gets the total length of all data.
   * @return the total length of all data.
   */
  public int getTotalLength() {
    int sum = 0;
    for (int i = 0; i < valueCount; i++) {
      if (!isNull(i)) {
        sum += getLength(i);
      }
    }
    return sum;
  }

  /**
   * Nullable value holder for the binary pointer.
   */
  public static class NullableBinaryPointerHolder {
    public int isSet;
    public long address;
    public int length;
  }

  /**
   * Value holder for the binary pointer.
   */
  public static class BinaryPointerHolder {
    public long address;
    public int length;
  }
}
