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

package org.apache.arrow.vector.complex;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ObjectArrays;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.impl.NullableMapReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

import javax.annotation.Nullable;

public class MapVector extends AbstractMapVector implements FieldVector {

  public static MapVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(Struct.INSTANCE);
    return new MapVector(name, allocator, fieldType, null);
  }

  private final NullableMapReaderImpl reader = new NullableMapReaderImpl(this);
  private final NullableMapWriter writer = new NullableMapWriter(this);

  protected ArrowBuf validityBuffer;
  private int validityAllocationSizeInBytes;
  protected final FieldType fieldType;
  public int valueCount;

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public MapVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), callBack);
  }

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public MapVector(String name, BufferAllocator allocator, DictionaryEncoding dictionary, CallBack callBack) {
    this(name, allocator, new FieldType(true, ArrowType.Struct.INSTANCE, dictionary, null), callBack);
  }

  public MapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, checkNotNull(allocator), callBack);
    this.fieldType = checkNotNull(fieldType);
    this.valueCount = 0;
    this.validityBuffer = allocator.getEmpty();
    this.validityAllocationSizeInBytes = BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION);
  }

  @Override
  public Types.MinorType getMinorType() {
    return Types.MinorType.MAP;
  }

  @Override
  public Field getField() {
    List<Field> children = new ArrayList<>();
    for (ValueVector child : getChildren()) {
      children.add(child.getField());
    }
    Field f = new Field(name, fieldType, children);
    FieldType type = new FieldType(true, f.getType(), f.getFieldType().getDictionary(), f.getFieldType().getMetadata());
    return new Field(f.getName(), type, f.getChildren());
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + 1 + ", got: " + ownBuffers.size());
    }

    ArrowBuf bitBuffer = ownBuffers.get(0);

    validityBuffer.release();
    validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    valueCount = fieldNode.getLength();
    validityAllocationSizeInBytes = validityBuffer.capacity();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(1);
    setReaderAndWriterIndex();
    result.add(validityBuffer);

    return result;
  }

  private void setReaderAndWriterIndex() {
    validityBuffer.readerIndex(0);
    validityBuffer.writerIndex(BitVectorHelper.getValidityBufferSize(valueCount));
  }

  @Override
  @Deprecated
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  @Override
  protected boolean supportsDirectRead() {
    return true;
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  public ValueVector getVectorById(int id) {
    return getChildByOrdinal(id);
  }

  @Override
  public NullableMapReaderImpl getReader() {
    return reader;
  }

  public NullableMapWriter getWriter() {
    return writer;
  }

  transient private NullableMapTransferPair ephPair;

  public void copyFromSafe(int fromIndex, int thisIndex, MapVector from) {
    if (ephPair == null || ephPair.from != from) {
      ephPair = (NullableMapTransferPair) from.makeTransferPair(this);
    }
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new NullableMapTransferPair(this, new MapVector(name, allocator, fieldType, null), false);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new NullableMapTransferPair(this, (MapVector) to, true);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new NullableMapTransferPair(this, new MapVector(ref, allocator, fieldType, null), false);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new NullableMapTransferPair(this, new MapVector(ref, allocator, fieldType, callBack), false);
  }

  protected class NullableMapTransferPair implements TransferPair {

    private final TransferPair[] pairs;
    private final MapVector from;
    private MapVector to;

    protected NullableMapTransferPair(MapVector from, MapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      // TODO: ?? this.to.ephPair = null;

      int i = 0;
      FieldVector vector;
      for (String child : from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        //DRILL-1872: we add the child fields for the vector, looking up the field by name. For a map vector,
        // the child fields may be nested fields of the top level child. For example if the structure
        // of a child field is oa.oab.oabc then we add oa, then add oab to oa then oabc to oab.
        // But the children member of a Materialized field is a HashSet. If the fields are added in the
        // children HashSet, and the hashCode of the Materialized field includes the hash code of the
        // children, the hashCode value of oa changes *after* the field has been added to the HashSet.
        // (This is similar to what happens in ScanBatch where the children cannot be added till they are
        // read). To take care of this, we ensure that the hashCode of the MaterializedField does not
        // include the hashCode of the children but is based only on MaterializedField$key.
        final FieldVector newVector = to.addOrGet(child, vector.getField().getFieldType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
      this.to = to;
    }

    @Override
    public void transfer() {
      to.clear();
      to.validityBuffer = validityBuffer.transferOwnership(to.allocator).buffer;
      for (final TransferPair p : pairs) {
        p.transfer();
      }
      to.valueCount = from.valueCount;
      from.clear();
      clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      while (toIndex >= to.getValidityBufferValueCapacity()) {
        to.reallocValidityBuffer();
      }
      BitVectorHelper.setValidityBit(to.validityBuffer, toIndex, isSet(fromIndex));
      for (TransferPair p : pairs) {
        p.copyValueSafe(fromIndex, toIndex);
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.clear();
      splitAndTransferValidityBuffer(startIndex, length, to);
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.setValueCount(length);
    }
  }

  /*
   * transfer the validity.
   */
  private void splitAndTransferValidityBuffer(int startIndex, int length, MapVector target) {
    assert startIndex + length <= valueCount;
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = BitVectorHelper.getValidityBufferSize(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        // slice
        if (target.validityBuffer != null) {
          target.validityBuffer.release();
        }
        target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
        target.validityBuffer.retain(1);
      } else {
        /* Copy data
         * When the first bit starts from the middle of a byte (offset != 0),
         * copy data from src BitVector.
         * Each byte in the target is composed by a part in i-th byte,
         * another part in (i+1)-th byte.
         */
        target.allocateValidityBuffer(byteSizeTarget);

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer, firstByteSource + i, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(validityBuffer, firstByteSource + i + 1, offset);

          target.validityBuffer.setByte(i, (b1 + b2));
        }

        /* Copying the last piece is done in the following manner:
         * if the source vector has 1 or more bytes remaining, we copy
         * the last piece as a byte formed by shifting data
         * from the current byte and the next byte.
         *
         * if the source vector has no more bytes remaining
         * (we are at the last byte), we copy the last piece as a byte
         * by shifting data from the current byte.
         */
        if ((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(validityBuffer,
                  firstByteSource + byteSizeTarget, offset);

          target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          target.validityBuffer.setByte(byteSizeTarget - 1, b1);
        }
      }
    }
  }

  /**
   * Get the value capacity of the internal validity buffer.
   * @return number of elements that validity buffer can hold
   */
  private int getValidityBufferValueCapacity() {
    return (int) (validityBuffer.capacity() * 8L);
  }

  /**
   * Get the current value capacity for the vector
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    if (size() == 0) {
      return 0;
    }

    final Ordering<ValueVector> natural = new Ordering<ValueVector>() {
      @Override
      public int compare(@Nullable ValueVector left, @Nullable ValueVector right) {
        return Ints.compare(
            checkNotNull(left).getValueCapacity(),
            checkNotNull(right).getValueCapacity()
        );
      }
    };

    return Math.min(getValidityBufferValueCapacity(),
        natural.min(getChildren()).getValueCapacity());
  }

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't
   * impact the reference counts for this buffer so it only should be used for in-context
   * access. Also note that this buffer changes regularly thus
   * external classes shouldn't hold a reference to it (unless they change it).
   *
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted
   *              but the returned array will be the only reference to them
   * @return The underlying {@link io.netty.buffer.ArrowBuf buffers} that is used by this
   *         vector instance.
   */
  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    setReaderAndWriterIndex();
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      buffers = ObjectArrays.concat(new ArrowBuf[]{validityBuffer}, super.getBuffers(false),
              ArrowBuf.class);
    }
    if (clear) {
      for (ArrowBuf buffer : buffers) {
        buffer.retain();
      }
      clear();
    }

    return buffers;
  }

  /**
   * Close the vector and release the associated buffers.
   */
  @Override
  public void close() {
    clearValidityBuffer();
    final Collection<FieldVector> vectors = getChildren();
    for (final FieldVector v : vectors) {
      v.close();
    }
    vectors.clear();
    valueCount = 0;
    super.close();
  }

  /**
   * Same as {@link #close()}
   */
  @Override
  public void clear() {
    clearValidityBuffer();
    for (final ValueVector v : getChildren()) {
      v.clear();
    }
    valueCount = 0;
  }

  /**
   * Release the validity buffer
   */
  private void clearValidityBuffer() {
    validityBuffer.release();
    validityBuffer = allocator.getEmpty();
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this
   * vector
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    if (valueCount == 0 || size() == 0) {
      return 0;
    }
    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSize();
    }

    return ((int) bufferSize) + BitVectorHelper.getValidityBufferSize(valueCount);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   * @param valueCount desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds
   *         a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return ((int) bufferSize) + BitVectorHelper.getValidityBufferSize(valueCount);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    validityAllocationSizeInBytes = BitVectorHelper.getValidityBufferSize(numRecords);
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      v.setInitialCapacity(numRecords);
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      clear();
      allocateValidityBuffer(validityAllocationSizeInBytes);
      success = super.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
        return false;
      }
    }
    return true;
  }

  private void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    /* reallocate the validity buffer */
    reallocValidityBuffer();
    super.reAlloc();
  }

  private void reallocValidityBuffer() {
    final int currentBufferCapacity = validityBuffer.capacity();
    long baseSize = validityAllocationSizeInBytes;

    if (baseSize < (long) currentBufferCapacity) {
      baseSize = (long) currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setZero(0, newBuf.capacity());
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    validityBuffer.release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
  }

  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }

  @Override
  public Object getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      Map<String, Object> vv = new JsonStringHashMap<>();
      for (String child : getChildFieldNames()) {
        ValueVector v = getChild(child);
        if (v != null && index < v.getValueCount()) {
          Object value = v.getObject(index);
          if (value != null) {
            vv.put(child, value);
          }
        }
      }
      return vv;
    }
  }

  public void get(int index, ComplexHolder holder) {
    holder.isSet = isSet(index);
    reader.setPosition(index);
    holder.reader = reader;
  }

  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  public boolean isNull(int index) {
    return isSet(index) == 0;
  }

  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return Long.bitCount(b & (1L << bitIndex));
  }

  public void setIndexDefined(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
  }

  public void setNull(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  @Override
  public void setValueCount(int valueCount) {
    assert valueCount >= 0;
    while (valueCount > getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    for (final ValueVector v : getChildren()) {
      v.setValueCount(valueCount);
    }
    this.valueCount = valueCount;
  }

  public void reset() {
    valueCount = 0;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    for (Field field : children) {
      FieldVector vector = (FieldVector) this.add(field.getName(), field.getFieldType());
      vector.initializeChildrenFromFields(field.getChildren());
    }
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return getChildren();
  }

  @Override
  @Deprecated
  public Accessor getAccessor() {
    throw new UnsupportedOperationException("Accessor is not supported for reading from Nullable MAP");
  }

  @Override
  @Deprecated
  public Mutator getMutator() {
    throw new UnsupportedOperationException("Mutator is not supported for writing to Nullable MAP");
  }
}
