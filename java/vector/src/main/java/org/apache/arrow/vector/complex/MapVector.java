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

package org.apache.arrow.vector.complex;

import static org.apache.arrow.util.Preconditions.checkArgument;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A MapVector is used to store entries of key/value pairs. It is a container vector that is
 * composed of a list of struct values with "key" and "value" fields. The MapVector is nullable,
 * but if a map is set at a given index, there must be an entry. In other words, the StructVector
 * data is non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can
 * be null.
 */
public class MapVector extends ListVector {

  public static final String KEY_NAME = "key";
  public static final String VALUE_NAME = "value";
  public static final String DATA_VECTOR_NAME = "entries";

  /**
   * Construct an empty MapVector with no data. Child vectors must be added subsequently.
   *
   * @param name The name of the vector.
   * @param allocator The allocator used for allocating/reallocating buffers.
   * @param keysSorted True if the map keys have been pre-sorted.
   * @return a new instance of MapVector.
   */
  public static MapVector empty(String name, BufferAllocator allocator, boolean keysSorted) {
    return new MapVector(name, allocator, FieldType.nullable(new Map(keysSorted)), null);
  }

  /**
   * Construct a MapVector instance.
   *
   * @param name The name of the vector.
   * @param allocator The allocator used for allocating/reallocating buffers.
   * @param fieldType The type definition of the MapVector.
   * @param callBack A schema change callback.
   */
  public MapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, fieldType, callBack);
    defaultDataVectorName = DATA_VECTOR_NAME;
  }

  /**
   * Initialize child vectors of the map from the given list of fields.
   *
   * @param children List of fields that will be children of this MapVector.
   */
  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    checkArgument(children.size() == 1, "Maps have one List child. Found: %s", children.isEmpty() ? "none" : children);

    Field structField = children.get(0);
    MinorType minorType = Types.getMinorTypeForArrowType(structField.getType());
    checkArgument(minorType == MinorType.STRUCT && !structField.isNullable(),
        "Map data should be a non-nullable struct type");
    checkArgument(structField.getChildren().size() == 2,
        "Map data should be a struct with 2 children. Found: %s", children);

    Field keyField = structField.getChildren().get(0);
    checkArgument(!keyField.isNullable(), "Map data key type should be a non-nullable");

    AddOrGetResult<FieldVector> addOrGetVector = addOrGetVector(structField.getFieldType());
    checkArgument(addOrGetVector.isCreated(), "Child vector already existed: %s", addOrGetVector.getVector());

    addOrGetVector.getVector().initializeChildrenFromFields(structField.getChildren());
  }

  /**
   * Get the writer for this MapVector instance.
   */
  @Override
  public UnionMapWriter getWriter() {
    return new UnionMapWriter(this);
  }

  /**
   * Get the reader for this MapVector instance.
   */
  @Override
  public UnionMapReader getReader() {
    if (reader == null) {
      reader = new UnionMapReader(this);
    }
    return (UnionMapReader) reader;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.MAP;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new TransferImpl(ref, allocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new MapVector.TransferImpl((MapVector) target);
  }

  private class TransferImpl implements TransferPair {

    MapVector to;
    TransferPair dataTransferPair;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      this(new MapVector(name, allocator, fieldType, callBack));
    }

    public TransferImpl(MapVector to) {
      this.to = to;
      to.addOrGetVector(vector.getField().getFieldType());
      if (to.getDataVector() instanceof ZeroVector) {
        to.addOrGetVector(vector.getField().getFieldType());
      }
      dataTransferPair = getDataVector().makeTransferPair(to.getDataVector());
    }

    /**
     * Transfer this vector'data to another vector. The memory associated
     * with this vector is transferred to the allocator of target vector
     * for accounting and management purposes.
     */
    @Override
    public void transfer() {
      to.clear();
      dataTransferPair.transfer();
      to.validityBuffer = transferBuffer(validityBuffer, to.allocator);
      to.offsetBuffer = transferBuffer(offsetBuffer, to.allocator);
      to.lastSet = lastSet;
      if (valueCount > 0) {
        to.setValueCount(valueCount);
      }
      clear();
    }

    /**
     * Slice this vector at desired index and length and transfer the
     * corresponding data to the target vector.
     * @param startIndex start position of the split in source vector.
     * @param length length of the split.
     */
    @Override
    public void splitAndTransfer(int startIndex, int length) {
      Preconditions.checkArgument(startIndex >= 0 && length >= 0 && startIndex + length <= valueCount,
              "Invalid parameters startIndex: %s, length: %s for valueCount: %s", startIndex, length, valueCount);
      final int startPoint = offsetBuffer.getInt(startIndex * OFFSET_WIDTH);
      final int sliceLength = offsetBuffer.getInt((startIndex + length) * OFFSET_WIDTH) - startPoint;
      to.clear();
      to.allocateOffsetBuffer((length + 1) * OFFSET_WIDTH);
      /* splitAndTransfer offset buffer */
      for (int i = 0; i < length + 1; i++) {
        final int relativeOffset = offsetBuffer.getInt((startIndex + i) * OFFSET_WIDTH) - startPoint;
        to.offsetBuffer.setInt(i * OFFSET_WIDTH, relativeOffset);
      }
      /* splitAndTransfer validity buffer */
      splitAndTransferValidityBuffer(startIndex, length, to);
      /* splitAndTransfer data buffer */
      dataTransferPair.splitAndTransfer(startPoint, sliceLength);
      to.lastSet = length - 1;
      to.setValueCount(length);
    }

    /*
     * transfer the validity.
     */
    private void splitAndTransferValidityBuffer(int startIndex, int length, MapVector target) {
      int firstByteSource = BitVectorHelper.byteIndex(startIndex);
      int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
      int byteSizeTarget = getValidityBufferSizeFromCount(length);
      int offset = startIndex % 8;

      if (length > 0) {
        if (offset == 0) {
          // slice
          if (target.validityBuffer != null) {
            target.validityBuffer.getReferenceManager().release();
          }
          target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
          target.validityBuffer.getReferenceManager().retain(1);
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

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, MapVector.this);
    }
  }
}
