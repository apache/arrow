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

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.DATA_VECTOR_NAME;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.NullReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A null type vector.
 */
public class NullVector implements FieldVector {

  private int valueCount;

  protected Field field;

  /**
   * Instantiate a NullVector.
   *
   * @param name name of the vector
   */
  public NullVector(String name) {
    this(name, FieldType.nullable(Types.MinorType.NULL.getType()));
  }

  /**
   * Instantiate a NullVector.
   *
   * @param name      name of the vector
   * @param fieldType type of Field materialized by this vector.
   */
  public NullVector(String name, FieldType fieldType) {
    this(new Field(name, fieldType, null));
  }

  /**
   * Instantiate a NullVector.
   *
   * @param field field materialized by this vector.
   */
  public NullVector(Field field) {
    this.valueCount = 0;
    this.field = field;
  }

  @Deprecated
  public NullVector() {
    this(new Field(DATA_VECTOR_NAME, FieldType.nullable(new ArrowType.Null()), null));
  }

  @Override
  public void close() {
  }

  @Override
  public void clear() {
  }

  @Override
  public void reset() {
  }

  @Override
  public Field getField() {
    return field;
  }

  @Override
  public Types.MinorType getMinorType() {
    return Types.MinorType.NULL;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(null, allocator);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    return 0;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return new ArrowBuf[0];
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    allocateNewSafe();
  }

  @Override
  public boolean allocateNewSafe() {
    return true;
  }

  @Override
  public void reAlloc() {
  }

  @Override
  public BufferAllocator getAllocator() {
    throw new UnsupportedOperationException("Tried to get allocator from NullVector");
  }

  @Override
  public void setInitialCapacity(int numRecords) {
  }

  @Override
  public int getValueCapacity() {
    return this.valueCount;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl();
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((NullVector) target);
  }

  @Override
  public FieldReader getReader() {
    return NullReader.INSTANCE;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (!children.isEmpty()) {
      throw new IllegalArgumentException("Null vector has no children");
    }
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return Collections.emptyList();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    Preconditions.checkArgument(ownBuffers.isEmpty(), "Null vector has no buffers");
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return Collections.emptyList();
  }

  /**
   * Get the inner vectors.
   *
   * @deprecated This API will be removed as the current implementations no longer support inner vectors.
   *
   * @return the inner vectors for this field as defined by the TypeLayout
   */
  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return Collections.emptyList();
  }

  @Override
  public long getValidityBufferAddress() {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
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
    return this.valueCount;
  }

  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  @Override
  public Object getObject(int index) {
    return null;
  }

  @Override
  public int getNullCount() {
    return this.valueCount;
  }


  /**
   * Set the element at the given index to null. In a NullVector, this is a no-op.
   *
   * @param index position of element
   */
  @Override
  public void setNull(int index) {}

  @Override
  public boolean isNull(int index) {
    return true;
  }

  @Override
  public int hashCode(int index) {
    return 31 * valueCount;
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return 31 * valueCount;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }

  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return this.getField().getName();
  }

  private class TransferImpl implements TransferPair {
    NullVector to;

    public TransferImpl(String ref) {
      to = new NullVector(ref);
    }

    @Deprecated
    public TransferImpl() {
      to = new NullVector();
    }

    public TransferImpl(NullVector to) {
      this.to = to;
    }

    @Override
    public NullVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      to.valueCount = valueCount;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.valueCount = length;
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      if (toIndex > to.valueCount) {
        to.valueCount = toIndex;
      }
    }
  }
}
