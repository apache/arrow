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

import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * A vector that wraps an underlying vector, used to help implement extension types.
 * @param <T> The wrapped vector type.
 */
public abstract class ExtensionTypeVector<T extends BaseValueVector & FieldVector> extends BaseValueVector implements
    FieldVector {

  private final T underlyingVector;
  private final String name;

  /**
   * Instantiate an extension type vector.
   * @param name name of the vector
   * @param allocator allocator for memory management
   * @param underlyingVector underlying filed vector
   */
  public ExtensionTypeVector(String name, BufferAllocator allocator, T underlyingVector) {
    super(allocator);
    this.name = name;
    this.underlyingVector = underlyingVector;
  }

  /**
   * Instantiate an extension type vector.
   * @param field field materialized by this vector.
   * @param allocator allocator for memory management
   * @param underlyingVector underlying filed vector
   */
  public ExtensionTypeVector(Field field, BufferAllocator allocator, T underlyingVector) {
    this(field.getName(), allocator, underlyingVector);
  }

  @Override
  public String getName() {
    return name;
  }

  /** Get the underlying vector. */
  public T getUnderlyingVector() {
    return underlyingVector;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    this.underlyingVector.allocateNew();
  }

  @Override
  public boolean allocateNewSafe() {
    return this.underlyingVector.allocateNewSafe();
  }

  @Override
  public void reAlloc() {
    this.underlyingVector.reAlloc();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    this.underlyingVector.setInitialCapacity(numRecords);
  }

  @Override
  public int getValueCapacity() {
    return this.underlyingVector.getValueCapacity();
  }

  @Override
  public void reset() {
    this.underlyingVector.reset();
  }

  @Override
  public Field getField() {
    return this.underlyingVector.getField();
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.EXTENSIONTYPE;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return underlyingVector.getTransferPair(ref, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return underlyingVector.getTransferPair(ref, allocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return underlyingVector.makeTransferPair(target);
  }

  @Override
  public FieldReader getReader() {
    return underlyingVector.getReader();
  }

  @Override
  public int getBufferSize() {
    return underlyingVector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    return underlyingVector.getBufferSizeFor(valueCount);
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return underlyingVector.getBuffers(clear);
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return underlyingVector.getValidityBuffer();
  }

  @Override
  public ArrowBuf getDataBuffer() {
    return underlyingVector.getDataBuffer();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    return underlyingVector.getOffsetBuffer();
  }

  @Override
  public int getValueCount() {
    return underlyingVector.getValueCount();
  }

  @Override
  public void setValueCount(int valueCount) {
    underlyingVector.setValueCount(valueCount);
  }

  /**
   * Get the extension object at the specified index.
   *
   * <p>Generally, this should access the underlying vector and construct the corresponding Java object from the raw
   * data.
   */
  @Override
  public abstract Object getObject(int index);

  @Override
  public int getNullCount() {
    return underlyingVector.getNullCount();
  }

  @Override
  public boolean isNull(int index) {
    return underlyingVector.isNull(index);
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    underlyingVector.initializeChildrenFromFields(children);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return underlyingVector.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    underlyingVector.loadFieldBuffers(fieldNode, ownBuffers);
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return underlyingVector.getFieldBuffers();
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return underlyingVector.getFieldInnerVectors();
  }

  @Override
  public long getValidityBufferAddress() {
    return underlyingVector.getValidityBufferAddress();
  }

  @Override
  public long getDataBufferAddress() {
    return underlyingVector.getDataBufferAddress();
  }

  @Override
  public long getOffsetBufferAddress() {
    return underlyingVector.getOffsetBufferAddress();
  }

  @Override
  public void clear() {
    underlyingVector.clear();
  }

  @Override
  public void close() {
    underlyingVector.close();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return underlyingVector.getTransferPair(allocator);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return underlyingVector.iterator();
  }

  @Override
  public BufferAllocator getAllocator() {
    return underlyingVector.getAllocator();
  }
}
