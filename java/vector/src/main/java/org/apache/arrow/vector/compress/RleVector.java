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

package org.apache.arrow.vector.compress;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * An RleVector represents a {@link ValueVector} encoded by
 * the RLE (run length encoding) encoding scheme.
 * @param <V> the type of the vector being encoded.
 */
public class RleVector<V extends FieldVector> extends BaseValueVector implements FieldVector {

  /**
   * The width (in byte) of the data in runEndIndexBuffer.
   */
  public static final int RUN_LENGTH_BUFFER_WIDTH = 4;

  /**
   * The buffer for the end indices of runs.
   */
  private ArrowBuf runEndIndexBuffer;

  private final V innerVector;

  private final Field field;

  private final String name;

  protected RleVector(String name, Field field, BufferAllocator allocator) {
    super(allocator);
    this.name = name;
    this.field = field;
    this.runEndIndexBuffer = allocator.getEmpty();
    this.innerVector = (V) field.getFieldType().createNewSingleVector("rle_inner_" + name, allocator, null);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    allocateNew(INITIAL_VALUE_ALLOCATION);
  }

  @Override
  public boolean allocateNewSafe() {
    try {
      allocateNew();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void allocateNew(int valueCount) {
    clear();
    try {
      allocateRunEndIndexBuffer(RUN_LENGTH_BUFFER_WIDTH);
      innerVector.allocateNew();
      while (innerVector.getValueCapacity() < valueCount) {
        innerVector.reAlloc();
      }
    } catch (Exception e) {
      clear();
      throw e;
    }
  }

  @Override
  public void reAlloc() {
    int targetValueCount = getValueCapacity() * 2;
    if (runEndIndexBuffer.capacity() < targetValueCount * RUN_LENGTH_BUFFER_WIDTH) {
      ArrowBuf newRunLengthBuffer = allocator.buffer(targetValueCount * RUN_LENGTH_BUFFER_WIDTH);
      PlatformDependent.copyMemory(runEndIndexBuffer.memoryAddress(),
              newRunLengthBuffer.memoryAddress(),
              innerVector.getValueCount() * RUN_LENGTH_BUFFER_WIDTH);
      runEndIndexBuffer.close();
      runEndIndexBuffer = newRunLengthBuffer;
    }

    while (innerVector.getValueCapacity() < targetValueCount) {
      innerVector.reAlloc();
    }
  }

  private void allocateRunEndIndexBuffer(int valueCount) {
    runEndIndexBuffer = allocator.buffer(valueCount * RUN_LENGTH_BUFFER_WIDTH);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    innerVector.setInitialCapacity(numRecords);
  }

  @Override
  public int getValueCapacity() {
    return Math.min(runEndIndexBuffer.capacity() / RUN_LENGTH_BUFFER_WIDTH, innerVector.getValueCapacity());
  }

  @Override
  public void reset() {
    runEndIndexBuffer.setZero(0, runEndIndexBuffer.capacity());
    innerVector.reset();
  }

  @Override
  public Field getField() {
    return field;
  }

  @Override
  public Types.MinorType getMinorType() {
    return innerVector.getMinorType();
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((RleVector<V>) target);
  }

  @Override
  public FieldReader getReader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBufferSize() {
    return innerVector.getValueCount() * RUN_LENGTH_BUFFER_WIDTH + innerVector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    return valueCount * RUN_LENGTH_BUFFER_WIDTH + innerVector.getBufferSizeFor(valueCount);
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      ArrowBuf[] innerBuffers = innerVector.getBuffers(clear);
      buffers = new ArrowBuf[innerBuffers.length + 1];
      for (int i = 0; i < innerBuffers.length; i++) {
        buffers[i] = innerBuffers[i];
      }
      buffers[innerBuffers.length] = runEndIndexBuffer;
    }
    if (clear) {
      for (final ArrowBuf buffer : buffers) {
        buffer.getReferenceManager().retain(1);
      }
      clear();
    }
    return buffers;
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
    return innerVector.getValueCount();
  }

  /**
   * Gets the number of values in the decoded vector.
   * @return the number of values after decoding.
   */
  public int getDecodedValueCount() {
    return runEndIndexBuffer.getInt((innerVector.getValueCount() - 1) * RUN_LENGTH_BUFFER_WIDTH);
  }

  @Override
  public void setValueCount(int valueCount) {
    innerVector.setValueCount(valueCount);
  }

  @Override
  public Object getObject(int index) {
    return innerVector.getObject(index);
  }

  @Override
  public int getNullCount() {
    return innerVector.getNullCount();
  }

  /**
   * Gets the number of null values in the decoded vector.
   * @return the number of nulls in the decoded vector.
   */
  public int getDecodedNullCount() {
    int ret = 0;
    for (int i = 0; i < innerVector.getValueCount(); i++) {
      if (isNull(i)) {
        int startIdx = i == 0 ? 0 : runEndIndexBuffer.getInt((i - 1) * RUN_LENGTH_BUFFER_WIDTH);
        int endIdx = runEndIndexBuffer.getInt(i * RUN_LENGTH_BUFFER_WIDTH);
        ret += endIdx - startIdx;
      }
    }
    return ret;
  }

  @Override
  public boolean isNull(int index) {
    return innerVector.isNull(index);
  }

  /**
   * Given the index in the decoded vector, gets the index
   * in the encoded vector.
   * This operation takes O(log(n)) time, where n is the length of
   * the encoded vector.
   * @param decodedIndex the index in the decoded vector.
   * @return the index in the encoded index.
   */
  public int getIndexInEncodedVector(int decodedIndex) {
    Preconditions.checkArgument(decodedIndex >= 0 && decodedIndex < getDecodedValueCount());

    int low = 0;
    int high = innerVector.getValueCount() - 1;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      int index = runEndIndexBuffer.getInt(mid * RUN_LENGTH_BUFFER_WIDTH);

      if (index < decodedIndex) {
        low = mid + 1;
      } else if (index > decodedIndex) {
        if (mid == 0) {
          // we are in the first run
          return mid;
        }

        // gets the start index of the current run
        int startIdx = runEndIndexBuffer.getInt((mid - 1) * RUN_LENGTH_BUFFER_WIDTH);
        if (startIdx <= decodedIndex) {
          // we are in the current run
          return mid;
        }
        high = mid - 1;
      } else {
        // we happen to hit the boundary, the match is in the next run
        return mid + 1;
      }
    }

    throw new IllegalStateException("Should never get here");
  }

  /**
   * Populates the decoded vector.
   * @param outVector the vector to populate.
   */
  public void populateDecodedVector(V outVector) {
    int outIndex = 0;
    for (int i = 0; i < innerVector.getValueCount(); i++) {
      int startIndex = i == 0 ? 0 : runEndIndexBuffer.getInt((i - 1) * RUN_LENGTH_BUFFER_WIDTH);
      int endIndex = runEndIndexBuffer.getInt(i * RUN_LENGTH_BUFFER_WIDTH);
      int repeat = endIndex - startIndex;
      for (int j = 0; j < repeat; j++) {
        outVector.copyFromSafe(i, outIndex++, innerVector);
      }
    }
    outVector.setValueCount(outIndex);
  }

  @Override
  public int hashCode(int index) {
    int bufHash = new ArrowBufPointer(
            runEndIndexBuffer, 0, innerVector.getValueCount() * RUN_LENGTH_BUFFER_WIDTH).hashCode();
    int vecHash = innerVector.hashCode();

    return bufHash * 37 + vecHash;
  }

  private void handleSafe(int index) {
    while (index >= getValueCapacity()) {
      reAlloc();
    }
  }

  /**
   * Append a value to the end of the vector.
   * @param vector the vector containing the value to append.
   * @param index the index within the vector to append.
   */
  public void appendFrom(V vector, int index) {
    int curValueCount = innerVector.getValueCount();
    int curIndex = curValueCount == 0 ? 0 : runEndIndexBuffer.getInt((curValueCount - 1) * RUN_LENGTH_BUFFER_WIDTH);

    RangeEqualsVisitor visitor =
            new RangeEqualsVisitor(vector, index, curValueCount - 1, 1, false);
    if (curValueCount > 0 && visitor.equals(innerVector)) {
      runEndIndexBuffer.setInt((curValueCount - 1) * RUN_LENGTH_BUFFER_WIDTH, curIndex + 1);
    } else {
      handleSafe(index);

      runEndIndexBuffer.setInt(curValueCount * RUN_LENGTH_BUFFER_WIDTH, curIndex + 1);
      innerVector.copyFrom(index, curValueCount, vector);
      innerVector.setValueCount(curValueCount + 1);
    }
  }

  /**
   * Gets the encoded vector.
   * @return the encoded vector.
   */
  public V getEncodedVector() {
    return innerVector;
  }

  /**
   * Gets the buffer containing the end indices of run lengths.
   * @return the buffer with end indices.
   */
  public ArrowBuf getRunEndIndexBuffer() {
    return runEndIndexBuffer;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    runEndIndexBuffer.close();
    innerVector.clear();
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    innerVector.initializeChildrenFromFields(children);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return innerVector.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    Preconditions.checkArgument(!ownBuffers.isEmpty());
    runEndIndexBuffer = ownBuffers.get(ownBuffers.size() - 1);
    innerVector.loadFieldBuffers(fieldNode, ownBuffers.subList(0, ownBuffers.size() - 1));
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> ret = new ArrayList<>();
    ret.addAll(innerVector.getFieldBuffers());
    ret.add(runEndIndexBuffer);
    return ret;
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException();
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

  /**
   * Transfer this vector'data to another vector. The memory associated
   * with this vector is transferred to the allocator of target vector
   * for accounting and management purposes.
   * @param target destination vector for transfer
   */
  public void transferTo(RleVector<V> target) {
    target.clear();
    TransferPair innerPair = innerVector.makeTransferPair(target.innerVector);
    innerPair.transfer();
    target.runEndIndexBuffer = transferBuffer(runEndIndexBuffer, target.allocator);
    clear();
  }

  private class TransferImpl implements TransferPair {
    RleVector<V> to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new RleVector<>(ref, field, allocator);
    }

    public TransferImpl(RleVector<V> to) {
      this.to = to;
    }

    @Override
    public RleVector<V> getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.clear();
      to.allocateRunEndIndexBuffer(length);
      PlatformDependent.copyMemory(
              runEndIndexBuffer.memoryAddress() + startIndex * RUN_LENGTH_BUFFER_WIDTH,
              to.runEndIndexBuffer.memoryAddress(),
              length * RUN_LENGTH_BUFFER_WIDTH);

      TransferPair innerPair = innerVector.makeTransferPair(to.innerVector);
      innerPair.splitAndTransfer(startIndex, length);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, RleVector.this);
    }
  }
}
