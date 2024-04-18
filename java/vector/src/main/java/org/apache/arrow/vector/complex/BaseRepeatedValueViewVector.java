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

import java.util.Iterator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.util.CallBack;

public abstract class BaseRepeatedValueViewVector extends BaseValueVector
    implements RepeatedValueVector, BaseListVector {

  public static final FieldVector DEFAULT_DATA_VECTOR = ZeroVector.INSTANCE;
  public static final String DATA_VECTOR_NAME = "$data$";

  public static final byte OFFSET_WIDTH = 4;
  public static final byte SIZE_WIDTH = 4;
  protected ArrowBuf offsetBuffer;
  protected ArrowBuf sizeBuffer;
  protected FieldVector vector;
  protected final CallBack repeatedCallBack;
  protected int valueCount;
  protected long offsetAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;
  protected long sizeAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * SIZE_WIDTH;
  private final String name;

  protected String defaultDataVectorName = DATA_VECTOR_NAME;

  protected BaseRepeatedValueViewVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, DEFAULT_DATA_VECTOR, callBack);
  }

  protected BaseRepeatedValueViewVector(
      String name, BufferAllocator allocator, FieldVector vector, CallBack callBack) {
    super(allocator);
    this.name = name;
    this.offsetBuffer = allocator.getEmpty();
    this.sizeBuffer = allocator.getEmpty();
    this.vector = Preconditions.checkNotNull(vector, "data vector cannot be null");
    this.repeatedCallBack = callBack;
    this.valueCount = 0;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public boolean allocateNewSafe() {
    return false;
  }

  @Override
  public void reAlloc() {
  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public void setInitialCapacity(int numRecords) {

  }

  @Override
  public void setInitialCapacity(int numRecords, double density) {

  }

  public void setInitialTotalCapacity(int numRecords, int totalNumberOfElements) {

  }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    return 0;
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return null;
  }

  @Override
  public void clear() {

  }

  @Override
  public void reset() {

  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return new ArrowBuf[0];
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public void setValueCount(int valueCount) {

  }

  public boolean isEmpty(int index) {
    return false;
  }

  public int startNewValue(int index) {
    return 0;
  }

  @Override
  @Deprecated
  public UInt4Vector getOffsetVector() {
    throw new UnsupportedOperationException("There is no inner offset vector");
  }
}
