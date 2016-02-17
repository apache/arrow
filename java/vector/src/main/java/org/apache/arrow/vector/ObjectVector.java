/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import io.netty.buffer.ArrowBuf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.ObjectHolder;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.util.TransferPair;

public class ObjectVector extends BaseValueVector {
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private int maxCount = 0;
  private int count = 0;
  private int allocationSize = 4096;

  private List<Object[]> objectArrayList = new ArrayList<>();

  public ObjectVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  public void addNewArray() {
    objectArrayList.add(new Object[allocationSize]);
    maxCount += allocationSize;
  }

  @Override
  public FieldReader getReader() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  public final class Mutator implements ValueVector.Mutator {

    public void set(int index, Object obj) {
      int listOffset = index / allocationSize;
      if (listOffset >= objectArrayList.size()) {
        addNewArray();
      }
      objectArrayList.get(listOffset)[index % allocationSize] = obj;
    }

    public boolean setSafe(int index, long value) {
      set(index, value);
      return true;
    }

    protected void set(int index, ObjectHolder holder) {
      set(index, holder.obj);
    }

    public boolean setSafe(int index, ObjectHolder holder){
      set(index, holder);
      return true;
    }

    @Override
    public void setValueCount(int valueCount) {
      count = valueCount;
    }

    @Override
    public void reset() {
      count = 0;
      maxCount = 0;
      objectArrayList = new ArrayList<>();
      addNewArray();
    }

    @Override
    public void generateTestData(int values) {
    }
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    // NoOp
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    addNewArray();
  }

  public void allocateNew(int valueCount) throws OutOfMemoryException {
    while (maxCount < valueCount) {
      addNewArray();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    allocateNew();
    return true;
  }

  @Override
  public int getBufferSize() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public void clear() {
    objectArrayList.clear();
    maxCount = 0;
    count = 0;
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  @Override
  public int getValueCapacity() {
    return maxCount;
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

//  @Override
//  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
//    throw new UnsupportedOperationException("ObjectVector does not support this");
//  }
//
//  @Override
//  public UserBitShared.SerializedField getMetadata() {
//    throw new UnsupportedOperationException("ObjectVector does not support this");
//  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public Iterator<ValueVector> iterator() {
    throw new UnsupportedOperationException("ObjectVector does not support this");
  }

  public final class Accessor extends BaseAccessor {
    @Override
    public Object getObject(int index) {
      int listOffset = index / allocationSize;
      if (listOffset >= objectArrayList.size()) {
        addNewArray();
      }
      return objectArrayList.get(listOffset)[index % allocationSize];
    }

    @Override
    public int getValueCount() {
      return count;
    }

    public Object get(int index) {
      return getObject(index);
    }

    public void get(int index, ObjectHolder holder){
      holder.obj = getObject(index);
    }
  }
}
