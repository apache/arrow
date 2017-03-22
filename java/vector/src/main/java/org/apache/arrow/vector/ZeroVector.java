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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.impl.NullReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

public class ZeroVector implements FieldVector {
  public final static ZeroVector INSTANCE = new ZeroVector();

  private final String name = "[DEFAULT]";

  private final TransferPair defaultPair = new TransferPair() {
    @Override
    public void transfer() { }

    @Override
    public void splitAndTransfer(int startIndex, int length) { }

    @Override
    public ValueVector getTo() {
      return ZeroVector.this;
    }

    @Override
    public void copyValueSafe(int from, int to) { }
  };

  private final Accessor defaultAccessor = new Accessor() {
    @Override
    public Object getObject(int index) {
      return null;
    }

    @Override
    public int getValueCount() {
      return 0;
    }

    @Override
    public boolean isNull(int index) {
      return true;
    }

    @Override
    public int getNullCount() {
      return 0;
    }
  };

  private final Mutator defaultMutator = new Mutator() {
    @Override
    public void setValueCount(int valueCount) { }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  };

  public ZeroVector() { }

  @Override
  public void close() { }

  @Override
  public void clear() { }

  @Override
  public Field getField() {
    return new Field(name, true, new Null(), null);
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.NULL;
  }


  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return defaultPair;
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
  public BufferAllocator getAllocator() {
    throw new UnsupportedOperationException("Tried to get allocator from ZeroVector");
  }

  @Override
  public void setInitialCapacity(int numRecords) { }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return defaultPair;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return defaultPair;
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return defaultPair;
  }

  @Override
  public Accessor getAccessor() {
    return defaultAccessor;
  }

  @Override
  public Mutator getMutator() {
    return defaultMutator;
  }

  @Override
  public FieldReader getReader() {
    return NullReader.INSTANCE;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (!children.isEmpty()) {
      throw new IllegalArgumentException("Zero vector has no children");
    }
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return Collections.emptyList();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (!ownBuffers.isEmpty()) {
      throw new IllegalArgumentException("Zero vector has no buffers");
    }
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return Collections.emptyList();
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return Collections.emptyList();
  }
}
