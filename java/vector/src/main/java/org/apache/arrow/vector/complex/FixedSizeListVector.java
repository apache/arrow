/*******************************************************************************

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
 ******************************************************************************/

package org.apache.arrow.vector.complex;

import static java.util.Collections.singletonList;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.DATA_VECTOR_NAME;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseDataValueVector;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;
import org.apache.arrow.vector.util.TransferPair;

public class FixedSizeListVector extends BaseValueVector implements FieldVector, PromotableVector {

  public static FixedSizeListVector empty(String name, int size, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(new ArrowType.FixedSizeList(size));
    return new FixedSizeListVector(name, allocator, fieldType, null);
  }

  private FieldVector vector;
  private final BitVector bits;
  private final int listSize;
  private final FieldType fieldType;
  private final List<BufferBacked> innerVectors;

  private UnionFixedSizeListReader reader;

  private Mutator mutator = new Mutator();
  private Accessor accessor = new Accessor();

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public FixedSizeListVector(String name,
                             BufferAllocator allocator,
                             int listSize,
                             DictionaryEncoding dictionary,
                             CallBack schemaChangeCallback) {
    this(name, allocator, new FieldType(true, new ArrowType.FixedSizeList(listSize), dictionary), schemaChangeCallback);
  }

  public FixedSizeListVector(String name,
                             BufferAllocator allocator,
                             FieldType fieldType,
                             CallBack schemaChangeCallback) {
    super(name, allocator);
    this.bits = new BitVector("$bits$", allocator);
    this.vector = ZeroVector.INSTANCE;
    this.fieldType = fieldType;
    this.listSize = ((ArrowType.FixedSizeList) fieldType.getType()).getListSize();
    Preconditions.checkArgument(listSize > 0, "list size must be positive");
    this.innerVectors = Collections.singletonList((BufferBacked) bits);
    this.reader = new UnionFixedSizeListReader(this);
  }

  @Override
  public Field getField() {
    List<Field> children = ImmutableList.of(getDataVector().getField());
    return new Field(name, fieldType, children);
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.FIXED_SIZE_LIST;
  }

  public int getListSize() {
    return listSize;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (children.size() != 1) {
      throw new IllegalArgumentException("Lists have only one child. Found: " + children);
    }
    Field field = children.get(0);
    AddOrGetResult<FieldVector> addOrGetVector = addOrGetVector(field.getFieldType());
    if (!addOrGetVector.isCreated()) {
      throw new IllegalArgumentException("Child vector already existed: " + addOrGetVector.getVector());
    }
    addOrGetVector.getVector().initializeChildrenFromFields(field.getChildren());
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return singletonList(vector);
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    BaseDataValueVector.load(fieldNode, innerVectors, ownBuffers);
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return BaseDataValueVector.unload(innerVectors);
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return innerVectors;
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public UnionFixedSizeListReader getReader() {
    return reader;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    allocateNewSafe();
  }

  @Override
  public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = bits.allocateNewSafe() && vector.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    if (success) {
      bits.zeroVector();
    }
    return success;
  }

  @Override
  public void reAlloc() {
    bits.reAlloc();
    vector.reAlloc();
  }

  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    vector.setInitialCapacity(numRecords * listSize);
  }

  @Override
  public int getValueCapacity() {
    if (vector == ZeroVector.INSTANCE) {
      return 0;
    }
    return vector.getValueCapacity() / listSize;
  }

  @Override
  public int getBufferSize() {
    if (accessor.getValueCount() == 0) {
      return 0;
    }
    return bits.getBufferSize() + vector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return bits.getBufferSizeFor(valueCount) + vector.getBufferSizeFor(valueCount * listSize);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>singleton(vector).iterator();
  }

  @Override
  public void clear() {
    bits.clear();
    vector.clear();
    super.clear();
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), vector.getBuffers(false), ArrowBuf.class);
    if (clear) {
      for (ArrowBuf buffer : buffers) {
        buffer.retain();
      }
      clear();
    }
    return buffers;
  }

  /**
   * @return 1 if inner vector is explicitly set via #addOrGetVector else 0
   */
  public int size() {
    return vector == ZeroVector.INSTANCE ? 0 : 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType type) {
    boolean created = false;
    if (vector == ZeroVector.INSTANCE) {
      vector = type.createNewSingleVector(DATA_VECTOR_NAME, allocator, null);
      this.reader = new UnionFixedSizeListReader(this);
      created = true;
    }
    // returned vector must have the same field
    if (!Objects.equals(vector.getField().getType(), type.getType())) {
      final String msg = String.format("Inner vector type mismatch. Requested type: [%s], actual type: [%s]",
          type.getType(), vector.getField().getType());
      throw new SchemaChangeRuntimeException(msg);
    }

    return new AddOrGetResult<>((T) vector, created);
  }

  public void copyFromSafe(int inIndex, int outIndex, FixedSizeListVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public void copyFrom(int fromIndex, int thisIndex, FixedSizeListVector from) {
    TransferPair pair = from.makeTransferPair(this);
    pair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public UnionVector promoteToUnion() {
    UnionVector vector = new UnionVector(name, allocator, null);
    this.vector.clear();
    this.vector = vector;
    this.reader = new UnionFixedSizeListReader(this);
    return vector;
  }

  public class Accessor extends BaseValueVector.BaseAccessor {

    @Override
    public Object getObject(int index) {
      if (isNull(index)) {
        return null;
      }
      final List<Object> vals = new JsonStringArrayList<>(listSize);
      final ValueVector.Accessor valuesAccessor = vector.getAccessor();
      for (int i = 0; i < listSize; i++) {
        vals.add(valuesAccessor.getObject(index * listSize + i));
      }
      return vals;
    }

    @Override
    public boolean isNull(int index) {
      return bits.getAccessor().get(index) == 0;
    }

    @Override
    public int getNullCount() {
      return bits.getAccessor().getNullCount();
    }

    @Override
    public int getValueCount() {
      return bits.getAccessor().getValueCount();
    }
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    public void setNull(int index) {
      bits.getMutator().setSafe(index, 0);
    }

    public void setNotNull(int index) {
      bits.getMutator().setSafe(index, 1);
    }

    @Override
    public void setValueCount(int valueCount) {
      bits.getMutator().setValueCount(valueCount);
      vector.getMutator().setValueCount(valueCount * listSize);
    }
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
    return new TransferImpl((FixedSizeListVector) target);
  }

  private class TransferImpl implements TransferPair {

    FixedSizeListVector to;
    TransferPair pairs[] = new TransferPair[2];

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      this(new FixedSizeListVector(name, allocator, fieldType, callBack));
    }

    public TransferImpl(FixedSizeListVector to) {
      this.to = to;
      to.addOrGetVector(vector.getField().getFieldType());
      pairs[0] = bits.makeTransferPair(to.bits);
      pairs[1] = vector.makeTransferPair(to.vector);
    }

    @Override
    public void transfer() {
      for (TransferPair pair : pairs) {
        pair.transfer();
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.allocateNew();
      for (int i = 0; i < length; i++) {
        copyValueSafe(startIndex + i, i);
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      pairs[0].copyValueSafe(from, to);
      int fromOffset = from * listSize;
      int toOffset = to * listSize;
      for (int i = 0; i < listSize; i++) {
        pairs[1].copyValueSafe(fromOffset + i, toOffset + i);
      }
    }
  }
}
