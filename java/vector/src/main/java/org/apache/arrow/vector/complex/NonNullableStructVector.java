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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.DensityAwareVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.SingleStructReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A struct vector that has no null values (and no validity buffer). Child Vectors are handled in
 * {@link AbstractStructVector}.
 */
public class NonNullableStructVector extends AbstractStructVector
    implements ValueIterableVector<Map<String, ?>> {

  /**
   * Construct a new empty instance which replaces an existing field with the new one in case of
   * name conflict.
   */
  public static NonNullableStructVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    return new NonNullableStructVector(
        name, allocator, fieldType, null, ConflictPolicy.CONFLICT_REPLACE, false);
  }

  /** Construct a new empty instance which preserve fields with identical names. */
  public static NonNullableStructVector emptyWithDuplicates(
      String name, BufferAllocator allocator) {
    FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    return new NonNullableStructVector(
        name, allocator, fieldType, null, ConflictPolicy.CONFLICT_APPEND, true);
  }

  private final SingleStructReaderImpl reader = new SingleStructReaderImpl(this);
  protected Field field;
  public int valueCount;

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param fieldType The type of this list.
   */
  public NonNullableStructVector(
      String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    this(new Field(name, fieldType, null), allocator, callBack);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param callBack A schema change callback.
   */
  public NonNullableStructVector(Field field, BufferAllocator allocator, CallBack callBack) {
    this(field, allocator, callBack, null, true);
  }

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param fieldType The type of this list.
   * @param callBack A schema change callback.
   * @param conflictPolicy How to handle duplicate field names in the struct.
   */
  public NonNullableStructVector(
      String name,
      BufferAllocator allocator,
      FieldType fieldType,
      CallBack callBack,
      ConflictPolicy conflictPolicy,
      boolean allowConflictPolicyChanges) {
    this(
        new Field(name, fieldType, null),
        allocator,
        callBack,
        conflictPolicy,
        allowConflictPolicyChanges);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param callBack A schema change callback.
   * @param conflictPolicy How to handle duplicate field names in the struct.
   */
  public NonNullableStructVector(
      Field field,
      BufferAllocator allocator,
      CallBack callBack,
      ConflictPolicy conflictPolicy,
      boolean allowConflictPolicyChanges) {
    super(field.getName(), allocator, callBack, conflictPolicy, allowConflictPolicyChanges);
    this.field = field;
    this.valueCount = 0;
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  private transient StructTransferPair ephPair;

  /**
   * Copies the element at fromIndex in the provided vector to thisIndex. Reallocates buffers if
   * thisIndex is larger then current capacity.
   */
  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    if (ephPair == null || ephPair.from != from) {
      ephPair = (StructTransferPair) from.makeTransferPair(this);
    }
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    copyFrom(fromIndex, thisIndex, from);
  }

  @Override
  protected boolean supportsDirectRead() {
    return true;
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    for (final ValueVector v : this) {
      v.setInitialCapacity(numRecords);
    }
  }

  @Override
  public void setInitialCapacity(int valueCount, double density) {
    for (final ValueVector vector : this) {
      if (vector instanceof DensityAwareVector) {
        ((DensityAwareVector) vector).setInitialCapacity(valueCount, density);
      } else {
        vector.setInitialCapacity(valueCount);
      }
    }
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0 || size() == 0) {
      return 0;
    }
    long buffer = 0;
    for (final ValueVector v : this) {
      buffer += v.getBufferSize();
    }

    return (int) buffer;
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
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
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new StructTransferPair(
        this,
        new NonNullableStructVector(
            name,
            allocator,
            field.getFieldType(),
            callBack,
            getConflictPolicy(),
            allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new StructTransferPair(this, (NonNullableStructVector) to);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new StructTransferPair(
        this,
        new NonNullableStructVector(
            ref,
            allocator,
            field.getFieldType(),
            callBack,
            getConflictPolicy(),
            allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new StructTransferPair(
        this,
        new NonNullableStructVector(
            field, allocator, callBack, getConflictPolicy(), allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return new StructTransferPair(
        this,
        new NonNullableStructVector(
            field, allocator, callBack, getConflictPolicy(), allowConflictPolicyChanges),
        false);
  }

  /** {@link TransferPair} for this this class. */
  protected static class StructTransferPair implements TransferPair {
    private final TransferPair[] pairs;
    private final NonNullableStructVector from;
    private final NonNullableStructVector to;

    public StructTransferPair(NonNullableStructVector from, NonNullableStructVector to) {
      this(from, to, true);
    }

    protected StructTransferPair(
        NonNullableStructVector from, NonNullableStructVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      this.to.ephPair = null;

      int i = 0;
      FieldVector vector;
      for (String child : from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        // DRILL-1872: we add the child fields for the vector, looking up the field by name. For a
        // map vector,
        // the child fields may be nested fields of the top level child. For example if the
        // structure
        // of a child field is oa.oab.oabc then we add oa, then add oab to oa then oabc to oab.
        // But the children member of a Materialized field is a HashSet. If the fields are added in
        // the
        // children HashSet, and the hashCode of the Materialized field includes the hash code of
        // the
        // children, the hashCode value of oa changes *after* the field has been added to the
        // HashSet.
        // (This is similar to what happens in ScanBatch where the children cannot be added till
        // they are
        // read). To take care of this, we ensure that the hashCode of the MaterializedField does
        // not
        // include the hashCode of the children but is based only on MaterializedField$key.
        final FieldVector newVector =
            to.addOrGet(child, vector.getField().getFieldType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    @Override
    public void transfer() {
      for (final TransferPair p : pairs) {
        p.transfer();
      }
      to.valueCount = from.valueCount;
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(from, to);
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.setValueCount(length);
    }
  }

  @Override
  public int getValueCapacity() {
    if (size() == 0) {
      return 0;
    }

    return getChildren().stream().mapToInt(child -> child.getValueCapacity()).min().getAsInt();
  }

  @Override
  public Map<String, ?> getObject(int index) {
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

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    int hash = 0;
    for (FieldVector v : getChildren()) {
      if (index < v.getValueCount()) {
        hash = ByteFunctionHelpers.combineHash(hash, v.hashCode(index, hasher));
      }
    }
    return hash;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  public void get(int index, ComplexHolder holder) {
    reader.setPosition(index);
    holder.reader = reader;
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }

  public ValueVector getVectorById(int id) {
    return getChildByOrdinal(id);
  }

  /** Gets a child vector by ordinal position and casts to the specified class. */
  public <V extends ValueVector> V getVectorById(int id, Class<V> clazz) {
    ValueVector untyped = getVectorById(id);
    if (clazz.isInstance(untyped)) {
      return clazz.cast(untyped);
    }
    throw new ClassCastException(
        "Id "
            + id
            + " had the wrong type. Expected "
            + clazz.getCanonicalName()
            + " but was "
            + untyped.getClass().getCanonicalName());
  }

  @Override
  public void setValueCount(int valueCount) {
    for (final ValueVector v : getChildren()) {
      v.setValueCount(valueCount);
    }
    NonNullableStructVector.this.valueCount = valueCount;
  }

  @Override
  public void clear() {
    for (final ValueVector v : getChildren()) {
      v.clear();
    }
    valueCount = 0;
  }

  @Override
  public void reset() {
    for (final ValueVector v : getChildren()) {
      v.reset();
    }
    valueCount = 0;
  }

  @Override
  public Field getField() {
    List<Field> children = new ArrayList<>();
    for (ValueVector child : getChildren()) {
      children.add(child.getField());
    }
    if (children.isEmpty() || field.getChildren().equals(children)) {
      return field;
    }
    field = new Field(field.getName(), field.getFieldType(), children);
    return field;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.STRUCT;
  }

  @Override
  public void close() {
    final Collection<FieldVector> vectors = getChildren();
    for (final FieldVector v : vectors) {
      v.close();
    }
    vectors.clear();

    valueCount = 0;

    super.close();
  }

  /** Initializes the struct's members from the given Fields. */
  public void initializeChildrenFromFields(List<Field> children) {
    for (Field field : children) {
      FieldVector vector = (FieldVector) this.add(field.getName(), field.getFieldType());
      vector.initializeChildrenFromFields(field.getChildren());
    }
  }

  public List<FieldVector> getChildrenFromFields() {
    return getChildren();
  }
}
