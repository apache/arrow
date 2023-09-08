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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.AbstractStructVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.DataSizeRoundingUtil;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/UnionVector.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex;

<#include "/@includes/vv_imports.ftl" />
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.util.Preconditions;

import static org.apache.arrow.vector.types.UnionMode.Sparse;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt;

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")


/**
 * A vector which can hold values of different types. It does so by using a StructVector which contains a vector for each
 * primitive type that is stored. StructVector is used in order to take advantage of its serialization/deserialization methods,
 * as well as the addOrGet method.
 *
 * For performance reasons, UnionVector stores a cached reference to each subtype vector, to avoid having to do the struct lookup
 * each time the vector is accessed.
 * Source code generated using FreeMarker template ${.template_name}
 */
public class UnionVector extends AbstractContainerVector implements FieldVector {
  int valueCount;

  NonNullableStructVector internalStruct;
  protected ArrowBuf typeBuffer;

  private StructVector structVector;
  private ListVector listVector;
  private MapVector mapVector;

  private FieldReader reader;

  private int singleType = 0;
  private ValueVector singleVector;

  private int typeBufferAllocationSizeInBytes;

  private final FieldType fieldType;
  private final Field[] typeIds = new Field[Byte.MAX_VALUE + 1];

  public static final byte TYPE_WIDTH = 1;
  private static final FieldType INTERNAL_STRUCT_TYPE = new FieldType(false /*nullable*/,
      ArrowType.Struct.INSTANCE, null /*dictionary*/, null /*metadata*/);

  public static UnionVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(new ArrowType.Union(
        UnionMode.Sparse, null));
    return new UnionVector(name, allocator, fieldType, null);
  }

  public UnionVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, callBack);
    this.fieldType = fieldType;
    this.internalStruct = new NonNullableStructVector(
        "internal",
        allocator,
        INTERNAL_STRUCT_TYPE,
        callBack,
        AbstractStructVector.ConflictPolicy.CONFLICT_REPLACE,
        false);
    this.typeBuffer = allocator.getEmpty();
    this.typeBufferAllocationSizeInBytes = BaseValueVector.INITIAL_VALUE_ALLOCATION * TYPE_WIDTH;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.UNION;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    int count = 0;
    for (Field child: children) {
      int typeId = Types.getMinorTypeForArrowType(child.getType()).ordinal();
      if (fieldType != null) {
        int[] typeIds = ((ArrowType.Union)fieldType.getType()).getTypeIds();
        if (typeIds != null) {
          typeId = typeIds[count++];
        }
      }
      typeIds[typeId] = child;
    }
    internalStruct.initializeChildrenFromFields(children);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return internalStruct.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException("Illegal buffer count, expected 1, got: " + ownBuffers.size());
    }
    ArrowBuf buffer = ownBuffers.get(0);
    typeBuffer.getReferenceManager().release();
    typeBuffer = buffer.getReferenceManager().retain(buffer, allocator);
    typeBufferAllocationSizeInBytes = checkedCastToInt(typeBuffer.capacity());
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(1);
    setReaderAndWriterIndex();
    result.add(typeBuffer);

    return result;
  }

  private void setReaderAndWriterIndex() {
    typeBuffer.readerIndex(0);
    typeBuffer.writerIndex(valueCount * TYPE_WIDTH);
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
     throw new UnsupportedOperationException("There are no inner vectors. Use geFieldBuffers");
  }

  private String fieldName(MinorType type) {
    return type.name().toLowerCase();
  }

  private FieldType fieldType(MinorType type) {
    return FieldType.nullable(type.getType());
  }

  private <T extends FieldVector> T addOrGet(Types.MinorType minorType, Class<T> c) {
    return addOrGet(null, minorType, c);
  }

  private <T extends FieldVector> T addOrGet(String name, Types.MinorType minorType, ArrowType arrowType, Class<T> c) {
    return internalStruct.addOrGet(name == null ? fieldName(minorType) : name, FieldType.nullable(arrowType), c);
  }

  private <T extends FieldVector> T addOrGet(String name, Types.MinorType minorType, Class<T> c) {
    return internalStruct.addOrGet(name == null ? fieldName(minorType) : name, fieldType(minorType), c);
  }


  @Override
  public long getValidityBufferAddress() {
    throw new UnsupportedOperationException();
  }

  public long getTypeBufferAddress() {
    return typeBuffer.memoryAddress();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException();
  }

  public ArrowBuf getTypeBuffer() {
    return typeBuffer;
  }

  @Override
  public ArrowBuf getValidityBuffer() { throw new UnsupportedOperationException(); }

  @Override
  public ArrowBuf getDataBuffer() { throw new UnsupportedOperationException(); }

  @Override
  public ArrowBuf getOffsetBuffer() { throw new UnsupportedOperationException(); }

  public StructVector getStruct() {
    if (structVector == null) {
      int vectorCount = internalStruct.size();
      structVector = addOrGet(MinorType.STRUCT, StructVector.class);
      if (internalStruct.size() > vectorCount) {
        structVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return structVector;
  }
  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#assign lowerCaseName = name?lower_case/>
      <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">

  private ${name}Vector ${uncappedName}Vector;

  <#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
  public ${name}Vector get${name}Vector() {
    if (${uncappedName}Vector == null) {
      throw new IllegalArgumentException("No ${name} present. Provide ArrowType argument to create a new vector");
    }
    return ${uncappedName}Vector;
  }
  public ${name}Vector get${name}Vector(ArrowType arrowType) {
    return get${name}Vector(null, arrowType);
  }
  public ${name}Vector get${name}Vector(String name, ArrowType arrowType) {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalStruct.size();
      ${uncappedName}Vector = addOrGet(name, MinorType.${name?upper_case}, arrowType, ${name}Vector.class);
      if (internalStruct.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return ${uncappedName}Vector;
  }
  <#else>
  public ${name}Vector get${name}Vector() {
    return get${name}Vector(null);
  }

  public ${name}Vector get${name}Vector(String name) {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalStruct.size();
      ${uncappedName}Vector = addOrGet(name, MinorType.${name?upper_case}, ${name}Vector.class);
      if (internalStruct.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return ${uncappedName}Vector;
  }
  </#if>
      </#if>
    </#list>
  </#list>

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalStruct.size();
      listVector = addOrGet(MinorType.LIST, ListVector.class);
      if (internalStruct.size() > vectorCount) {
        listVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return listVector;
  }

  public MapVector getMap() {
    if (mapVector == null) {
      throw new IllegalArgumentException("No map present. Provide ArrowType argument to create a new vector");
    }
    return mapVector;
  }

  public MapVector getMap(ArrowType arrowType) {
    return getMap(null, arrowType);
  }

  public MapVector getMap(String name, ArrowType arrowType) {
    if (mapVector == null) {
      int vectorCount = internalStruct.size();
      mapVector = addOrGet(name, MinorType.MAP, arrowType, MapVector.class);
      if (internalStruct.size() > vectorCount) {
        mapVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return mapVector;
  }

  public int getTypeValue(int index) {
    return typeBuffer.getByte(index * TYPE_WIDTH);
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    /* new allocation -- clear the current buffers */
    clear();
    internalStruct.allocateNew();
    try {
      allocateTypeBuffer();
    } catch (Exception e) {
      clear();
      throw e;
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* new allocation -- clear the current buffers */
    clear();
    boolean safe = internalStruct.allocateNewSafe();
    if (!safe) { return false; }
    try {
      allocateTypeBuffer();
    } catch (Exception e) {
      clear();
      return  false;
    }

    return true;
  }

  private void allocateTypeBuffer() {
    typeBuffer = allocator.buffer(typeBufferAllocationSizeInBytes);
    typeBuffer.readerIndex(0);
    typeBuffer.setZero(0, typeBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    internalStruct.reAlloc();
    reallocTypeBuffer();
  }

  private void reallocTypeBuffer() {
    final long currentBufferCapacity = typeBuffer.capacity();
    long newAllocationSize = currentBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (typeBufferAllocationSizeInBytes > 0) {
        newAllocationSize = typeBufferAllocationSizeInBytes;
      } else {
        newAllocationSize = BaseValueVector.INITIAL_VALUE_ALLOCATION * TYPE_WIDTH * 2;
      }
    }
    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer(checkedCastToInt(newAllocationSize));
    newBuf.setBytes(0, typeBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    typeBuffer.getReferenceManager().release(1);
    typeBuffer = newBuf;
    typeBufferAllocationSizeInBytes = (int)newAllocationSize;
  }

  @Override
  public void setInitialCapacity(int numRecords) { }

  @Override
  public int getValueCapacity() {
    return Math.min(getTypeBufferValueCapacity(), internalStruct.getValueCapacity());
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public void clear() {
    valueCount = 0;
    typeBuffer.getReferenceManager().release();
    typeBuffer = allocator.getEmpty();
    internalStruct.clear();
  }

  @Override
  public void reset() {
    valueCount = 0;
    typeBuffer.setZero(0, typeBuffer.capacity());
    internalStruct.reset();
  }

  @Override
  public Field getField() {
    List<org.apache.arrow.vector.types.pojo.Field> childFields = new ArrayList<>();
    List<FieldVector> children = internalStruct.getChildren();
    int[] typeIds = new int[children.size()];
    for (ValueVector v : children) {
      typeIds[childFields.size()] = v.getMinorType().ordinal();
      childFields.add(v.getField());
    }

    FieldType fieldType;
    if (this.fieldType == null) {
      fieldType = FieldType.nullable(new ArrowType.Union(Sparse, typeIds));
    } else {
      final UnionMode mode = ((ArrowType.Union)this.fieldType.getType()).getMode();
      fieldType = new FieldType(this.fieldType.isNullable(), new ArrowType.Union(mode, typeIds),
          this.fieldType.getDictionary(), this.fieldType.getMetadata());
    }

    return new Field(name, fieldType, childFields);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new org.apache.arrow.vector.complex.UnionVector.TransferImpl(ref, allocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  @Override
  public void copyFrom(int inIndex, int outIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    UnionVector fromCast = (UnionVector) from;
    fromCast.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(fromCast.reader, writer);
  }

  @Override
  public void copyFromSafe(int inIndex, int outIndex, ValueVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public FieldVector addVector(FieldVector v) {
    final String name = v.getName().isEmpty() ? fieldName(v.getMinorType()) : v.getName();
    Preconditions.checkState(internalStruct.getChild(name) == null, String.format("%s vector already exists", name));
    final FieldVector newVector = internalStruct.addOrGet(name, v.getField().getFieldType(), v.getClass());
    v.makeTransferPair(newVector).transfer();
    internalStruct.putChild(name, newVector);
    if (callBack != null) {
      callBack.doWork();
    }
    return newVector;
  }

  /**
   * Directly put a vector to internalStruct without creating a new one with same type.
   */
  public void directAddVector(FieldVector v) {
    String name = fieldName(v.getMinorType());
    Preconditions.checkState(internalStruct.getChild(name) == null, String.format("%s vector already exists", name));
    internalStruct.putChild(name, v);
    if (callBack != null) {
      callBack.doWork();
    }
  }

  private class TransferImpl implements TransferPair {
    private final TransferPair internalStructVectorTransferPair;
    private final UnionVector to;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      to = new UnionVector(name, allocator, /* field type */ null, callBack);
      internalStructVectorTransferPair = internalStruct.makeTransferPair(to.internalStruct);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
      internalStructVectorTransferPair = internalStruct.makeTransferPair(to.internalStruct);
    }

    @Override
    public void transfer() {
      to.clear();
      ReferenceManager refManager = typeBuffer.getReferenceManager();
      to.typeBuffer = refManager.transferOwnership(typeBuffer, to.allocator).getTransferredBuffer();
      internalStructVectorTransferPair.transfer();
      to.valueCount = valueCount;
      clear();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      Preconditions.checkArgument(startIndex >= 0 && length >= 0 && startIndex + length <= valueCount,
          "Invalid parameters startIndex: %s, length: %s for valueCount: %s", startIndex, length, valueCount);
      to.clear();

      internalStructVectorTransferPair.splitAndTransfer(startIndex, length);
      final int startPoint = startIndex * TYPE_WIDTH;
      final int sliceLength = length * TYPE_WIDTH;
      final ArrowBuf slicedBuffer = typeBuffer.slice(startPoint, sliceLength);
      final ReferenceManager refManager = slicedBuffer.getReferenceManager();
      to.typeBuffer = refManager.transferOwnership(slicedBuffer, to.allocator).getTransferredBuffer();
      to.setValueCount(length);
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, UnionVector.this);
    }
  }

  @Override
  public FieldReader getReader() {
    if (reader == null) {
      reader = new UnionReader(this);
    }
    return reader;
  }

  public FieldWriter getWriter() {
    if (writer == null) {
      writer = new UnionWriter(this);
    }
    return writer;
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0) { return 0; }

    return (valueCount * TYPE_WIDTH) + internalStruct.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize + (valueCount * TYPE_WIDTH);
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    List<ArrowBuf> list = new java.util.ArrayList<>();
    setReaderAndWriterIndex();
    if (getBufferSize() != 0) {
      list.add(typeBuffer);
      list.addAll(java.util.Arrays.asList(internalStruct.getBuffers(clear)));
    }
    if (clear) {
      valueCount = 0;
      typeBuffer.getReferenceManager().retain();
      typeBuffer.getReferenceManager().release();
      typeBuffer = allocator.getEmpty();
    }
    return list.toArray(new ArrowBuf[list.size()]);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return internalStruct.iterator();
  }

  public ValueVector getVector(int index) {
    return getVector(index, null);
  }

  public ValueVector getVector(int index, ArrowType arrowType) {
    int type = typeBuffer.getByte(index * TYPE_WIDTH);
    return getVectorByType(type, arrowType);
  }

  public ValueVector getVectorByType(int typeId) {
    return getVectorByType(typeId, null);
  }

    public ValueVector getVectorByType(int typeId, ArrowType arrowType) {
      Field type = typeIds[typeId];
      Types.MinorType minorType;
      String name = null;
      if (type == null) {
        minorType = Types.MinorType.values()[typeId];
      } else {
        minorType = Types.getMinorTypeForArrowType(type.getType());
        name = type.getName();
      }
      switch (minorType) {
        case NULL:
          return null;
      <#list vv.types as type>
        <#list type.minor as minor>
          <#assign name = minor.class?cap_first />
          <#assign fields = minor.fields!type.fields />
          <#assign uncappedName = name?uncap_first/>
          <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
        case ${name?upper_case}:
        return get${name}Vector(name<#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">, arrowType</#if>);
          </#if>
        </#list>
      </#list>
        case STRUCT:
          return getStruct();
        case LIST:
          return getList();
        case MAP:
          return getMap(name, arrowType);
        default:
          throw new UnsupportedOperationException("Cannot support type: " + MinorType.values()[typeId]);
      }
    }

    public Object getObject(int index) {
      ValueVector vector = getVector(index);
      if (vector != null) {
        return vector.isNull(index) ? null : vector.getObject(index);
      }
      return null;
    }

    public byte[] get(int index) {
      return null;
    }

    public void get(int index, ComplexHolder holder) {
    }

    public void get(int index, UnionHolder holder) {
      FieldReader reader = new UnionReader(UnionVector.this);
      reader.setPosition(index);
      holder.reader = reader;
    }

    public int getValueCount() {
      return valueCount;
    }

    /**
     * IMPORTANT: Union types always return non null as there is no validity buffer.
     *
     * To check validity correctly you must check the underlying vector.
     */
    public boolean isNull(int index) {
      return false;
    }

    @Override
    public int getNullCount() {
      return 0;
    }

    public int isSet(int index) {
      return isNull(index) ? 0 : 1;
    }

    UnionWriter writer;

    public void setValueCount(int valueCount) {
      this.valueCount = valueCount;
      while (valueCount > getTypeBufferValueCapacity()) {
        reallocTypeBuffer();
      }
      internalStruct.setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
      setSafe(index, holder, null);
    }

    public void setSafe(int index, UnionHolder holder, ArrowType arrowType) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getMinorType();
      switch (type) {
      <#list vv.types as type>
        <#list type.minor as minor>
          <#assign name = minor.class?cap_first />
          <#assign fields = minor.fields!type.fields />
          <#assign uncappedName = name?uncap_first/>
          <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
      case ${name?upper_case}:
        Nullable${name}Holder ${uncappedName}Holder = new Nullable${name}Holder();
        reader.read(${uncappedName}Holder);
        <#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
        setSafe(index, ${uncappedName}Holder, arrowType);
        <#else>
        setSafe(index, ${uncappedName}Holder);
        </#if>
        break;
          </#if>
        </#list>
      </#list>
      case STRUCT: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      case LIST: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      default:
        throw new UnsupportedOperationException();
      }
    }

    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
    <#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
    public void setSafe(int index, Nullable${name}Holder holder, ArrowType arrowType) {
      setType(index, MinorType.${name?upper_case});
      get${name}Vector(null, arrowType).setSafe(index, holder);
    }
    <#else>
    public void setSafe(int index, Nullable${name}Holder holder) {
      setType(index, MinorType.${name?upper_case});
      get${name}Vector(null).setSafe(index, holder);
    }
    </#if>
        </#if>
      </#list>
    </#list>

    public void setType(int index, MinorType type) {
      while (index >= getTypeBufferValueCapacity()) {
        reallocTypeBuffer();
      }
      typeBuffer.setByte(index * TYPE_WIDTH , (byte) type.ordinal());
    }

    private int getTypeBufferValueCapacity() {
      return capAtMaxInt(typeBuffer.capacity() / TYPE_WIDTH);
    }

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      ValueVector vec = getVector(index);
      if (vec == null) {
        return ArrowBufPointer.NULL_HASH_CODE;
      }
      return vec.hashCode(index, hasher);
    }

    @Override
    public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
      return visitor.visit(this, value);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return ValueVectorUtility.getToString(this, 0, getValueCount());
    }

    @Override
    public <T extends FieldVector> T addOrGet(String name, FieldType fieldType, Class<T> clazz) {
      return internalStruct.addOrGet(name, fieldType, clazz);
    }

    @Override
    public <T extends FieldVector> T getChild(String name, Class<T> clazz) {
      return internalStruct.getChild(name, clazz);
    }

    @Override
    public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
      return internalStruct.getChildVectorWithOrdinal(name);
    }

    @Override
    public int size() {
      return internalStruct.size();
    }

    @Override
    public void setInitialCapacity(int valueCount, double density) {
      for (final ValueVector vector : internalStruct) {
        if (vector instanceof DensityAwareVector) {
          ((DensityAwareVector) vector).setInitialCapacity(valueCount, density);
        } else {
          vector.setInitialCapacity(valueCount);
        }
      }
    }

  /**
   * Set the element at the given index to null. For UnionVector, it throws an UnsupportedOperationException
   * as nulls are not supported at the top level and isNull() always returns false.
   *
   * @param index position of element
   * @throws UnsupportedOperationException whenever invoked
   */
  @Override
  public void setNull(int index) {
    throw new UnsupportedOperationException("The method setNull() is not supported on UnionVector.");
  }
}
