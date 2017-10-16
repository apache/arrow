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
<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/UnionVector.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex;

<#include "/@includes/vv_imports.ftl" />
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ArrowBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.vector.BaseDataValueVector;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.util.OversizedAllocationException;

import static org.apache.arrow.vector.types.UnionMode.Sparse;



/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")


/**
 * A vector which can hold values of different types. It does so by using a MapVector which contains a vector for each
 * primitive type that is stored. MapVector is used in order to take advantage of its serialization/deserialization methods,
 * as well as the addOrGet method.
 *
 * For performance reasons, UnionVector stores a cached reference to each subtype vector, to avoid having to do the map lookup
 * each time the vector is accessed.
 * Source code generated using FreeMarker template ${.template_name}
 */
public class UnionVector implements FieldVector {

  private String name;
  private BufferAllocator allocator;
  int valueCount;

  MapVector internalMap;
  ArrowBuf typeBuffer;

  private NullableMapVector mapVector;
  private ListVector listVector;

  private FieldReader reader;

  private int singleType = 0;
  private ValueVector singleVector;

  private static final byte TYPE_WIDTH = 1;
  private final CallBack callBack;
  private int typeBufferAllocationSizeInBytes;

  public UnionVector(String name, BufferAllocator allocator, CallBack callBack) {
    this.name = name;
    this.allocator = allocator;
    this.internalMap = new MapVector("internal", allocator, new FieldType(false, ArrowType.Struct.INSTANCE, null, null), callBack);
    this.typeBuffer = allocator.getEmpty();
    this.callBack = callBack;
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
    internalMap.initializeChildrenFromFields(children);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return internalMap.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + 1 + ", got: " + ownBuffers.size());
    }

    ArrowBuf buffer = ownBuffers.get(0);
    typeBuffer.release();
    typeBuffer = buffer.retain(allocator);
    typeBufferAllocationSizeInBytes = typeBuffer.capacity();
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(1);

    typeBuffer.readerIndex(0);
    typeBuffer.writerIndex(valueCount * TYPE_WIDTH);

    result.add(typeBuffer);

    return result;
  }

  @Override
  @Deprecated
  public List<BufferBacked> getFieldInnerVectors() {
     throw new UnsupportedOperationException("There are no inner vectors. Use geFieldBuffers");
  }

  private String fieldName(MinorType type) {
    return type.name().toLowerCase();
  }

  private FieldType fieldType(MinorType type) {
    return FieldType.nullable(type.getType());
  }

  private <T extends FieldVector> T addOrGet(MinorType minorType, Class<T> c) {
    return internalMap.addOrGet(fieldName(minorType), fieldType(minorType), c);
  }

  @Override
  public long getValidityBufferAddress() {
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

  @Override
  public ArrowBuf getValidityBuffer() { return typeBuffer; }

  @Override
  public ArrowBuf getDataBuffer() { throw new UnsupportedOperationException(); }

  @Override
  public ArrowBuf getOffsetBuffer() { throw new UnsupportedOperationException(); }

  public NullableMapVector getMap() {
    if (mapVector == null) {
      int vectorCount = internalMap.size();
      mapVector = addOrGet(MinorType.MAP, NullableMapVector.class);
      if (internalMap.size() > vectorCount) {
        mapVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return mapVector;
  }
  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#assign lowerCaseName = name?lower_case/>
      <#if !minor.typeParams?? >

  private Nullable${name}Vector ${uncappedName}Vector;

  public Nullable${name}Vector get${name}Vector() {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalMap.size();
      ${uncappedName}Vector = addOrGet(MinorType.${name?upper_case}, Nullable${name}Vector.class);
      if (internalMap.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return ${uncappedName}Vector;
  }
      </#if>
    </#list>
  </#list>

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalMap.size();
      listVector = addOrGet(MinorType.LIST, ListVector.class);
      if (internalMap.size() > vectorCount) {
        listVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return listVector;
  }

  public int getTypeValue(int index) {
    return typeBuffer.getByte(index * TYPE_WIDTH);
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    internalMap.allocateNew();
    try {
      allocateTypeBuffer();
    } catch (Exception e) {
      clear();
      throw e;
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean safe = internalMap.allocateNewSafe();
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
    internalMap.reAlloc();
    reallocTypeBuffer();
  }

  private void reallocTypeBuffer() {
    final int currentBufferCapacity = typeBuffer.capacity();
    long baseSize  = typeBufferAllocationSizeInBytes;

    if (baseSize < (long)currentBufferCapacity) {
      baseSize = (long)currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
    newBuf.setBytes(0, typeBuffer, 0, currentBufferCapacity);
    final int halfNewCapacity = newBuf.capacity() / 2;
    newBuf.setZero(halfNewCapacity, halfNewCapacity);
    typeBuffer.release(1);
    typeBuffer = newBuf;
    typeBufferAllocationSizeInBytes = (int)newAllocationSize;
  }

  @Override
  public void setInitialCapacity(int numRecords) { }

  @Override
  public int getValueCapacity() {
    return Math.min(getTypeBufferValueCapacity(), internalMap.getValueCapacity());
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public void clear() {
    valueCount = 0;
    typeBuffer.release();
    typeBuffer = allocator.getEmpty();
    internalMap.clear();
  }

  @Override
  public Field getField() {
    List<org.apache.arrow.vector.types.pojo.Field> childFields = new ArrayList<>();
    List<FieldVector> children = internalMap.getChildren();
    int[] typeIds = new int[children.size()];
    for (ValueVector v : children) {
      typeIds[childFields.size()] = v.getMinorType().ordinal();
      childFields.add(v.getField());
    }
    return new Field(name, FieldType.nullable(new ArrowType.Union(Sparse, typeIds)), childFields);
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

  public void copyFrom(int inIndex, int outIndex, UnionVector from) {
    from.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(from.reader, writer);
  }

  public void copyFromSafe(int inIndex, int outIndex, UnionVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public FieldVector addVector(FieldVector v) {
    String name = v.getMinorType().name().toLowerCase();
    Preconditions.checkState(internalMap.getChild(name) == null, String.format("%s vector already exists", name));
    final FieldVector newVector = internalMap.addOrGet(name, v.getField().getFieldType(), v.getClass());
    v.makeTransferPair(newVector).transfer();
    internalMap.putChild(name, newVector);
    if (callBack != null) {
      callBack.doWork();
    }
    return newVector;
  }

  private class TransferImpl implements TransferPair {
    private final TransferPair internalMapVectorTransferPair;
    private final UnionVector to;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      to = new UnionVector(name, allocator, callBack);
      internalMapVectorTransferPair = internalMap.makeTransferPair(to.internalMap);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
      internalMapVectorTransferPair = internalMap.makeTransferPair(to.internalMap);
    }

    @Override
    public void transfer() {
      to.clear();
      to.typeBuffer = typeBuffer.transferOwnership(to.allocator).buffer;
      internalMapVectorTransferPair.transfer();
      to.valueCount = valueCount;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.clear();
      internalMapVectorTransferPair.splitAndTransfer(startIndex, length);
      final int startPoint = startIndex * TYPE_WIDTH;
      final int sliceLength = length * TYPE_WIDTH;
      to.typeBuffer = typeBuffer.slice(startPoint, sliceLength).transferOwnership(to.allocator).buffer;
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
  @Deprecated
  public Accessor getAccessor() {
    throw new UnsupportedOperationException("Accessor is not supported for reading from UNION");
  }

  @Override
  @Deprecated
  public Mutator getMutator() {
    throw new UnsupportedOperationException("Mutator is not supported for writing to UNION");
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

    return (valueCount * TYPE_WIDTH) + internalMap.getBufferSize();
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
    ImmutableList.Builder<ArrowBuf> builder = ImmutableList.builder();
    builder.add(typeBuffer);
    builder.add(internalMap.getBuffers(clear));
    List<ArrowBuf> list = builder.build();
    return list.toArray(new ArrowBuf[list.size()]);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    List<ValueVector> vectors = Lists.newArrayList(internalMap.iterator());
    return vectors.iterator();
  }


    public Object getObject(int index) {
      int type = typeBuffer.getByte(index * TYPE_WIDTH);
      switch (MinorType.values()[type]) {
      case NULL:
        return null;
      <#list vv.types as type>
        <#list type.minor as minor>
          <#assign name = minor.class?cap_first />
          <#assign fields = minor.fields!type.fields />
          <#assign uncappedName = name?uncap_first/>
          <#if !minor.typeParams?? >
      case ${name?upper_case}:
          return get${name}Vector().getObject(index);
          </#if>
        </#list>
      </#list>
      case MAP:
        return getMap().getObject(index);
      case LIST:
        return getList().getObject(index);
      default:
        throw new UnsupportedOperationException("Cannot support type: " + MinorType.values()[type]);
      }
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

    public boolean isNull(int index) {
      return (typeBuffer.getByte(index * TYPE_WIDTH) == 0);
    }

    @Override
    public int getNullCount() {
      int nullCount = 0;
      for (int i = 0; i < getValueCount(); i++) {
        if (isNull(i)) {
          nullCount++;
        }
      }
      return nullCount;
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
      internalMap.setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
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
          <#if !minor.typeParams?? >
      case ${name?upper_case}:
        Nullable${name}Holder ${uncappedName}Holder = new Nullable${name}Holder();
        reader.read(${uncappedName}Holder);
        setSafe(index, ${uncappedName}Holder);
        break;
          </#if>
        </#list>
      </#list>
      case MAP: {
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
        <#if !minor.typeParams?? >
    public void setSafe(int index, Nullable${name}Holder holder) {
      setType(index, MinorType.${name?upper_case});
      get${name}Vector().setSafe(index, holder);
    }

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
      return (int) ((typeBuffer.capacity() * 1.0) / TYPE_WIDTH);
    }
}
