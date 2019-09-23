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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/DenseUnionVector.java" />


<#include "/@includes/license.ftl" />

        package org.apache.arrow.vector.complex;

<#include "/@includes/vv_imports.ftl" />
        import io.netty.buffer.ArrowBuf;
        import java.util.ArrayList;
        import java.util.Collections;
        import java.util.Iterator;
        import org.apache.arrow.vector.compare.VectorVisitor;
        import org.apache.arrow.vector.complex.impl.ComplexCopier;
        import org.apache.arrow.vector.util.CallBack;
        import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
        import org.apache.arrow.memory.BaseAllocator;
        import org.apache.arrow.vector.BaseValueVector;
        import org.apache.arrow.vector.util.OversizedAllocationException;
        import org.apache.arrow.util.Preconditions;

        import static org.apache.arrow.vector.types.UnionMode.Dense;



/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")


/**
 * A vector which can hold values of different types. It does so by using a StructVector which contains a vector for each
 * primitive type that is stored. StructVector is used in order to take advantage of its serialization/deserialization methods,
 * as well as the addOrGet method.
 *
 * For performance reasons, DenseUnionVector stores a cached reference to each subtype vector, to avoid having to do the struct lookup
 * each time the vector is accessed.
 * Source code generated using FreeMarker template ${.template_name}
 */
public class DenseUnionVector extends UnionVector {

  private static final byte OFFSET_WIDTH = 4;

  private ArrowBuf offsetBuffer;
  private long offsetBufferAllocationSizeInBytes;

  public static DenseUnionVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(new ArrowType.Union(
            UnionMode.Dense, null));
    return new DenseUnionVector(name, allocator, fieldType, null);
  }

  public DenseUnionVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, fieldType, callBack);
    this.offsetBuffer = allocator.getEmpty();
    this.offsetBufferAllocationSizeInBytes = BaseValueVector.INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 2) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + 2 + ", got: " + ownBuffers.size());
    }

    ArrowBuf buffer = ownBuffers.get(0);
    typeBuffer.getReferenceManager().release();
    typeBuffer = buffer.getReferenceManager().retain(buffer, allocator);
    typeBufferAllocationSizeInBytes = typeBuffer.capacity();

    buffer = ownBuffers.get(1);
    offsetBuffer.getReferenceManager().release();
    offsetBuffer = buffer.getReferenceManager().retain(buffer, allocator);
    offsetBufferAllocationSizeInBytes = offsetBuffer.capacity();

    this.valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(1);
    setReaderAndWriterIndex();
    result.add(typeBuffer);
    result.add(offsetBuffer);

    return result;
  }

  protected void setReaderAndWriterIndex() {
    typeBuffer.readerIndex(0);
    typeBuffer.writerIndex(valueCount * TYPE_WIDTH);

    offsetBuffer.readerIndex(0);
    offsetBuffer.writerIndex(valueCount * OFFSET_WIDTH);
  }

  @Override
  public long getOffsetBufferAddress() {
    return offsetBuffer.memoryAddress();
  }

  @Override
  public ArrowBuf getOffsetBuffer() { return offsetBuffer; }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    /* new allocation -- clear the current buffers */
    clear();
    internalStruct.allocateNew();
    try {
      allocateTypeBuffer();
      allocateOffsetBuffer();
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
      allocateOffsetBuffer();
    } catch (Exception e) {
      clear();
      return  false;
    }

    return true;
  }

  private void allocateOffsetBuffer() {
    offsetBuffer = allocator.buffer(offsetBufferAllocationSizeInBytes);
    offsetBuffer.readerIndex(0);
    offsetBuffer.setZero(0, offsetBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    internalStruct.reAlloc();
    reallocTypeBuffer();
    reallocOffsetBuffer();
  }

  private void reallocOffsetBuffer() {
    final long currentBufferCapacity = offsetBuffer.capacity();
    long baseSize  = offsetBufferAllocationSizeInBytes;

    if (baseSize < (long) currentBufferCapacity) {
      baseSize = (long) currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setBytes(0, offsetBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    offsetBuffer.getReferenceManager().release(1);
    offsetBuffer = newBuf;
    offsetBufferAllocationSizeInBytes = (int) newAllocationSize;
  }

  @Override
  public int getValueCapacity() {
    return (int) Math.min(Math.min(getTypeBufferValueCapacity(), getOffsetBufferValueCapacity()),
            internalStruct.getValueCapacity());
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
    offsetBuffer.getReferenceManager().release();
    offsetBuffer = allocator.getEmpty();
    internalStruct.clear();
  }

  @Override
  public void reset() {
    valueCount = 0;
    typeBuffer.setZero(0, typeBuffer.capacity());
    offsetBuffer.setZero(0, offsetBuffer.capacity());
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
      fieldType = FieldType.nullable(new ArrowType.Union(Dense, typeIds));
    } else {
      final UnionMode mode = ((ArrowType.Union)this.fieldType.getType()).getMode();
      fieldType = new FieldType(this.fieldType.isNullable(), new ArrowType.Union(mode, typeIds),
              this.fieldType.getDictionary(), this.fieldType.getMetadata());
    }

    return new Field(name, fieldType, childFields);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new org.apache.arrow.vector.complex.DenseUnionVector.TransferImpl(ref, allocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((DenseUnionVector) target);
  }

  @Override
  public void copyFrom(int inIndex, int outIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    DenseUnionVector fromCast = (DenseUnionVector) from;
    int inOffset = fromCast.offsetBuffer.getInt(inIndex * OFFSET_WIDTH);
    fromCast.getReader().setPosition(inOffset);
    int outOffset = offsetBuffer.getInt(outIndex * OFFSET_WIDTH);
    getWriter().setPosition(outOffset);
    ComplexCopier.copy(fromCast.reader, writer);
  }

  private class TransferImpl implements TransferPair {
    private final TransferPair[] internalTransferPairs = new TransferPair[Types.MinorType.values().length];
    private final DenseUnionVector to;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      to = new DenseUnionVector(name, allocator, null, callBack);
      internalStruct.makeTransferPair(to.internalStruct);
      createTransferPairs();
    }

    public TransferImpl(DenseUnionVector to) {
      this.to = to;
      internalStruct.makeTransferPair(to.internalStruct);
      createTransferPairs();
    }

    private void createTransferPairs() {
      for (int i = 0; i < internalStruct.getField().getChildren().size(); i++) {
        ValueVector srcVec = internalStruct.getVectorById(i);
        ValueVector dstVec = to.internalStruct.getVectorById(i);
        internalTransferPairs[srcVec.getMinorType().ordinal()] = srcVec.makeTransferPair(dstVec);
      }
    }

    @Override
    public void transfer() {
      to.clear();
      final ReferenceManager refManager = typeBuffer.getReferenceManager();
      to.typeBuffer = refManager.transferOwnership(typeBuffer, to.allocator).getTransferredBuffer();
      to.offsetBuffer = refManager.transferOwnership(offsetBuffer, to.allocator).getTransferredBuffer();
      for (int i = 0; i < MinorType.values().length; i++) {
        if (internalTransferPairs[i] != null) {
          internalTransferPairs[i].transfer();
        }
      }
      to.valueCount = valueCount;
      clear();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.clear();
      int startPoint = startIndex * TYPE_WIDTH;
      int sliceLength = length * TYPE_WIDTH;
      ArrowBuf slicedBuffer = typeBuffer.slice(startPoint, sliceLength);
      ReferenceManager refManager = slicedBuffer.getReferenceManager();
      to.typeBuffer = refManager.transferOwnership(slicedBuffer, to.allocator).getTransferredBuffer();

      while (to.offsetBuffer.capacity() < length * OFFSET_WIDTH) {
        to.reallocOffsetBuffer();
      }

      int [] typeCounts = new int[MinorType.values().length];
      int [] typeStarts = new int[Types.MinorType.values().length];
      for (int i = 0; i < typeCounts.length; i++) {
        typeCounts[i] = 0;
        typeStarts[i] = -1;
      }

      for (int i = startIndex; i < startIndex + length; i++) {
        int ord = typeBuffer.getByte(i);
        to.offsetBuffer.setInt((i - startIndex) * OFFSET_WIDTH, typeCounts[ord]);
        typeCounts[ord] += 1;
        if (typeStarts[ord] == -1) {
          typeStarts[ord] = offsetBuffer.getInt(i * OFFSET_WIDTH);
        }
      }
      to.setValueCount(length);

      // transfer vector values
      for (int i = 0; i < MinorType.values().length; i++) {
        if (typeCounts[i] > 0 && typeStarts[i] != -1) {
          internalTransferPairs[i].splitAndTransfer(typeStarts[i], typeCounts[i]);
        }
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, DenseUnionVector.this);
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

    return valueCount * TYPE_WIDTH + valueCount * OFFSET_WIDTH + internalStruct.getBufferSize();
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

    return (int) bufferSize + valueCount * TYPE_WIDTH + valueCount * OFFSET_WIDTH;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    List<ArrowBuf> list = new java.util.ArrayList<>();
    setReaderAndWriterIndex();
    if (getBufferSize() != 0) {
      list.add(typeBuffer);
      list.add(offsetBuffer);
      list.addAll(java.util.Arrays.asList(internalStruct.getBuffers(clear)));
    }
    if (clear) {
      valueCount = 0;
      typeBuffer.getReferenceManager().retain();
      typeBuffer.getReferenceManager().release();
      typeBuffer = allocator.getEmpty();
      offsetBuffer.getReferenceManager().retain();
      offsetBuffer.getReferenceManager().release();
      offsetBuffer = allocator.getEmpty();
    }
    return list.toArray(new ArrowBuf[list.size()]);
  }

  public Object getObject(int index) {
    ValueVector vector = getVector(index);
    if (vector != null) {
      int offset = offsetBuffer.getInt(index * OFFSET_WIDTH);
      return vector.getObject(offset);
    }
    return null;
  }

  public void get(int index, UnionHolder holder) {
    FieldReader reader = new UnionReader(DenseUnionVector.this);
    int offset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    reader.setPosition(offset);
    holder.reader = reader;
  }

  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    while (valueCount > getTypeBufferValueCapacity()) {
      reallocTypeBuffer();
      reallocOffsetBuffer();
    }
    internalStruct.setValueCount(valueCount);
  }

  public void setSafe(int index, UnionHolder holder) {
    FieldReader reader = holder.reader;
    if (writer == null) {
      writer = new UnionWriter(DenseUnionVector.this);
    }
    int offset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    writer.setPosition(offset);
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
        <#if !minor.typeParams?? >
  public void setSafe(int index, Nullable${name}Holder holder) {
    setType(index, MinorType.${name?upper_case});
    while (index >= getOffsetBufferValueCapacity()) {
      reallocOffsetBuffer();
    }
    ${name}Vector vector = get${name}Vector();
    int offset = vector.getValueCount();
    vector.setValueCount(offset + 1);
    vector.setSafe(offset, holder);
    offsetBuffer.setInt(index * OFFSET_WIDTH, offset);
  }

        </#if>
      </#list>
    </#list>

  private long getOffsetBufferValueCapacity() {
    return offsetBuffer.capacity() / OFFSET_WIDTH;
  }

  @Override
  public int hashCode(int index) {
    int offset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    return getVector(index).hashCode(offset);
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }
}
