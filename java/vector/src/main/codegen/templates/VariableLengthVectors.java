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

import java.lang.Override;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<#if type.major == "VarLen">
<@pp.changeOutputFile name="/org/apache/arrow/vector/${minor.class}Vector.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * ${minor.class}Vector implements a vector of variable width values.  Elements in the vector
 * are accessed by position from the logical start of the vector.  A fixed width offsetVector
 * is used to convert an element's position to it's offset from the start of the (0-based)
 * ArrowBuf.  Size is inferred by adjacent elements.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
public final class ${minor.class}Vector extends BaseDataValueVector implements VariableWidthVector{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private static final int DEFAULT_RECORD_BYTE_COUNT = 8;
  private static final int INITIAL_BYTE_COUNT = 4096 * DEFAULT_RECORD_BYTE_COUNT;
  private static final int MIN_BYTE_COUNT = 4096;

  public final static String OFFSETS_VECTOR_NAME = "$offsets$";
  final UInt${type.width}Vector offsetVector = new UInt${type.width}Vector(OFFSETS_VECTOR_NAME, allocator);

  private final Accessor accessor;
  private final Mutator mutator;

  private final UInt${type.width}Vector.Accessor oAccessor;

  private int allocationSizeInBytes = INITIAL_BYTE_COUNT;
  private int allocationMonitor = 0;

  <#if minor.class == "Decimal">

  private final int precision;
  private final int scale;

  public ${minor.class}Vector(String name, BufferAllocator allocator, int precision, int scale) {
    super(name, allocator);
    this.oAccessor = offsetVector.getAccessor();
    this.accessor = new Accessor();
    this.mutator = new Mutator();
    this.precision = precision;
    this.scale = scale;
  }
  <#else>

  public ${minor.class}Vector(String name, BufferAllocator allocator) {
    super(name, allocator);
    this.oAccessor = offsetVector.getAccessor();
    this.accessor = new Accessor();
    this.mutator = new Mutator();
  }
  </#if>

  @Override
  public Field getField() {
    throw new UnsupportedOperationException("internal vector");
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.${minor.class?upper_case};
  }

  @Override
  public FieldReader getReader(){
    throw new UnsupportedOperationException("internal vector");
  }

  @Override
  public int getBufferSize(){
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return offsetVector.getBufferSize() + data.writerIndex();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    final int idx = offsetVector.getAccessor().get(valueCount);
    return offsetVector.getBufferSizeFor(valueCount + 1) + idx;
  }

  @Override
  public int getValueCapacity(){
    return Math.max(offsetVector.getValueCapacity() - 1, 0);
  }

  @Override
  public int getByteCapacity(){
    return data.capacity();
  }

  @Override
  public int getCurrentSizeInBytes() {
    return offsetVector.getAccessor().get(getAccessor().getValueCount());
  }

  /**
   * Return the number of bytes contained in the current var len byte vector.
   * @return the number of bytes contained in the current var len byte vector
   */
  public int getVarByteLength(){
    final int valueCount = getAccessor().getValueCount();
    if(valueCount == 0) {
      return 0;
    }
    return offsetVector.getAccessor().get(valueCount);
  }

  @Override
  public void clear() {
    super.clear();
    offsetVector.clear();
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(offsetVector.getBuffers(false), super.getBuffers(false), ArrowBuf.class);
    if (clear) {
      // does not make much sense but we have to retain buffers even when clear is set. refactor this interface.
      for (final ArrowBuf buffer:buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  public long getOffsetAddr(){
    return offsetVector.getBuffer().memoryAddress();
  }

  public UInt${type.width}Vector getOffsetVector(){
    return offsetVector;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator){
        return new TransferImpl(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((${minor.class}Vector) to);
  }

  public void transferTo(${minor.class}Vector target){
    target.clear();
    this.offsetVector.transferTo(target.offsetVector);
    target.data = data.transferOwnership(target.allocator).buffer;
    target.data.writerIndex(data.writerIndex());
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${minor.class}Vector target) {
    UInt${type.width}Vector.Accessor offsetVectorAccessor = this.offsetVector.getAccessor();
    final int startPoint = offsetVectorAccessor.get(startIndex);
    final int sliceLength = offsetVectorAccessor.get(startIndex + length) - startPoint;
    target.clear();
    target.offsetVector.allocateNew(length + 1);
    offsetVectorAccessor = this.offsetVector.getAccessor();
    final UInt4Vector.Mutator targetOffsetVectorMutator = target.offsetVector.getMutator();
    for (int i = 0; i < length + 1; i++) {
      targetOffsetVectorMutator.set(i, offsetVectorAccessor.get(startIndex + i) - startPoint);
    }
    target.data = data.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
    target.getMutator().setValueCount(length);
}

  protected void copyFrom(int fromIndex, int thisIndex, ${minor.class}Vector from){
    final UInt4Vector.Accessor fromOffsetVectorAccessor = from.offsetVector.getAccessor();
    final int start = fromOffsetVectorAccessor.get(fromIndex);
    final int end = fromOffsetVectorAccessor.get(fromIndex + 1);
    final int len = end - start;

    final int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width});
    from.data.getBytes(start, data, outputStart, len);
    offsetVector.data.set${(minor.javaType!type.javaType)?cap_first}( (thisIndex+1) * ${type.width}, outputStart + len);
  }

  public boolean copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    final UInt${type.width}Vector.Accessor fromOffsetVectorAccessor = from.offsetVector.getAccessor();
    final int start = fromOffsetVectorAccessor.get(fromIndex);
    final int end =   fromOffsetVectorAccessor.get(fromIndex + 1);
    final int len = end - start;
    final int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width});

    while(data.capacity() < outputStart + len) {
        reAlloc();
    }

    offsetVector.getMutator().setSafe(thisIndex + 1, outputStart + len);
    from.data.getBytes(start, data, outputStart, len);
    return true;
  }

  private class TransferImpl implements TransferPair{
    ${minor.class}Vector to;

    public TransferImpl(String name, BufferAllocator allocator){
      <#if minor.class == "Decimal">
      to = new ${minor.class}Vector(name, allocator, precision, scale);
      <#else>
      to = new ${minor.class}Vector(name, allocator);
      </#if>
    }

    public TransferImpl(${minor.class}Vector to){
      this.to = to;
    }

    @Override
    public ${minor.class}Vector getTo(){
      return to;
    }

    @Override
    public void transfer(){
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, ${minor.class}Vector.this);
    }
  }

  @Override
  public void setInitialCapacity(final int valueCount) {
    final long size = 1L * valueCount * ${type.width};
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }
    allocationSizeInBytes = (int)size;
    offsetVector.setInitialCapacity(valueCount + 1);
  }

  @Override
  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    long curAllocationSize = allocationSizeInBytes;
    if (allocationMonitor > 10) {
      curAllocationSize = Math.max(MIN_BYTE_COUNT, curAllocationSize / 2);
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      curAllocationSize = curAllocationSize * 2L;
      allocationMonitor = 0;
    }

    if (curAllocationSize > MAX_ALLOCATION_SIZE) {
      return false;
    }

    clear();
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    try {
      final int requestedSize = (int)curAllocationSize;
      data = allocator.buffer(requestedSize);
      allocationSizeInBytes = requestedSize;
      offsetVector.allocateNew();
    } catch (OutOfMemoryException e) {
      clear();
      return false;
    }
    data.readerIndex(0);
    offsetVector.zeroVector();
    return true;
  }

  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    clear();
    assert totalBytes >= 0;
    try {
      data = allocator.buffer(totalBytes);
      offsetVector.allocateNew(valueCount + 1);
    } catch (RuntimeException e) {
      clear();
      throw e;
    }
    data.readerIndex(0);
    allocationSizeInBytes = totalBytes;
    offsetVector.zeroVector();
  }

  @Override
  public void reset() {
    allocationSizeInBytes = INITIAL_BYTE_COUNT;
    allocationMonitor = 0;
    data.readerIndex(0);
    offsetVector.zeroVector();
    super.reset();
  }

  public void reAlloc() {
    offsetVector.reAlloc();
    final long newAllocationSize = allocationSizeInBytes*2L;
    if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
      throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
    }

    final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
    newBuf.setBytes(0, data, 0, data.capacity());
    data.release();
    data = newBuf;
    allocationSizeInBytes = (int)newAllocationSize;
  }

  public void decrementAllocationMonitor() {
    if (allocationMonitor > 0) {
      allocationMonitor = 0;
    }
    --allocationMonitor;
  }

  private void incrementAllocationMonitor() {
    ++allocationMonitor;
  }

  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public final class Accessor extends BaseValueVector.BaseAccessor implements VariableWidthAccessor {
    final UInt${type.width}Vector.Accessor oAccessor = offsetVector.getAccessor();
    public long getStartEnd(int index){
      return oAccessor.getTwoAsLong(index);
    }

    public byte[] get(int index) {
      assert index >= 0;
      final int startIdx = oAccessor.get(index);
      final int length = oAccessor.get(index + 1) - startIdx;
      assert length >= 0;
      final byte[] dst = new byte[length];
      data.getBytes(startIdx, dst, 0, length);
      return dst;
    }

    @Override
    public int getValueLength(int index) {
      final UInt${type.width}Vector.Accessor offsetVectorAccessor = offsetVector.getAccessor();
      return offsetVectorAccessor.get(index + 1) - offsetVectorAccessor.get(index);
    }

    public void get(int index, ${minor.class}Holder holder){
      holder.start = oAccessor.get(index);
      holder.end = oAccessor.get(index + 1);
      holder.buffer = data;
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.start = oAccessor.get(index);
      holder.end = oAccessor.get(index + 1);
      holder.buffer = data;
    }


    <#switch minor.class>
    <#case "VarChar">
    @Override
    public ${friendlyType} getObject(int index) {
      Text text = new Text();
      text.set(get(index));
      return text;
    }
    <#break>
    <#case "Decimal">
    @Override
    public ${friendlyType} getObject(int index) {
      return new BigDecimal(new BigInteger(get(index)), scale);
    }
    <#break>
    <#default>
    @Override
    public ${friendlyType} getObject(int index) {
      return get(index);
    }
    </#switch>

    @Override
    public int getValueCount() {
      return Math.max(offsetVector.getAccessor().getValueCount()-1, 0);
    }

    @Override
    public boolean isNull(int index){
      return false;
    }

    public UInt${type.width}Vector getOffsetVector(){
      return offsetVector;
    }
  }

  /**
   * Mutable${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width offsetVector
   * is used to convert an element's position to it's offset from the start of the (0-based)
   * ArrowBuf.  Size is inferred by adjacent elements.
   *   The width of each element is ${type.width} byte(s)
   *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public final class Mutator extends BaseValueVector.BaseMutator implements VariableWidthVector.VariableWidthMutator {

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    protected void set(int index, byte[] bytes) {
      assert index >= 0;
      final int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + bytes.length);
      data.setBytes(currentOffset, bytes, 0, bytes.length);
    }

    public void setSafe(int index, byte[] bytes) {
      assert index >= 0;

      final int currentOffset = offsetVector.getAccessor().get(index);
      while (data.capacity() < currentOffset + bytes.length) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, currentOffset + bytes.length);
      data.setBytes(currentOffset, bytes, 0, bytes.length);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     * @param start   start index of bytes to write
     * @param length  length of bytes to write
     */
    protected void set(int index, byte[] bytes, int start, int length) {
      assert index >= 0;
      final int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, bytes, start, length);
    }

    public void setSafe(int index, ByteBuffer bytes, int start, int length) {
      assert index >= 0;

      int currentOffset = offsetVector.getAccessor().get(index);

      while (data.capacity() < currentOffset + length) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, currentOffset + length);
      data.setBytes(currentOffset, bytes, start, length);
    }

    public void setSafe(int index, byte[] bytes, int start, int length) {
      assert index >= 0;

      final int currentOffset = offsetVector.getAccessor().get(index);

      while (data.capacity() < currentOffset + length) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, currentOffset + length);
      data.setBytes(currentOffset, bytes, start, length);
    }

    @Override
    public void setValueLengthSafe(int index, int length) {
      final int offset = offsetVector.getAccessor().get(index);
      while(data.capacity() < offset + length ) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, offsetVector.getAccessor().get(index) + length);
    }


    public void setSafe(int index, int start, int end, ArrowBuf buffer){
      final int len = end - start;
      final int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});

      while(data.capacity() < outputStart + len) {
        reAlloc();
      }

      offsetVector.getMutator().setSafe( index+1,  outputStart + len);
      buffer.getBytes(start, data, outputStart, len);
    }

    public void setSafe(int index, Nullable${minor.class}Holder holder){
      assert holder.isSet == 1;

      final int start = holder.start;
      final int end =   holder.end;
      final int len = end - start;

      int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});

      while(data.capacity() < outputStart + len) {
        reAlloc();
      }

      holder.buffer.getBytes(start, data, outputStart, len);
      offsetVector.getMutator().setSafe( index+1,  outputStart + len);
    }

    public void setSafe(int index, ${minor.class}Holder holder){
      final int start = holder.start;
      final int end =   holder.end;
      final int len = end - start;
      final int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});

      while(data.capacity() < outputStart + len) {
        reAlloc();
      }

      holder.buffer.getBytes(start, data, outputStart, len);
      offsetVector.getMutator().setSafe( index+1,  outputStart + len);
    }

    protected void set(int index, int start, int length, ArrowBuf buffer){
      assert index >= 0;
      final int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      final ArrowBuf bb = buffer.slice(start, length);
      data.setBytes(currentOffset, bb);
    }

    protected void set(int index, Nullable${minor.class}Holder holder){
      final int length = holder.end - holder.start;
      final int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, holder.buffer, holder.start, length);
    }

    protected void set(int index, ${minor.class}Holder holder){
      final int length = holder.end - holder.start;
      final int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, holder.buffer, holder.start, length);
    }

    @Override
    public void setValueCount(int valueCount) {
      if (valueCount == 0) {
        // if no values in vector, don't try to retrieve the current value count.
        offsetVector.getMutator().setValueCount(0);
      } else {
        final int currentByteCapacity = getByteCapacity();
        final int idx = offsetVector.getAccessor().get(valueCount);
        data.writerIndex(idx);
        if (currentByteCapacity > idx * 2) {
          incrementAllocationMonitor();
        } else if (allocationMonitor > 0) {
          allocationMonitor = 0;
        }
        VectorTrimmer.trim(data, idx);
        offsetVector.getMutator().setValueCount(valueCount+1);
      }
    }

    @Override
    public void generateTestData(int size){
      boolean even = true;
      <#switch minor.class>
      <#case "Var16Char">
      final java.nio.charset.Charset charset = Charsets.UTF_16;
      <#break>
      <#case "VarChar">
      <#default>
      final java.nio.charset.Charset charset = Charsets.UTF_8;
      </#switch>
      final byte[] evenValue = new String("aaaaa").getBytes(charset);
      final byte[] oddValue = new String("bbbbbbbbbb").getBytes(charset);
      for(int i =0; i < size; i++, even = !even){
        set(i, even ? evenValue : oddValue);
        }
      setValueCount(size);
    }
  }
}

</#if> <#-- type.major -->
</#list>
</#list>
