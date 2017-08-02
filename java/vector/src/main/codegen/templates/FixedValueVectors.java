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

import org.apache.arrow.vector.util.DecimalUtility;

import java.lang.Override;
import java.util.concurrent.TimeUnit;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#assign className = "${minor.class}Vector" />

<#if type.major == "Fixed">
<@pp.changeOutputFile name="/org/apache/arrow/vector/${className}.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * ${minor.class} implements a vector of fixed width values.  Elements in the vector are accessed
 * by position, starting from the logical start of the vector.  Values should be pushed onto the
 * vector sequentially, but may be randomly accessed.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
public final class ${className} extends BaseDataValueVector implements FixedWidthVector{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

  public static final int TYPE_WIDTH = ${type.width};

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * ${type.width};
  private int allocationMonitor = 0;
  <#if minor.typeParams??>

    <#assign typeParams = minor.typeParams?reverse />
    <#list typeParams as typeParam>
  private final ${typeParam.type} ${typeParam.name};
    </#list>

  public ${className}(String name, BufferAllocator allocator<#list typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    super(name, allocator);
    <#list typeParams as typeParam>
    this.${typeParam.name} = ${typeParam.name};
    </#list>
  }
  <#else>
  public ${className}(String name, BufferAllocator allocator) {
    super(name, allocator);
  }
  </#if>

  @Override
  public MinorType getMinorType() {
    return MinorType.${minor.class?upper_case};
  }

  @Override
  public Field getField() {
        throw new UnsupportedOperationException("internal vector");
  }

  @Override
  public FieldReader getReader(){
        throw new UnsupportedOperationException("non-nullable vectors cannot be used in readers");
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return valueCount * ${type.width};
  }

  @Override
  public int getValueCapacity(){
    return (int) (data.capacity() *1.0 / ${type.width});
  }

  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  @Override
  public Mutator getMutator(){
    return mutator;
  }

  int getAllocationSize() {
    return allocationSizeInBytes;
  }

  @Override
  public void setInitialCapacity(final int valueCount) {
    final long size = 1L * valueCount * ${type.width};
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }
    allocationSizeInBytes = (int)size;
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
      curAllocationSize = Math.max(8, curAllocationSize / 2);
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      curAllocationSize = allocationSizeInBytes * 2L;
      allocationMonitor = 0;
    }

    try{
      allocateBytes(curAllocationSize);
    } catch (RuntimeException ex) {
      return false;
    }
    return true;
  }

  /**
   * Allocate a new buffer that supports setting at least the provided number of values. May actually be sized bigger
   * depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
   *
   * Note that the maximum number of values a vector can allocate is Integer.MAX_VALUE / value width.
   *
   * @param valueCount the number of values to allocate for
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the new buffer
   */
  @Override
  public void allocateNew(final int valueCount) {
    allocateBytes(valueCount * ${type.width});
  }

  @Override
  public void reset() {
    allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * ${type.width};
    allocationMonitor = 0;
    zeroVector();
    super.reset();
  }

  private void allocateBytes(final long size) {
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int)size;
    clear();
    data = allocator.buffer(curSize);
    data.readerIndex(0);
    allocationSizeInBytes = curSize;
  }

  /**
   * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's buffer with new buffer, and release old one
   *
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the new buffer
   */
  public void reAlloc() {
    final long newAllocationSize = allocationSizeInBytes * 2L;
    if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
      throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
    }

    logger.debug("Reallocating vector [{}]. # of bytes: [{}] -> [{}]", name, allocationSizeInBytes, newAllocationSize);
    final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
    newBuf.setBytes(0, data, 0, data.capacity());
    final int halfNewCapacity = newBuf.capacity() / 2;
    newBuf.setZero(halfNewCapacity, halfNewCapacity);
    newBuf.writerIndex(data.writerIndex());
    data.release(1);
    data = newBuf;
    allocationSizeInBytes = (int)newAllocationSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  public TransferPair getTransferPair(BufferAllocator allocator){
    return new TransferImpl(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((${className}) to);
  }

  public void transferTo(${className} target){
    target.clear();
    target.data = data.transferOwnership(target.allocator).buffer;
    target.data.writerIndex(data.writerIndex());
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${className} target) {
    final int startPoint = startIndex * ${type.width};
    final int sliceLength = length * ${type.width};
    target.clear();
    target.data = data.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
    target.data.writerIndex(sliceLength);
  }

  private class TransferImpl implements TransferPair{
    private ${className} to;

    public TransferImpl(String name, BufferAllocator allocator){
      to = new ${className}(name, allocator<#if minor.typeParams??><#list typeParams as typeParam>,  ${className}.this.${typeParam.name}</#list></#if>);
    }

    public TransferImpl(${className} to) {
      this.to = to;
    }

    @Override
    public ${className} getTo(){
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
      to.copyFromSafe(fromIndex, toIndex, ${className}.this);
    }
  }

  public void copyFrom(int fromIndex, int thisIndex, ${className} from){
    <#if (type.width > 8 || minor.class == "IntervalDay")>
    from.data.getBytes(fromIndex * ${type.width}, data, thisIndex * ${type.width}, ${type.width});
    <#else> <#-- type.width <= 8 -->
    data.set${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width},
        from.data.get${(minor.javaType!type.javaType)?cap_first}(fromIndex * ${type.width})
    );
    </#if> <#-- type.width -->
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${className} from){
    while(thisIndex >= getValueCapacity()) {
        reAlloc();
    }
    copyFrom(fromIndex, thisIndex, from);
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

  public final class Accessor extends BaseDataValueVector.BaseAccessor {
    @Override
    public int getValueCount() {
      return data.writerIndex() / ${type.width};
    }

    @Override
    public boolean isNull(int index){
      return false;
    }

    <#if (type.width > 8 || minor.class == "IntervalDay")>
    public ${minor.javaType!type.javaType} get(int index) {
      return data.slice(index * ${type.width}, ${type.width});
    }

      <#if (minor.class == "IntervalDay")>
    public void get(int index, ${minor.class}Holder holder){
      final int offsetIndex = index * ${type.width};
      holder.days = data.getInt(offsetIndex);
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      final int offsetIndex = index * ${type.width};
      holder.isSet = 1;
      holder.days = data.getInt(offsetIndex);
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      final int offsetIndex = index * ${type.width};
      final int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      final int  days   = data.getInt(offsetIndex);
      final Period p = new Period();
      return p.plusDays(days).plusMillis(millis);
    }

    public StringBuilder getAsStringBuilder(int index) {
      final int offsetIndex = index * ${type.width};

      int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      final int  days   = data.getInt(offsetIndex);

      final int hours  = millis / (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);

      final int minutes = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.minutesToMillis);

      final int seconds = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.arrow.vector.util.DateUtility.secondsToMillis);

      final String dayString = (Math.abs(days) == 1) ? " day " : " days ";

      return(new StringBuilder().
              append(days).append(dayString).
              append(hours).append(":").
              append(minutes).append(":").
              append(seconds).append(".").
              append(millis));
    }

      <#elseif minor.class == "Decimal">
    public void get(int index, ${minor.class}Holder holder) {
        holder.start = index * ${type.width};
        holder.buffer = data;
        holder.scale = scale;
        holder.precision = precision;
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
        holder.isSet = 1;
        holder.start = index * ${type.width};
        holder.buffer = data;
        holder.scale = scale;
        holder.precision = precision;
    }

    @Override
    public ${friendlyType} getObject(int index) {
      return org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(data, ${type.width} * index, scale);
    }

      <#else>
    public void get(int index, ${minor.class}Holder holder){
      holder.buffer = data;
      holder.start = index * ${type.width};
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.buffer = data;
      holder.start = index * ${type.width};
    }

    @Override
    public ${friendlyType} getObject(int index) {
      return data.slice(index * ${type.width}, ${type.width})
    }

      </#if>
    <#else> <#-- type.width <= 8 -->

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }
      <#if type.width == 4>
    public long getTwoAsLong(int index) {
      return data.getLong(index * ${type.width});
    }

      </#if>
      <#if minor.class == "DateDay" ||
           minor.class == "TimeSec" ||
           minor.class == "TimeMicro" ||
           minor.class == "TimeNano">
    @Override
    public ${friendlyType} getObject(int index) {
      return get(index);
    }

      <#elseif minor.class == "DateMilli" || minor.class == "TimeMilli" || minor.class == "TimeStampMilli">
    @Override
    public ${friendlyType} getObject(int index) {
      org.joda.time.LocalDateTime ldt = new org.joda.time.LocalDateTime(get(index), org.joda.time.DateTimeZone.UTC);
      return ldt;
    }

      <#elseif minor.class == "TimeStampSec">
    @Override
    public ${friendlyType} getObject(int index) {
      long secs = java.util.concurrent.TimeUnit.SECONDS.toMillis(get(index));
      org.joda.time.LocalDateTime date = new org.joda.time.LocalDateTime(secs, org.joda.time.DateTimeZone.UTC);
      return date;
    }

      <#elseif minor.class == "TimeStampMicro">
    @Override
    public ${friendlyType} getObject(int index) {
      // value is truncated when converting microseconds to milliseconds in order to use DateTime type
      long micros = java.util.concurrent.TimeUnit.MICROSECONDS.toMillis(get(index));
      org.joda.time.LocalDateTime date = new org.joda.time.LocalDateTime(micros, org.joda.time.DateTimeZone.UTC);
      return date;
    }

      <#elseif minor.class == "TimeStampNano">
    @Override
    public ${friendlyType} getObject(int index) {
      // value is truncated when converting nanoseconds to milliseconds in order to use DateTime type
      long millis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(get(index));
      org.joda.time.LocalDateTime date = new org.joda.time.LocalDateTime(millis, org.joda.time.DateTimeZone.UTC);
      return date;
    }

      <#elseif minor.class == "IntervalYear">
    @Override
    public ${friendlyType} getObject(int index) {

      final int value = get(index);

      final int years  = (value / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
      final int months = (value % org.apache.arrow.vector.util.DateUtility.yearsToMonths);
      final Period p = new Period();
      return p.plusYears(years).plusMonths(months);
    }

    public StringBuilder getAsStringBuilder(int index) {
      int months  = data.getInt(index);

      final int years  = (months / org.apache.arrow.vector.util.DateUtility.yearsToMonths);
      months = (months % org.apache.arrow.vector.util.DateUtility.yearsToMonths);

      final String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      final String monthString = (Math.abs(months) == 1) ? " month " : " months ";

      return(new StringBuilder().
             append(years).append(yearString).
             append(months).append(monthString));
    }

      <#else>
    @Override
    public ${friendlyType} getObject(int index) {
      return get(index);
    }
    public ${minor.javaType!type.javaType} getPrimitiveObject(int index) {
      return get(index);
    }

      </#if>
    public void get(int index, ${minor.class}Holder holder){
      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    </#if> <#-- type.width -->
  }

  /**
   * ${minor.class}.Mutator implements a mutable vector of fixed width values.  Elements in the
   * vector are accessed by position from the logical start of the vector.  Values should be pushed
   * onto the vector sequentially, but may be randomly accessed.
   *   The width of each element is ${type.width} byte(s)
   *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from FixedValueVectorTypes.tdd using FreeMarker.
   */
  public final class Mutator extends BaseDataValueVector.BaseMutator {

    private Mutator(){};

   /**
    * Set the element at the given index to the given value.  Note that widths smaller than
    * 32 bits are handled by the ArrowBuf interface.
    *
    * @param index   position of the bit to set
    * @param value   value to set
    */
    <#if (type.width > 8) || minor.class == "IntervalDay">
   public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.setBytes(index * ${type.width}, value, 0, ${type.width});
   }

   public void setSafe(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     data.setBytes(index * ${type.width}, value, 0, ${type.width});
   }

      <#if (minor.class == "IntervalDay")>
   public void set(int index, int days, int milliseconds){
     final int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, days);
     data.setInt((offsetIndex + ${minor.millisecondsOffset}), milliseconds);
   }

   protected void set(int index, ${minor.class}Holder holder){
     set(index, holder.days, holder.milliseconds);
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, int days, int milliseconds){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, days, milliseconds);
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     setSafe(index, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, Nullable${minor.class}Holder holder){
     setSafe(index, holder.days, holder.milliseconds);
   }

       <#elseif minor.class == "Decimal">
   public void set(int index, ${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   public void setSafe(int index,  Nullable${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }
   public void setSafe(int index,  ${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }

   public void setSafe(int index, int start, ArrowBuf buffer){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, start, buffer);
   }

   public void set(int index, int start, ArrowBuf buffer){
     data.setBytes(index * ${type.width}, buffer, start, ${type.width});
   }

       <#else>
   protected void set(int index, ${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   public void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   public void set(int index, int start, ArrowBuf buffer){
     data.setBytes(index * ${type.width}, buffer, start, ${type.width});
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }

   public void setSafe(int index, Nullable${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }

   public void setSafe(int index, int start, ArrowBuf buffer){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   public void set(int index, Nullable${minor.class}Holder holder){
     data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});
   }
       </#if>

   @Override
   public void generateTestData(int count) {
     setValueCount(count);
     boolean even = true;
     final int valueCount = getAccessor().getValueCount();
     for(int i = 0; i < valueCount; i++, even = !even) {
       final byte b = even ? Byte.MIN_VALUE : Byte.MAX_VALUE;
       for(int w = 0; w < ${type.width}; w++){
         data.setByte(i + w, b);
       }
     }
   }

     <#else> <#-- type.width <= 8 -->
   public void set(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, value);
   }

   public void setSafe(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, value);
   }

   protected void set(int index, ${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   public void setSafe(int index, Nullable${minor.class}Holder holder){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   @Override
   public void generateTestData(int size) {
     setValueCount(size);
     boolean even = true;
     final int valueCount = getAccessor().getValueCount();
     for(int i = 0; i < valueCount; i++, even = !even) {
       if(even){
         set(i, ${minor.boxedType!type.boxedType}.MIN_VALUE);
       }else{
         set(i, ${minor.boxedType!type.boxedType}.MAX_VALUE);
       }
     }
   }

   public void generateTestDataAlt(int size) {
     setValueCount(size);
     boolean even = true;
     final int valueCount = getAccessor().getValueCount();
     for(int i = 0; i < valueCount; i++, even = !even) {
       if(even){
         set(i, (${(minor.javaType!type.javaType)}) 1);
       }else{
         set(i, (${(minor.javaType!type.javaType)}) 0);
       }
     }
   }

    </#if> <#-- type.width -->

   @Override
   public void setValueCount(int valueCount) {
     final int currentValueCapacity = getValueCapacity();
     final int idx = (${type.width} * valueCount);
     while(valueCount > getValueCapacity()) {
       reAlloc();
     }
     if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
       incrementAllocationMonitor();
     } else if (allocationMonitor > 0) {
       allocationMonitor = 0;
     }
     VectorTrimmer.trim(data, idx);
     data.writerIndex(valueCount * ${type.width});
   }
 }
}

  </#if> <#-- type.major -->
</#list>
</#list>
