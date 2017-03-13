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
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}Vector" />
<#assign valuesName = "${minor.class}Vector" />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<@pp.changeOutputFile name="/org/apache/arrow/vector/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

import org.apache.arrow.vector.schema.ArrowFieldNode;
import java.util.Collections;

<#include "/@includes/vv_imports.ftl" />

import org.apache.arrow.flatbuf.Precision;

/**
 * ${className} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${className} extends BaseDataValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, NullableVector, FieldVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

  private final FieldReader reader = new ${minor.class}ReaderImpl(${className}.this);

  private final String bitsField = "$bits$";
  private final String valuesField = "$values$";
  private final Field field;

  final BitVector bits = new BitVector(bitsField, allocator);
  final ${valuesName} values;

  private final Mutator mutator;
  private final Accessor accessor;

  private final List<BufferBacked> innerVectors;

  <#if minor.class == "Decimal">
  private final int precision;
  private final int scale;

  public ${className}(String name, BufferAllocator allocator, DictionaryEncoding dictionary, int precision, int scale) {
    super(name, allocator);
    values = new ${valuesName}(valuesField, allocator, precision, scale);
    this.precision = precision;
    this.scale = scale;
    mutator = new Mutator();
    accessor = new Accessor();
    field = new Field(name, true, new Decimal(precision, scale), dictionary, null);
    innerVectors = Collections.unmodifiableList(Arrays.<BufferBacked>asList(
        bits,
        values
    ));
  }
  <#else>
  public ${className}(String name, BufferAllocator allocator, DictionaryEncoding dictionary) {
    super(name, allocator);
    values = new ${valuesName}(valuesField, allocator);
    mutator = new Mutator();
    accessor = new Accessor();
  <#if minor.class == "TinyInt" ||
        minor.class == "SmallInt" ||
        minor.class == "Int" ||
        minor.class == "BigInt">
    field = new Field(name, true, new Int(${type.width} * 8, true), dictionary, null);
  <#elseif minor.class == "UInt1" ||
        minor.class == "UInt2" ||
        minor.class == "UInt4" ||
        minor.class == "UInt8">
    field = new Field(name, true, new Int(${type.width} * 8, false), dictionary, null);
  <#elseif minor.class == "Date">
    field = new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.Date(), dictionary, null);
  <#elseif minor.class == "Time">
    field = new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.Time(), dictionary, null);
  <#elseif minor.class == "Float4">
    field = new Field(name, true, new FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), dictionary, null);
  <#elseif minor.class == "Float8">
    field = new Field(name, true, new FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), dictionary, null);
  <#elseif minor.class == "TimeStampSec">
    field = new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND), dictionary, null);
  <#elseif minor.class == "TimeStampMilli">
    field = new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND), dictionary, null);
  <#elseif minor.class == "TimeStampMicro">
    field = new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND), dictionary, null);
  <#elseif minor.class == "TimeStampNano">
    field = new Field(name, true, new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND), dictionary, null);
  <#elseif minor.class == "IntervalDay">
    field = new Field(name, true, new Interval(org.apache.arrow.vector.types.IntervalUnit.DAY_TIME), dictionary, null);
  <#elseif minor.class == "IntervalYear">
    field = new Field(name, true, new Interval(org.apache.arrow.vector.types.IntervalUnit.YEAR_MONTH), dictionary, null);
  <#elseif minor.class == "VarChar">
    field = new Field(name, true, new Utf8(), dictionary, null);
  <#elseif minor.class == "VarBinary">
    field = new Field(name, true, new Binary(), dictionary, null);
  <#elseif minor.class == "Bit">
    field = new Field(name, true, new Bool(), dictionary, null);
  </#if>
    innerVectors = Collections.unmodifiableList(Arrays.<BufferBacked>asList(
        bits,
        <#if type.major = "VarLen">
        values.offsetVector,
        </#if>
        values
    ));
  }
  </#if>

  @Override
  public BitVector getValidityVector() {
    return bits;
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return innerVectors;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (!children.isEmpty()) {
      throw new IllegalArgumentException("primitive type vector ${className} can not have children: " + children);
    }
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return Collections.emptyList();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    <#if type.major = "VarLen">
    // variable width values: truncate offset vector buffer to size (#1)
    org.apache.arrow.vector.BaseDataValueVector.truncateBufferBasedOnSize(ownBuffers, 1, values.offsetVector.getBufferSizeFor(fieldNode.getLength() + 1));
    <#else>
    // fixed width values truncate value vector to size (#1)
    org.apache.arrow.vector.BaseDataValueVector.truncateBufferBasedOnSize(ownBuffers, 1, values.getBufferSizeFor(fieldNode.getLength()));
    </#if>
    org.apache.arrow.vector.BaseDataValueVector.load(fieldNode, getFieldInnerVectors(), ownBuffers);
    bits.valueCount = fieldNode.getLength();
  }

  public List<ArrowBuf> getFieldBuffers() {
    return org.apache.arrow.vector.BaseDataValueVector.unload(getFieldInnerVectors());
  }

  @Override
  public Field getField() {
    return field;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.${minor.class?upper_case};
  }

  @Override
  public FieldReader getReader(){
    return reader;
  }

  @Override
  public int getValueCapacity(){
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), values.getBuffers(false), ArrowBuf.class);
    if (clear) {
      for (final ArrowBuf buffer:buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
  }

  @Override
  public int getBufferSize(){
    return values.getBufferSize() + bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return values.getBufferSizeFor(valueCount)
        + bits.getBufferSizeFor(valueCount);
  }

  @Override
  public ArrowBuf getBuffer() {
    return values.getBuffer();
  }

  @Override
  public ${valuesName} getValuesVector() {
    return values;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    values.setInitialCapacity(numRecords);
  }

  @Override
  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = values.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return success;
  }

  <#if type.major == "VarLen">
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    try {
      values.allocateNew(totalBytes, valueCount);
      bits.allocateNew(valueCount);
    } catch(RuntimeException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  @Override
  public int getByteCapacity(){
    return values.getByteCapacity();
  }

  @Override
  public int getCurrentSizeInBytes(){
    return values.getCurrentSizeInBytes();
  }

  <#else>
  @Override
  public void allocateNew(int valueCount) {
    try {
      values.allocateNew(valueCount);
      bits.allocateNew(valueCount+1);
    } catch(OutOfMemoryException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    bits.zeroVector();
    values.zeroVector();
  }
  </#if>

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
    return new TransferImpl((${className}) to);
  }

  public void transferTo(${className} target){
    bits.transferTo(target.bits);
    values.transferTo(target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = mutator.lastSet;
    </#if>
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${className} target) {
    bits.splitAndTransferTo(startIndex, length, target.bits);
    values.splitAndTransferTo(startIndex, length, target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = length - 1;
    </#if>
  }

  private class TransferImpl implements TransferPair {
    ${className} to;

    public TransferImpl(String name, BufferAllocator allocator){
      <#if minor.class == "Decimal">
      to = new ${className}(name, allocator, field.getDictionary(), precision, scale);
      <#else>
      to = new ${className}(name, allocator, field.getDictionary());
      </#if>
    }

    public TransferImpl(${className} to){
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

  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  @Override
  public Mutator getMutator(){
    return mutator;
  }

  public void copyFrom(int fromIndex, int thisIndex, ${className} from){
    final Accessor fromAccessor = from.getAccessor();
    if (!fromAccessor.isNull(fromIndex)) {
      mutator.set(thisIndex, fromAccessor.get(fromIndex));
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${valuesName} from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafeToOne(thisIndex);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${className} from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
  }

  public final class Accessor extends BaseDataValueVector.BaseAccessor <#if type.major = "VarLen">implements VariableWidthVector.VariableWidthAccessor</#if> {
    final BitVector.Accessor bAccessor = bits.getAccessor();
    final ${valuesName}.Accessor vAccessor = values.getAccessor();

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  NullValueException if the value is null
     */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      if (isNull(index)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(index);
    }

    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index){
      return bAccessor.get(index);
    }

    <#if type.major == "VarLen">
    public long getStartEnd(int index){
      return vAccessor.getStartEnd(index);
    }

    @Override
    public int getValueLength(int index) {
      return values.getAccessor().getValueLength(index);
    }
    </#if>

    public void get(int index, Nullable${minor.class}Holder holder){
      vAccessor.get(index, holder);
      holder.isSet = bAccessor.get(index);

      <#if minor.class.startsWith("Decimal")>
      holder.scale = scale;
      holder.precision = precision;
      </#if>
    }

    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(index);
      }
    }

    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    public StringBuilder getAsStringBuilder(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getAsStringBuilder(index);
      }
    }
    </#if>

    @Override
    public int getValueCount(){
      return bits.getAccessor().getValueCount();
    }

    public void reset(){}
  }

  public final class Mutator extends BaseDataValueVector.BaseMutator implements NullableVectorDefinitionSetter<#if type.major = "VarLen">, VariableWidthVector.VariableWidthMutator</#if> {
    private int setCount;
    <#if type.major = "VarLen"> private int lastSet = -1;</#if>

    private Mutator(){
    }

    public ${valuesName} getVectorWithValues(){
      return values;
    }

    @Override
    public void setIndexDefined(int index){
      bits.getMutator().setToOne(index);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setCount++;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      final BitVector.Mutator bitsMutator = bits.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bitsMutator.setToOne(index);
      valuesMutator.set(index, value);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if type.major == "VarLen">

    private void fillEmpties(int index){
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      for (int i = lastSet; i < index; i++) {
        valuesMutator.setSafe(i + 1, emptyByteArray);
      }
      while(index > bits.getValueCapacity()) {
        bits.reAlloc();
      }
      lastSet = index;
    }

    @Override
    public void setValueLengthSafe(int index, int length) {
      values.getMutator().setValueLengthSafe(index, length);
      lastSet = index;
    }
    </#if>

    public void setSafe(int index, byte[] value, int start, int length) {
      <#if type.major != "VarLen">
      throw new UnsupportedOperationException();
      <#else>
      fillEmpties(index);

      bits.getMutator().setSafeToOne(index);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
      </#if>
    }

    public void setSafe(int index, ByteBuffer value, int start, int length) {
      <#if type.major != "VarLen">
      throw new UnsupportedOperationException();
      <#else>
      fillEmpties(index);

      bits.getMutator().setSafeToOne(index);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
      </#if>
    }

    public void setNull(int index){
      bits.getMutator().setSafe(index, 0);
    }

    public void setSkipNull(int index, ${minor.class}Holder holder){
      values.getMutator().set(index, holder);
    }

    public void setSkipNull(int index, Nullable${minor.class}Holder holder){
      values.getMutator().set(index, holder);
    }


    public void set(int index, Nullable${minor.class}Holder holder){
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, holder.isSet);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void set(int index, ${minor.class}Holder holder){
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().setToOne(index);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public boolean isSafe(int outIndex) {
      return outIndex < ${className}.this.getValueCapacity();
    }

    <#assign fields = minor.fields!type.fields />
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ){
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, isSet);
      valuesMutator.set(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      <#if type.major == "VarLen">
      fillEmpties(index);
      </#if>

      bits.getMutator().setSafe(index, isSet);
      values.getMutator().setSafe(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }


    public void setSafe(int index, Nullable${minor.class}Holder value) {

      <#if type.major == "VarLen">
      fillEmpties(index);
      </#if>
      bits.getMutator().setSafe(index, value.isSet);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void setSafe(int index, ${minor.class}Holder value) {

      <#if type.major == "VarLen">
      fillEmpties(index);
      </#if>
      bits.getMutator().setSafeToOne(index);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "Interval" || minor.class == "IntervalDay")>
      public void setSafe(int index, ${minor.javaType!type.javaType} value) {
        <#if type.major == "VarLen">
        fillEmpties(index);
        </#if>
        bits.getMutator().setSafeToOne(index);
        values.getMutator().setSafe(index, value);
        setCount++;
      }

    </#if>

    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      <#if type.major == "VarLen">
      fillEmpties(valueCount);
      </#if>
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }

    @Override
    public void generateTestData(int valueCount){
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }

    @Override
    public void reset(){
      setCount = 0;
      <#if type.major = "VarLen">lastSet = -1;</#if>
    }
  }
}
</#list>
</#list>
