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
public final class ${className} extends BaseValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, NullableVector, FieldVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

protected final static byte[] emptyByteArray = new byte[]{};
  private final FieldReader reader = new ${minor.class}ReaderImpl(${className}.this);

  private final String bitsField = "$bits$";
  private final String valuesField = "$values$";
  private final Field field;

  final BitVector bits = new BitVector(bitsField, allocator);
  final ${valuesName} values;

  private final Mutator mutator;
  private final Accessor accessor;

  private final List<BufferBacked> innerVectors;

  <#if minor.typeParams??>
    <#assign typeParams = minor.typeParams?reverse>
    <#list typeParams as typeParam>
  private final ${typeParam.type} ${typeParam.name};
    </#list>

  /**
   * Assumes the type is nullable and not dictionary encoded
   * @param name name of the field
   * @param allocator allocator to use to resize the vector<#list typeParams as typeParam>
   * @param ${typeParam.name} type parameter ${typeParam.name}</#list>
   */
  public ${className}(String name, BufferAllocator allocator<#list typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    <#if minor.arrowTypeConstructorParams??>
       <#assign constructorParams = minor.arrowTypeConstructorParams />
    <#else>
       <#assign constructorParams = [] />
       <#list typeParams as typeParam>
         <#assign constructorParams = constructorParams + [ typeParam.name ] />
      </#list>
    </#if>
    this(name, FieldType.nullable(new ${minor.arrowType}(${constructorParams?join(", ")})), allocator);
  }
  <#else>
  public ${className}(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.${minor.class?upper_case}.getType()), allocator);
  }
  </#if>

  public ${className}(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, allocator);
    <#if minor.typeParams??>
    <#assign typeParams = minor.typeParams?reverse>
    ${minor.arrowType} arrowType = (${minor.arrowType})fieldType.getType();
    <#list typeParams as typeParam>
    this.${typeParam.name} = arrowType.get${typeParam.name?cap_first}();
    </#list>
    this.values = new ${valuesName}(valuesField, allocator<#list typeParams as typeParam>, ${typeParam.name}</#list>);
    <#else>
    this.values = new ${valuesName}(valuesField, allocator);
    </#if>
    this.mutator = new Mutator();
    this.accessor = new Accessor();
    this.field = new Field(name, fieldType, null);
    innerVectors = Collections.unmodifiableList(Arrays.<BufferBacked>asList(
        bits,
        <#if type.major = "VarLen">
        values.offsetVector,
        </#if>
        values
    ));
  }

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
    org.apache.arrow.vector.BaseDataValueVector.truncateBufferBasedOnSize(ownBuffers, 1,
        values.offsetVector.getBufferSizeFor(
        fieldNode.getLength() == 0? 0 : fieldNode.getLength() + 1));
    mutator.lastSet = fieldNode.getLength() - 1;
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

  public ArrowBuf getBuffer() {
    return values.getDataBuffer();
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

  @Override
  public void reAlloc() {
    bits.reAlloc();
    values.reAlloc();
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

  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
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
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
        return getTransferPair(ref, allocator);
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

    public TransferImpl(String ref, BufferAllocator allocator){
      to = new ${className}(ref, field.getFieldType(), allocator);
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
    <#if type.major == "VarLen">mutator.lastSet = thisIndex;</#if>
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${valuesName} from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafeToOne(thisIndex);
    <#if type.major == "VarLen">mutator.lastSet = thisIndex;</#if>
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${className} from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
    <#if type.major == "VarLen">mutator.lastSet = thisIndex;</#if>
  }

  @Override
  public long getValidityBufferAddress() {
    /* address of the databuffer associated with the bitVector */
    return (bits.getDataBuffer().memoryAddress());
  }

  @Override
  public long getDataBufferAddress() {
    /* address of the dataBuffer associated with the valueVector */
    return (values.getDataBuffer().memoryAddress());
  }

  @Override
  public long getOffsetBufferAddress() {
    /* address of the dataBuffer associated with the offsetVector
     * this operation is not supported for fixed-width vector types.
     */
    <#if type.major != "VarLen">
        throw new UnsupportedOperationException();
    <#else>
        return (values.getOffsetAddr());
    </#if>
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    /* dataBuffer associated with the bitVector */
    return (bits.getDataBuffer());
  }

  @Override
  public ArrowBuf getDataBuffer() {
    /* dataBuffer associated with the valueVector */
    return (values.getDataBuffer());
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    /* dataBuffer associated with the offsetVector of the valueVector */
    <#if type.major != "VarLen">
        throw new UnsupportedOperationException();
    <#else>
        return (values.getOffsetBuffer());
    </#if>
  }

  public final class Accessor extends BaseDataValueVector.BaseAccessor <#if type.major = "VarLen">implements VariableWidthVector.VariableWidthAccessor</#if> {
    final BitVector.Accessor bAccessor = bits.getAccessor();
    final ${valuesName}.Accessor vAccessor = values.getAccessor();

    /**
     * Get the element at the specified position.
     *
     * @param  index   position of the value
     * @return value of the element, if not null
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
    }

    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(index);
      }
    }

    <#if minor.class == "IntervalYear" || minor.class == "IntervalDay">
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
     * @param value   array of bytes (or int if smaller than 4 bytes) to write
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

    public void fillEmpties(int index){
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.setSafe(i, emptyByteArray);
      }
      while(index > bits.getValueCapacity()) {
        bits.reAlloc();
      }
      lastSet = index - 1;
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
    public void set(int index, int isSet<#list fields as field>, ${field.type} ${field.name}Field</#list> ){
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

    <#if !(type.major == "VarLen" || minor.class == "IntervalDay")>
    public void setSafe(int index, ${minor.javaType!type.javaType} value) {
      bits.getMutator().setSafeToOne(index);
      values.getMutator().setSafe(index, value);
      setCount++;
    }

    </#if>
    <#if minor.class == "Decimal">
    public void set(int index, ${friendlyType} value) {
      bits.getMutator().setToOne(index);
      values.getMutator().set(index, value);
    }

    public void setSafe(int index, ${friendlyType} value) {
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

    public void setLastSet(int value) {
      <#if type.major = "VarLen">
        lastSet = value;
      <#else>
        throw new UnsupportedOperationException();
      </#if>
    }

    public int getLastSet() {
      <#if type.major != "VarLen">
        throw new UnsupportedOperationException();
      <#else>
        return lastSet;
      </#if>
    }
  }
}
</#list>
</#list>
