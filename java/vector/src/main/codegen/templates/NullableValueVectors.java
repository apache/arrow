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

<#assign className = "LegacyNullable${minor.class}Vector" />
<#assign valuesName = "Nullable${minor.class}Vector" />
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
@Deprecated
public final class ${className} extends BaseValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, FieldVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

protected final static byte[] emptyByteArray = new byte[]{};

  private final String bitsField = "$bits$";
  private final String valuesField = "$values$";

  final BitVector bits = new BitVector(bitsField, allocator);
  final ${valuesName} values;

  private final Mutator mutator;
  private final Accessor accessor;

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
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    /* DELEGATE TO NEW VECTOR */
    return values.getFieldInnerVectors();
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
    /* DELEGATE TO NEW VECTOR */
    values.loadFieldBuffers(fieldNode, ownBuffers);
  }

  public List<ArrowBuf> getFieldBuffers() {
    /* DELEGATE TO NEW VECTOR */
    return values.getFieldBuffers();
  }

  @Override
  public Field getField() {
    /* DELEGATE TO NEW VECTOR */
    return values.getField();
  }

  @Override
  public MinorType getMinorType() {
    /* DELEGATE TO NEW VECTOR */
    return values.getMinorType();
  }

  @Override
  public FieldReader getReader(){
    /* DELEGATE TO NEW VECTOR */
    return values.getReader();
  }

  @Override
  public int getValueCapacity(){
    /* DELEGATE TO NEW VECTOR */
    return values.getValueCapacity();
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    /* DELEGATE TO NEW VECTOR */
    return values.getBuffers(clear);
  }

  @Override
  public void close() {
    /* DELEGATE TO NEW VECTOR */
    values.close();
  }

  @Override
  public void clear() {
    /* DELEGATE TO NEW VECTOR */
    values.clear();
  }

  @Override
  public int getBufferSize(){
    /* DELEGATE TO NEW VECTOR */
    return values.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    /* DELEGATE TO NEW VECTOR */
    return values.getBufferSizeFor(valueCount);
  }

  public ArrowBuf getBuffer() {
    return values.getDataBuffer();
  }

  public ${valuesName} getValuesVector() {
    return values;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    /* DELEGATE TO NEW VECTOR */
    values.setInitialCapacity(numRecords);
  }

  @Override
  public void allocateNew() {
    /* DELEGATE TO NEW VECTOR */
    values.allocateNew();
  }

  @Override
  public boolean allocateNewSafe() {
    /* DELEGATE TO NEW VECTOR */
    return values.allocateNewSafe();
  }

  @Override
  public void reAlloc() {
    /* DELEGATE TO NEW VECTOR */
    values.reAlloc();
  }

  public void reset() {
    /* DELEGATE TO NEW VECTOR */
    values.reset();
  }

  <#if type.major == "VarLen">
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    /* DELEGATE TO NEW VECTOR */
    values.allocateNew(totalBytes, valueCount);
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
    /* DELEGATE TO NEW VECTOR */
    values.allocateNew(valueCount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    /* DELEGATE TO NEW VECTOR */
    values.zeroVector();
  }
  </#if>



  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    /* DELEGATE TO NEW VECTOR */
    return values.getTransferPair(ref, allocator, callBack);
  }



  @Override
  public TransferPair getTransferPair(BufferAllocator allocator){
    /* DELEGATE TO NEW VECTOR */
    return values.getTransferPair(allocator);
  }



  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    /* DELEGATE TO NEW VECTOR */
    return values.getTransferPair(ref, allocator);
  }



  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    /* DELEGATE TO NEW VECTOR */
    return values.makeTransferPair(to);
  }



  public void transferTo(${valuesName} target) {
    /* DELEGATE TO NEW VECTOR */
    <#if type.major == "VarLen">
        values.transferTo((BaseNullableVariableWidthVector) target);
    <#else>
        values.transferTo((BaseNullableFixedWidthVector) target);
    </#if>
  }

  public void splitAndTransferTo(int startIndex, int length, ${valuesName} target) {
    /* DELEGATE TO NEW VECTOR */
    <#if type.major == "VarLen">
        values.splitAndTransferTo(startIndex, length, (BaseNullableVariableWidthVector) target);
    <#else>
        values.splitAndTransferTo(startIndex, length, (BaseNullableFixedWidthVector) target);
    </#if>
  }



  @Override
  public Accessor getAccessor(){
    return accessor;
  }

  @Override
  public Mutator getMutator(){
    return mutator;
  }


  public void copyFrom(int fromIndex, int thisIndex, ${valuesName} from) {
    /* DELEGATE TO NEW VECTOR */
    values.copyFrom(fromIndex, thisIndex, from);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${valuesName} from) {
    /* DELEGATE TO NEW VECTOR */
    values.copyFromSafe(fromIndex, thisIndex, from);
  }

  @Override
  public long getValidityBufferAddress() {
    /* DELEGATE TO NEW VECTOR */
    return values.getValidityBufferAddress();
  }

  @Override
  public long getDataBufferAddress() {
    /* DELEGATE TO NEW VECTOR */
    return values.getDataBufferAddress();
  }

  @Override
  public long getOffsetBufferAddress() {
    /* DELEGATE TO NEW VECTOR */
    return values.getOffsetBufferAddress();
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    /* DELEGATE TO NEW VECTOR */
    return values.getValidityBuffer();
  }

  @Override
  public ArrowBuf getDataBuffer() {
    return (values.getDataBuffer());
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    /* DELEGATE TO NEW VECTOR */
    return values.getOffsetBuffer();
  }

  public final class Accessor extends BaseDataValueVector.BaseAccessor <#if type.major = "VarLen">implements VariableWidthVector.VariableWidthAccessor</#if> {

    /**
     * Get the element at the specified position.
     *
     * @param  index   position of the value
     * @return value of the element, if not null
     */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      /* DELEGATE TO NEW VECTOR */
      return values.get(index);
    }

    @Override
    public boolean isNull(int index) {
      /* DELEGATE TO NEW VECTOR */
      return values.isNull(index);
    }

    public int isSet(int index){
      /* DELEGATE TO NEW VECTOR */
      return values.isSet(index);
    }

    <#if type.major == "VarLen">
    public long getStartEnd(int index){
        /* DELEGATE TO NEW VECTOR */
        return values.getStartEnd(index);
    }

    @Override
    public int getValueLength(int index) {
        /* DELEGATE TO NEW VECTOR */
        return values.getValueLength(index);
    }
    </#if>

    public void get(int index, Nullable${minor.class}Holder holder){
        /* DELEGATE TO NEW VECTOR */
        values.get(index, holder);
    }

    @Override
    public ${friendlyType} getObject(int index) {
      /* DELEGATE TO NEW VECTOR */
      return values.getObject(index);
    }

    <#if minor.class == "IntervalYear" || minor.class == "IntervalDay">
    public StringBuilder getAsStringBuilder(int index) {
       /* DELEGATE TO NEW VECTOR */
       return values.getAsStringBuilder(index);
    }
    </#if>

    @Override
    public int getValueCount(){
      /* DELEGATE TO NEW VECTOR */
      return values.getValueCount();
    }

    public void reset() { }
  }

  public final class Mutator extends BaseDataValueVector.BaseMutator implements NullableVectorDefinitionSetter<#if type.major = "VarLen">, VariableWidthVector.VariableWidthMutator</#if> {
    private int setCount;
    <#if type.major = "VarLen"> private int lastSet = -1;</#if>

    private Mutator() { }

    public ${valuesName} getVectorWithValues() {
      return values;
    }


    @Override
    public void setIndexDefined(int index) {
      /* DELEGATE TO NEW VECTOR */
      values.setIndexDefined(index);
    }



    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param value   array of bytes (or int if smaller than 4 bytes) to write
     */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
       /* DELEGATE TO NEW VECTOR */
       values.set(index, value);
    }



    <#if type.major == "VarLen">
    public void fillEmpties(int index) {
      /* DELEGATE TO NEW VECTOR */
      values.fillEmpties(index);
    }

    @Override
    public void setValueLengthSafe(int index, int length) {
      /* DELEGATE TO NEW VECTOR */
      values.setValueLengthSafe(index, length);
    }
    </#if>


    public void setSafe(int index, byte[] value, int start, int length) {
       /* DELEGATE TO NEW VECTOR */
      values.setSafe(index, value, start, length);
    }


    public void setSafe(int index, ByteBuffer value, int start, int length) {
       /* DELEGATE TO NEW VECTOR */
       values.setSafe(index, value, start, length);
    }


    public void setNull(int index) {
       /* DELEGATE TO NEW VECTOR */
       values.setNull(index);
    }


    public void set(int index, Nullable${minor.class}Holder holder) {
      /* DELEGATE TO NEW VECTOR */
      values.set(index, holder);
    }


    public void set(int index, ${minor.class}Holder holder) {
        /* DELEGATE TO NEW VECTOR */
        values.set(index, holder);
    }


    public boolean isSafe(int outIndex) {
       /* DELEGATE TO NEW VECTOR */
       return values.isSafe(outIndex);
    }


    <#assign fields = minor.fields!type.fields />
    public void set(int index, int isSet<#list fields as field>, ${field.type} ${field.name}Field</#list> ){
      values.set(index, isSet<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
    }

    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      values.setSafe(index, isSet<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
    }


    public void setSafe(int index, Nullable${minor.class}Holder value) {
      /* DELEGATE TO NEW VECTOR */
      values.setSafe(index, value);
    }

    public void setSafe(int index, ${minor.class}Holder value) {
      /* DELEGATE TO NEW VECTOR */
      values.setSafe(index, value);
    }


    <#if !(type.major == "VarLen" || minor.class == "IntervalDay")>
    public void setSafe(int index, ${minor.javaType!type.javaType} value) {
      /* DELEGATE TO NEW VECTOR */
      values.setSafe(index, value);
    }
    </#if>



    <#if minor.class == "Decimal">
    public void set(int index, ${friendlyType} value) {
      /* DELEGATE TO NEW VECTOR */
      values.set(index, value);
    }

    public void setSafe(int index, ${friendlyType} value) {
      /* DELEGATE TO NEW VECTOR */
      values.setSafe(index, value);
    }
    </#if>


    @Override
    public void setValueCount(int valueCount) {
      /* DELEGATE TO NEW VECTOR */
      values.setValueCount(valueCount);
    }


    /* THIS METHOD IS PROBABLY NOT NEEDED FOR NEW VECTORS */
    @Override
    public void generateTestData(int valueCount) { }


    /* MUTATOR RESET IS NOT NEEDED FOR NEW VECTORS */
    @Override
    public void reset() { }


    <#if type.major == "VarLen">
    public void setLastSet(int value) {
      /* DELEGATE TO NEW VECTOR */
      values.setLastSet(value);
    }


    public int getLastSet() {
      /* DELEGATE TO NEW VECTOR */
      return values.getLastSet();
    }
    </#if>
  }
}
</#list>
</#list>
