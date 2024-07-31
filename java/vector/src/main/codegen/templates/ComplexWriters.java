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

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#list ["Nullable"] as mode>
<#assign name = minor.class?cap_first />
<#assign eName = name />
<#assign javaType = (minor.javaType!type.javaType) />
<#assign fields = minor.fields!type.fields />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${eName}WriterImpl.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/*
 * This class is generated using FreeMarker on the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ${eName}WriterImpl extends AbstractFieldWriter {

  final ${name}Vector vector;

<#if minor.class?ends_with("VarChar")>
  private final Text textBuffer = new Text();
</#if>

public ${eName}WriterImpl(${name}Vector vector) {
    this.vector = vector;
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void close() {
    vector.close();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  protected int idx() {
    return super.idx();
  }

  <#if mode == "Repeated">

  public void write(${minor.class?cap_first}Holder h) {
    mutator.addSafe(idx(), h);
    vector.setValueCount(idx()+1);
  }

  public void write(${minor.class?cap_first}Holder h) {
    mutator.addSafe(idx(), h);
    vector.setValueCount(idx()+1);
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    mutator.addSafe(idx(), <#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    vector.setValueCount(idx()+1);
  }

  public void setPosition(int idx) {
    super.setPosition(idx);
    mutator.startNewValue(idx);
  }


  <#else>

  <#if !minor.class?starts_with("Decimal")>
  public void write(${minor.class}Holder h) {
    vector.setSafe(idx(), h);
    vector.setValueCount(idx()+1);
  }

  public void write(Nullable${minor.class}Holder h) {
    vector.setSafe(idx(), h);
    vector.setValueCount(idx()+1);
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    vector.setSafe(idx(), 1<#list fields as field><#if field.include!true >, ${field.name}</#if></#list>);
    vector.setValueCount(idx()+1);
  }
  </#if>

  <#if minor.class?ends_with("VarChar")>
  @Override
  public void write${minor.class}(${friendlyType} value) {
    vector.setSafe(idx(), value);
    vector.setValueCount(idx()+1);
  }

  @Override
  public void write${minor.class}(String value) {
    textBuffer.set(value);
    vector.setSafe(idx(), textBuffer);
    vector.setValueCount(idx()+1);
  }
  </#if>

  <#if minor.class?starts_with("Decimal")>

  public void write(${minor.class}Holder h){
    DecimalUtility.checkPrecisionAndScale(h.precision, h.scale, vector.getPrecision(), vector.getScale());
    vector.setSafe(idx(), h);
    vector.setValueCount(idx() + 1);
  }

  public void write(Nullable${minor.class}Holder h){
    if (h.isSet == 1) {
      DecimalUtility.checkPrecisionAndScale(h.precision, h.scale, vector.getPrecision(), vector.getScale());
    }
    vector.setSafe(idx(), h);
    vector.setValueCount(idx() + 1);
  }

  public void write${minor.class}(long start, ArrowBuf buffer){
    vector.setSafe(idx(), 1, start, buffer);
    vector.setValueCount(idx() + 1);
  }

  public void write${minor.class}(long start, ArrowBuf buffer, ArrowType arrowType){
    DecimalUtility.checkPrecisionAndScale(((ArrowType.Decimal) arrowType).getPrecision(),
      ((ArrowType.Decimal) arrowType).getScale(), vector.getPrecision(), vector.getScale());
    vector.setSafe(idx(), 1, start, buffer);
    vector.setValueCount(idx() + 1);
  }

  public void write${minor.class}(BigDecimal value){
    // vector.setSafe already does precision and scale checking
    vector.setSafe(idx(), value);
    vector.setValueCount(idx() + 1);
  }

  public void writeBigEndianBytesTo${minor.class}(byte[] value, ArrowType arrowType){
    DecimalUtility.checkPrecisionAndScale(((ArrowType.Decimal) arrowType).getPrecision(),
        ((ArrowType.Decimal) arrowType).getScale(), vector.getPrecision(), vector.getScale());
    vector.setBigEndianSafe(idx(), value);
    vector.setValueCount(idx() + 1);
  }

  public void writeBigEndianBytesTo${minor.class}(byte[] value){
    vector.setBigEndianSafe(idx(), value);
    vector.setValueCount(idx() + 1);
  }
  </#if>

  
  public void writeNull() {
    vector.setNull(idx());
    vector.setValueCount(idx()+1);
  }
  </#if>

  <#if minor.class?ends_with("VarBinary")>
  public void write${minor.class}(byte[] value) {
    vector.setSafe(idx(), value);
    vector.setValueCount(idx() + 1);
  }

  public void write${minor.class}(byte[] value, int offset, int length) {
    vector.setSafe(idx(), value, offset, length);
    vector.setValueCount(idx() + 1);
  }

  public void write${minor.class}(ByteBuffer value) {
    vector.setSafe(idx(), value, 0, value.remaining());
    vector.setValueCount(idx() + 1);
  }

  public void write${minor.class}(ByteBuffer value, int offset, int length) {
    vector.setSafe(idx(), value, offset, length);
    vector.setValueCount(idx() + 1);
  }
  </#if>
}

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/writer/${eName}Writer.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.writer;

<#include "/@includes/vv_imports.ftl" />
/*
 * This class is generated using FreeMarker on the ${.template_name} template.
 */
@SuppressWarnings("unused")
public interface ${eName}Writer extends BaseWriter {
  public void write(${minor.class}Holder h);

<#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
  /**
   * @deprecated
   * The holder version should be used instead because the plain value version does not contain enough information
   * to fully specify this field type.
   * @see #write(${minor.class}Holder)
   */
  @Deprecated
</#if>
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>);
<#if minor.class?starts_with("Decimal")>

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>, ArrowType arrowType);

  public void write${minor.class}(${friendlyType} value);

  public void writeBigEndianBytesTo${minor.class}(byte[] value, ArrowType arrowType);

  /**
   * @deprecated
   * Use either the version that additionally takes in an ArrowType or use the holder version.
   * This version does not contain enough information to fully specify this field type.
   * @see #writeBigEndianBytesTo${minor.class}(byte[], ArrowType)
   * @see #write(${minor.class}Holder)
   */
  @Deprecated
  public void writeBigEndianBytesTo${minor.class}(byte[] value);
</#if>

<#if minor.class?ends_with("VarBinary")>
  public void write${minor.class}(byte[] value);

  public void write${minor.class}(byte[] value, int offset, int length);

  public void write${minor.class}(ByteBuffer value);

  public void write${minor.class}(ByteBuffer value, int offset, int length);
</#if>

<#if minor.class?ends_with("VarChar")>
  public void write${minor.class}(${friendlyType} value);

  public void write${minor.class}(String value);
</#if>
}

</#list>
</#list>
</#list>
