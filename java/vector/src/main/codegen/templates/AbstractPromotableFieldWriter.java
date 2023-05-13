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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/AbstractPromotableFieldWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/*
 * A FieldWriter which delegates calls to another FieldWriter. The delegate FieldWriter can be promoted to a new type
 * when necessary. Classes that extend this class are responsible for handling promotion.
 *
 * This class is generated using freemarker and the ${.template_name} template.
 *
 */
@SuppressWarnings("unused")
abstract class AbstractPromotableFieldWriter extends AbstractFieldWriter {
  /**
   * Retrieve the FieldWriter, promoting if it is not a FieldWriter of the specified type
   * @param type the type of the values we want to write
   * @return the corresponding field writer
   */
  protected FieldWriter getWriter(MinorType type) {
    return getWriter(type, null);
  }

  abstract protected FieldWriter getWriter(MinorType type, ArrowType arrowType);

  /**
   * @return the current FieldWriter
   */
  abstract protected FieldWriter getWriter();

  @Override
  public void start() {
    getWriter(MinorType.STRUCT).start();
  }

  @Override
  public void end() {
    getWriter(MinorType.STRUCT).end();
    setPosition(idx() + 1);
  }

  @Override
  public void startList() {
    getWriter(MinorType.LIST).startList();
  }

  @Override
  public void endList() {
    getWriter(MinorType.LIST).endList();
    setPosition(idx() + 1);
  }

  @Override
  public void startMap() {
    getWriter(MinorType.MAP).startMap();
  }

  @Override
  public void endMap() {
    getWriter(MinorType.MAP).endMap();
    setPosition(idx() + 1);
  }

  @Override
  public void startEntry() {
    getWriter(MinorType.MAP).startEntry();
  }

  @Override
  public MapWriter key() {
    return getWriter(MinorType.MAP).key();
  }

  @Override
  public MapWriter value() {
    return getWriter(MinorType.MAP).value();
  }

  @Override
  public void endEntry() {
    getWriter(MinorType.MAP).endEntry();
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
    <#assign fields = minor.fields!type.fields />
  <#if minor.class == "Decimal">
  @Override
  public void write(DecimalHolder holder) {
    getWriter(MinorType.DECIMAL).write(holder);
  }

  public void writeDecimal(int start, ArrowBuf buffer, ArrowType arrowType) {
    getWriter(MinorType.DECIMAL).writeDecimal(start, buffer, arrowType);
  }

  public void writeDecimal(int start, ArrowBuf buffer) {
    getWriter(MinorType.DECIMAL).writeDecimal(start, buffer);
  }

  public void writeBigEndianBytesToDecimal(byte[] value, ArrowType arrowType) {
    getWriter(MinorType.DECIMAL).writeBigEndianBytesToDecimal(value, arrowType);
  }

  public void writeBigEndianBytesToDecimal(byte[] value) {
    getWriter(MinorType.DECIMAL).writeBigEndianBytesToDecimal(value);
  }
  <#elseif minor.class == "Decimal256">
  @Override
  public void write(Decimal256Holder holder) {
    getWriter(MinorType.DECIMAL256).write(holder);
  }

  public void writeDecimal256(long start, ArrowBuf buffer, ArrowType arrowType) {
    getWriter(MinorType.DECIMAL256).writeDecimal256(start, buffer, arrowType);
  }

  public void writeDecimal256(long start, ArrowBuf buffer) {
    getWriter(MinorType.DECIMAL256).writeDecimal256(start, buffer);
  }
  public void writeBigEndianBytesToDecimal256(byte[] value, ArrowType arrowType) {
    getWriter(MinorType.DECIMAL256).writeBigEndianBytesToDecimal256(value, arrowType);
  }

  public void writeBigEndianBytesToDecimal256(byte[] value) {
    getWriter(MinorType.DECIMAL256).writeBigEndianBytesToDecimal256(value);
  }
  <#elseif is_timestamp_tz(minor.class)>
  @Override
  public void write(${name}Holder holder) {
    ArrowType.Timestamp arrowTypeWithoutTz = (ArrowType.Timestamp) MinorType.${name?upper_case?remove_ending("TZ")}.getType();
    // Take the holder.timezone similar to how PromotableWriter.java:write(DecimalHolder) takes the scale from the holder.
    ArrowType.Timestamp arrowType = new ArrowType.Timestamp(arrowTypeWithoutTz.getUnit(), holder.timezone);
    getWriter(MinorType.${name?upper_case}, arrowType).write(holder);
  }

  /**
   * @deprecated
   * The holder version should be used instead otherwise the timezone will default to UTC.
   * @see #write(${name}Holder)
   */
  @Deprecated
  @Override
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    ArrowType.Timestamp arrowTypeWithoutTz = (ArrowType.Timestamp) MinorType.${name?upper_case?remove_ending("TZ")}.getType();
    // Assumes UTC if no timezone is provided
    ArrowType.Timestamp arrowType = new ArrowType.Timestamp(arrowTypeWithoutTz.getUnit(), "UTC");
    getWriter(MinorType.${name?upper_case}, arrowType).write${minor.class}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
  <#elseif minor.class == "Duration">
  @Override
  public void write(${name}Holder holder) {
    ArrowType.Duration arrowType = new ArrowType.Duration(holder.unit);
    getWriter(MinorType.${name?upper_case}, arrowType).write(holder);
  }

  /**
   * @deprecated
   * If you experience errors with using this version of the method, switch to the holder version.
   * The errors occur when using an untyped or unioned PromotableWriter, because this version of the
   * method does not have enough information to infer the ArrowType.
   * @see #write(${name}Holder)
   */
  @Deprecated
  @Override
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    getWriter(MinorType.${name?upper_case}).write${minor.class}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
  <#elseif minor.class == "FixedSizeBinary">
  @Override
  public void write(${name}Holder holder) {
    ArrowType.FixedSizeBinary arrowType = new ArrowType.FixedSizeBinary(holder.byteWidth);
    getWriter(MinorType.${name?upper_case}, arrowType).write(holder);
  }

  /**
   * @deprecated
   * If you experience errors with using this version of the method, switch to the holder version.
   * The errors occur when using an untyped or unioned PromotableWriter, because this version of the
   * method does not have enough information to infer the ArrowType.
   * @see #write(${name}Holder)
   */
  @Deprecated
  @Override
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    getWriter(MinorType.${name?upper_case}).write${minor.class}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
  <#else>
  @Override
  public void write(${name}Holder holder) {
    getWriter(MinorType.${name?upper_case}).write(holder);
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    getWriter(MinorType.${name?upper_case}).write${minor.class}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
  </#if>

  </#list></#list>
  public void writeNull() {
  }

  @Override
  public StructWriter struct() {
    return getWriter(MinorType.LIST).struct();
  }

  @Override
  public ListWriter list() {
    return getWriter(MinorType.LIST).list();
  }

  @Override
  public MapWriter map() {
    return getWriter(MinorType.LIST).map();
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    return getWriter(MinorType.MAP, new ArrowType.Map(keysSorted));
  }

  @Override
  public StructWriter struct(String name) {
    return getWriter(MinorType.STRUCT).struct(name);
  }

  @Override
  public ListWriter list(String name) {
    return getWriter(MinorType.STRUCT).list(name);
  }

  @Override
  public MapWriter map(String name) {
    return getWriter(MinorType.STRUCT).map(name);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    return getWriter(MinorType.STRUCT).map(name, keysSorted);
  }
  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />

  <#if minor.typeParams?? >
  @Override
  public ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    return getWriter(MinorType.STRUCT).${lowerName}(name<#list minor.typeParams as typeParam>, ${typeParam.name}</#list>);
  }

  </#if>
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    return getWriter(MinorType.STRUCT).${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    return getWriter(MinorType.LIST).${lowerName}();
  }

  </#list></#list>

  public void copyReader(FieldReader reader) {
    getWriter().copyReader(reader);
  }

  public void copyReaderToField(String name, FieldReader reader) {
    getWriter().copyReaderToField(name, reader);
  }
}
