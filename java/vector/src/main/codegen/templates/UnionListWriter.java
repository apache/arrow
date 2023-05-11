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
import org.apache.arrow.vector.complex.writer.Decimal256Writer;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.holders.Decimal256Holder;
import org.apache.arrow.vector.holders.DecimalHolder;


import java.lang.UnsupportedOperationException;
import java.math.BigDecimal;

<@pp.dropOutputFile />
<#list ["List", "LargeList"] as listName>

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/Union${listName}Writer.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
<#include "/@includes/vv_imports.ftl" />

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
public class Union${listName}Writer extends AbstractFieldWriter {

  protected ${listName}Vector vector;
  protected PromotableWriter writer;
  private boolean inStruct = false;
  private boolean listStarted = false;
  private String structName;
  <#if listName == "LargeList">
  private static final long OFFSET_WIDTH = 8;
  <#else>
  private static final int OFFSET_WIDTH = 4;
  </#if>

  public Union${listName}Writer(${listName}Vector vector) {
    this(vector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public Union${listName}Writer(${listName}Vector vector, NullableStructWriterFactory nullableStructWriterFactory) {
    this.vector = vector;
    this.writer = new PromotableWriter(vector.getDataVector(), vector, nullableStructWriterFactory);
  }

  public Union${listName}Writer(${listName}Vector vector, AbstractFieldWriter parent) {
    this(vector);
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  public void setValueCount(int count) {
    vector.setValueCount(count);
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void close() throws Exception {
    vector.close();
    writer.close();
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#assign vectName = capName />
  @Override
  public ${minor.class}Writer ${lowerName}() {
    return this;
  }

  <#if minor.typeParams?? >
  @Override
  public ${minor.class}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    return writer.${lowerName}(name<#list minor.typeParams as typeParam>, ${typeParam.name}</#list>);
  }
  </#if>

  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
    structName = name;
    return writer.${lowerName}(name);
  }

  </#list></#list>

  @Override
  public StructWriter struct() {
    inStruct = true;
    return this;
  }

  @Override
  public ListWriter list() {
    return writer;
  }

  @Override
  public ListWriter list(String name) {
    ListWriter listWriter = writer.list(name);
    return listWriter;
  }

  @Override
  public StructWriter struct(String name) {
    StructWriter structWriter = writer.struct(name);
    return structWriter;
  }

  @Override
  public MapWriter map() {
    return writer;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    writer.map(keysSorted);
    return writer;
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    MapWriter mapWriter = writer.map(name, keysSorted);
    return mapWriter;
  }

  <#if listName == "LargeList">
  @Override
  public void startList() {
    vector.startNewValue(idx());
    writer.setPosition(checkedCastToInt(vector.getOffsetBuffer().getLong((idx() + 1L) * OFFSET_WIDTH)));
    listStarted = true;
  }

  @Override
  public void endList() {
    vector.getOffsetBuffer().setLong((idx() + 1L) * OFFSET_WIDTH, writer.idx());
    setPosition(idx() + 1);
    listStarted = false;
  }
  <#else>
  @Override
  public void startList() {
    vector.startNewValue(idx());
    writer.setPosition(vector.getOffsetBuffer().getInt((idx() + 1L) * OFFSET_WIDTH));
    listStarted = true;
  }

  @Override
  public void endList() {
    vector.getOffsetBuffer().setInt((idx() + 1L) * OFFSET_WIDTH, writer.idx());
    setPosition(idx() + 1);
    listStarted = false;
  }
  </#if>

  @Override
  public void start() {
    writer.start();
  }

  @Override
  public void end() {
    writer.end();
    inStruct = false;
  }

  @Override
  public void writeNull() {
    if (!listStarted){
      vector.setNull(idx());
    } else {
      writer.writeNull();
    }
  }

  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
  @Override
  public void write${name}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    writer.write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    writer.setPosition(writer.idx()+1);
  }

  <#if is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
  @Override
  public void write(${name}Holder holder) {
    writer.write(holder);
    writer.setPosition(writer.idx()+1);
  }

  <#elseif minor.class?starts_with("Decimal")>
  public void write${name}(long start, ArrowBuf buffer, ArrowType arrowType) {
    writer.write${name}(start, buffer, arrowType);
    writer.setPosition(writer.idx()+1);
  }

  @Override
  public void write(${name}Holder holder) {
    writer.write(holder);
    writer.setPosition(writer.idx()+1);
  }

  public void write${name}(BigDecimal value) {
    writer.write${name}(value);
    writer.setPosition(writer.idx()+1);
  }

  public void writeBigEndianBytesTo${name}(byte[] value, ArrowType arrowType){
    writer.writeBigEndianBytesTo${name}(value, arrowType);
    writer.setPosition(writer.idx() + 1);
  }
  <#else>
  @Override
  public void write(${name}Holder holder) {
    writer.write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
    writer.setPosition(writer.idx()+1);
  }
  </#if>

    </#list>
  </#list>
}
</#list>
