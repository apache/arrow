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


import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionReader.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/**
 * Source code generated using FreeMarker template ${.template_name}
 */
@SuppressWarnings("unused")
public class UnionReader extends AbstractFieldReader {

  private static final int NUM_SUPPORTED_TYPES = 48;

  private BaseReader[] readers = new BaseReader[NUM_SUPPORTED_TYPES];
  public UnionVector data;
  
  public UnionReader(UnionVector data) {
    this.data = data;
  }

  public MinorType getMinorType() {
    return TYPES[data.getTypeValue(idx())];
  }

  private static MinorType[] TYPES = new MinorType[NUM_SUPPORTED_TYPES];

  static {
    for (MinorType minorType : MinorType.values()) {
      TYPES[minorType.ordinal()] = minorType;
    }
  }

  @Override
  public Field getField() {
    return data.getField();
  }

  public boolean isSet(){
    return !data.isNull(idx());
  }

  public void read(UnionHolder holder) {
    holder.reader = this;
    holder.isSet = this.isSet() ? 1 : 0;
  }

  public void read(int index, UnionHolder holder) {
    getList().read(index, holder);
  }

  private FieldReader getReaderForIndex(int index) {
    int typeValue = data.getTypeValue(index);
    FieldReader reader = (FieldReader) readers[typeValue];
    if (reader != null) {
      return reader;
    }
    switch (MinorType.values()[typeValue]) {
    case NULL:
      return NullReader.INSTANCE;
    case STRUCT:
      return (FieldReader) getStruct();
    case LIST:
      return (FieldReader) getList();
    case MAP:
      return (FieldReader) getMap();
    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
    case ${name?upper_case}:
      return (FieldReader) get${name}();
        </#if>
      </#list>
    </#list>
    default:
      throw new UnsupportedOperationException("Unsupported type: " + MinorType.values()[typeValue]);
    }
  }

  private SingleStructReaderImpl structReader;

  private StructReader getStruct() {
    if (structReader == null) {
      structReader = (SingleStructReaderImpl) data.getStruct().getReader();
      structReader.setPosition(idx());
      readers[MinorType.STRUCT.ordinal()] = structReader;
    }
    return structReader;
  }

  private UnionListReader listReader;

  private FieldReader getList() {
    if (listReader == null) {
      listReader = new UnionListReader(data.getList());
      listReader.setPosition(idx());
      readers[MinorType.LIST.ordinal()] = listReader;
    }
    return listReader;
  }

  private UnionMapReader mapReader;

  private FieldReader getMap() {
    if (mapReader == null) {
      mapReader = new UnionMapReader(data.getMap());
      mapReader.setPosition(idx());
      readers[MinorType.MAP.ordinal()] = mapReader;
    }
    return mapReader;
  }

  @Override
  public java.util.Iterator<String> iterator() {
    return getStruct().iterator();
  }

  @Override
  public void copyAsValue(UnionWriter writer) {
    writer.data.copyFrom(idx(), writer.idx(), data);
  }

  <#list ["Object", "BigDecimal", "Short", "Integer", "Long", "Boolean",
          "LocalDateTime", "Duration", "Period", "Double", "Float",
          "Character", "Text", "Byte", "byte[]", "PeriodDuration"] as friendlyType>
  <#assign safeType=friendlyType />
  <#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>

  @Override
  public ${friendlyType} read${safeType}() {
    return getReaderForIndex(idx()).read${safeType}();
  }

  </#list>

  public int size() {
    return getReaderForIndex(idx()).size();
  }

  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign uncappedName = name?uncap_first/>
      <#assign boxedType = (minor.boxedType!type.boxedType) />
      <#assign javaType = (minor.javaType!type.javaType) />
      <#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
      <#assign safeType=friendlyType />
      <#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>
      <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">

  private ${name}ReaderImpl ${uncappedName}Reader;

  private ${name}ReaderImpl get${name}() {
    if (${uncappedName}Reader == null) {
      ${uncappedName}Reader = new ${name}ReaderImpl(data.get${name}Vector());
      ${uncappedName}Reader.setPosition(idx());
      readers[MinorType.${name?upper_case}.ordinal()] = ${uncappedName}Reader;
    }
    return ${uncappedName}Reader;
  }

  public void read(Nullable${name}Holder holder){
    getReaderForIndex(idx()).read(holder);
  }

  public void copyAsValue(${name}Writer writer){
    getReaderForIndex(idx()).copyAsValue(writer);
  }
      </#if>
    </#list>
  </#list>

  @Override
  public void copyAsValue(ListWriter writer) {
    ComplexCopier.copy(this, (FieldWriter) writer);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseReader reader : readers) {
      if (reader != null) {
        reader.setPosition(index);
      }
    }
  }

  public FieldReader reader(String name){
    return getStruct().reader(name);
  }

  public FieldReader reader() {
    return getList().reader();
  }

  public boolean next() {
    return getReaderForIndex(idx()).next();
  }
}
