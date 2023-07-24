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

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.impl.NullableStructWriterFactory;
import org.apache.arrow.vector.types.Types;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types.MinorType;

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>


/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class UnionWriter extends AbstractFieldWriter implements FieldWriter {

  UnionVector data;
  private StructWriter structWriter;
  private UnionListWriter listWriter;
  private UnionMapWriter mapWriter;
  private List<BaseWriter> writers = new java.util.ArrayList<>();
  private final NullableStructWriterFactory nullableStructWriterFactory;

  public UnionWriter(UnionVector vector) {
    this(vector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public UnionWriter(UnionVector vector, NullableStructWriterFactory nullableStructWriterFactory) {
    data = vector;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseWriter writer : writers) {
      writer.setPosition(index);
    }
  }


  @Override
  public void start() {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().start();
  }

  @Override
  public void end() {
    getStructWriter().end();
  }

  @Override
  public void startList() {
    getListWriter().startList();
    data.setType(idx(), MinorType.LIST);
  }

  @Override
  public void endList() {
    getListWriter().endList();
  }

  @Override
  public void startMap() {
    getMapWriter().startMap();
    data.setType(idx(), MinorType.MAP);
  }

  @Override
  public void endMap() {
    getMapWriter().endMap();
  }

  @Override
  public void startEntry() {
    getMapWriter().startEntry();
  }

  @Override
  public MapWriter key() {
    return getMapWriter().key();
  }

  @Override
  public MapWriter value() {
    return getMapWriter().value();
  }

  @Override
  public void endEntry() {
    getMapWriter().endEntry();
  }

  private StructWriter getStructWriter() {
    if (structWriter == null) {
      structWriter = nullableStructWriterFactory.build(data.getStruct());
      structWriter.setPosition(idx());
      writers.add(structWriter);
    }
    return structWriter;
  }

  public StructWriter asStruct() {
    data.setType(idx(), MinorType.STRUCT);
    return getStructWriter();
  }

  private ListWriter getListWriter() {
    if (listWriter == null) {
      listWriter = new UnionListWriter(data.getList(), nullableStructWriterFactory);
      listWriter.setPosition(idx());
      writers.add(listWriter);
    }
    return listWriter;
  }

  public ListWriter asList() {
    data.setType(idx(), MinorType.LIST);
    return getListWriter();
  }

  private MapWriter getMapWriter() {
    if (mapWriter == null) {
      mapWriter = new UnionMapWriter(data.getMap(new ArrowType.Map(false)));
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  private MapWriter getMapWriter(ArrowType arrowType) {
    if (mapWriter == null) {
      mapWriter = new UnionMapWriter(data.getMap(arrowType));
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  public MapWriter asMap(ArrowType arrowType) {
    data.setType(idx(), MinorType.MAP);
    return getMapWriter(arrowType);
  }

  BaseWriter getWriter(MinorType minorType) {
    return getWriter(minorType, null);
  }

  BaseWriter getWriter(MinorType minorType, ArrowType arrowType) {
    switch (minorType) {
    case STRUCT:
      return getStructWriter();
    case LIST:
      return getListWriter();
    case MAP:
      return getMapWriter(arrowType);
    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
    case ${name?upper_case}:
      <#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
      return get${name}Writer(arrowType);
      <#else>
      return get${name}Writer();
      </#if>
        </#if>
      </#list>
    </#list>
    default:
      throw new UnsupportedOperationException("Unknown type: " + minorType);
    }
  }
  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
      <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">

  private ${name}Writer ${name?uncap_first}Writer;

  <#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
  private ${name}Writer get${name}Writer(ArrowType arrowType) {
    if (${uncappedName}Writer == null) {
      ${uncappedName}Writer = new ${name}WriterImpl(data.get${name}Vector(arrowType));
      ${uncappedName}Writer.setPosition(idx());
      writers.add(${uncappedName}Writer);
    }
    return ${uncappedName}Writer;
  }

  public ${name}Writer as${name}(ArrowType arrowType) {
    data.setType(idx(), MinorType.${name?upper_case});
    return get${name}Writer(arrowType);
  }
  <#else>
  private ${name}Writer get${name}Writer() {
    if (${uncappedName}Writer == null) {
      ${uncappedName}Writer = new ${name}WriterImpl(data.get${name}Vector());
      ${uncappedName}Writer.setPosition(idx());
      writers.add(${uncappedName}Writer);
    }
    return ${uncappedName}Writer;
  }

  public ${name}Writer as${name}() {
    data.setType(idx(), MinorType.${name?upper_case});
    return get${name}Writer();
  }
  </#if>

  @Override
  public void write(${name}Holder holder) {
    data.setType(idx(), MinorType.${name?upper_case});
    <#if minor.class?starts_with("Decimal")>
    ArrowType arrowType = new ArrowType.Decimal(holder.precision, holder.scale, ${name}Holder.WIDTH * 8);
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>, arrowType);
    <#elseif is_timestamp_tz(minor.class)>
    ArrowType.Timestamp arrowTypeWithoutTz = (ArrowType.Timestamp) MinorType.${name?upper_case?remove_ending("TZ")}.getType();
    ArrowType arrowType = new ArrowType.Timestamp(arrowTypeWithoutTz.getUnit(), holder.timezone);
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write(holder);
    <#elseif minor.class == "Duration">
    ArrowType arrowType = new ArrowType.Duration(holder.unit);
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write(holder);
    <#elseif minor.class == "FixedSizeBinary">
    ArrowType arrowType = new ArrowType.FixedSizeBinary(holder.byteWidth);
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write(holder);
    <#else>
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
    </#if>
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list><#if minor.class?starts_with("Decimal")>, ArrowType arrowType</#if>) {
    data.setType(idx(), MinorType.${name?upper_case});
    <#if minor.class?starts_with("Decimal")>
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>, arrowType);
    <#elseif is_timestamp_tz(minor.class)>
    ArrowType.Timestamp arrowTypeWithoutTz = (ArrowType.Timestamp) MinorType.${name?upper_case?remove_ending("TZ")}.getType();
    ArrowType arrowType = new ArrowType.Timestamp(arrowTypeWithoutTz.getUnit(), "UTC");
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    <#elseif minor.class == "Duration" || minor.class == "FixedSizeBinary">
    // This is expected to throw. There's nothing more that we can do here since we can't infer any
    // sort of default unit for the Duration or a default width for the FixedSizeBinary types.
    ArrowType arrowType = MinorType.${name?upper_case}.getType();
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    <#else>
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    </#if>
  }
  <#if minor.class?starts_with("Decimal")>
  public void write${name}(${friendlyType} value) {
    data.setType(idx(), MinorType.${name?upper_case});
    ArrowType arrowType = new ArrowType.Decimal(value.precision(), value.scale(), ${name}Vector.TYPE_WIDTH * 8);
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).write${name}(value);
  }

  public void writeBigEndianBytesTo${name}(byte[] value, ArrowType arrowType) {
    data.setType(idx(), MinorType.${name?upper_case});
    get${name}Writer(arrowType).setPosition(idx());
    get${name}Writer(arrowType).writeBigEndianBytesTo${name}(value, arrowType);
  }
  </#if>
      </#if>
    </#list>
  </#list>

  public void writeNull() {
  }

  @Override
  public StructWriter struct() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().struct();
  }

  @Override
  public ListWriter list() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().list();
  }

  @Override
  public ListWriter list(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().list(name);
  }

  @Override
  public StructWriter struct(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().struct(name);
  }

  @Override
  public MapWriter map() {
    data.setType(idx(), MinorType.MAP);
    getListWriter().setPosition(idx());
    return getListWriter().map();
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    data.setType(idx(), MinorType.MAP);
    getListWriter().setPosition(idx());
    return getListWriter().map(keysSorted);
  }

  @Override
  public MapWriter map(String name) {
    data.setType(idx(), MinorType.MAP);
    getStructWriter().setPosition(idx());
    return getStructWriter().map(name);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    data.setType(idx(), MinorType.MAP);
    getStructWriter().setPosition(idx());
    return getStructWriter().map(name, keysSorted);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().${lowerName}();
  }
  </#if>
  <#if minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
  @Override
  public ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().${lowerName}(name<#list minor.typeParams as typeParam>, ${typeParam.name}</#list>);
  }
  </#if>
  </#list></#list>

  @Override
  public void allocate() {
    data.allocateNew();
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public void close() throws Exception {
    data.close();
  }

  @Override
  public Field getField() {
    return data.getField();
  }

  @Override
  public int getValueCapacity() {
    return data.getValueCapacity();
  }
}
