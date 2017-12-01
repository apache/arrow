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

import org.apache.arrow.vector.complex.impl.NullableMapWriterFactory;
import org.apache.arrow.vector.types.Types;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types.MinorType;

/**
 * This class is generated using freemarker and the ${.template_name} template.
 *
 * Example:
 * <pre>
 * {@code
 * UnionWriter unionWriter = new UnionWriter(unionVector);
 * unionWriter.asFloat4().writeFloat4(1.0);
 * unionWriter.asTimestamp(TimeUnit.SECOND, "UTC").write(1000);
 * }
 * </pre>
 *
 */
@SuppressWarnings("unused")
public class UnionWriter extends AbstractFieldWriter implements FieldWriter {

  UnionVector data;
  private MapWriter mapWriter;
  private UnionListWriter listWriter;
  private List<BaseWriter> writers = Lists.newArrayList();
  private final NullableMapWriterFactory nullableMapWriterFactory;

  public UnionWriter(UnionVector vector) {
    this(vector, NullableMapWriterFactory.getNullableMapWriterFactoryInstance());
  }

  public UnionWriter(UnionVector vector, NullableMapWriterFactory nullableMapWriterFactory) {
    data = vector;
    this.nullableMapWriterFactory = nullableMapWriterFactory;
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
    data.setType(idx(), MinorType.MAP);
    getMapWriter().start();
  }

  @Override
  public void end() {
    getMapWriter().end();
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

  private MapWriter getMapWriter() {
    if (mapWriter == null) {
      mapWriter = nullableMapWriterFactory.build(data.getMap());
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  public MapWriter asMap() {
    data.setType(idx(), MinorType.MAP);
    return getMapWriter();
  }

  private ListWriter getListWriter() {
    if (listWriter == null) {
      listWriter = new UnionListWriter(data.getList(), nullableMapWriterFactory);
      listWriter.setPosition(idx());
      writers.add(listWriter);
    }
    return listWriter;
  }

  public ListWriter asList() {
    data.setType(idx(), MinorType.LIST);
    return getListWriter();
  }

  /**
   * Get writer from a MinorType. For complex minor type, the method will throw excepion
   * because it cannot create writer from complex minor types. For simple MinorType, it will
   * create the writer.
   * @param minorType
   * @return
   */
  BaseWriter getWriter(MinorType minorType) {
    switch (minorType) {
      case MAP:
        return getMapWriter();
      case LIST:
        return getListWriter();
      <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
      case ${name?upper_case}:
      return get${name}Writer();
      </#list>
    </#list>
      default:
        throw new UnsupportedOperationException("Unknown type: " + minorType);
    }
  }

  /**
   * Get writer from a ArrowType. This will create the writer if it doesn't exist.
   * @param minorType
   * @return
   */
  BaseWriter getWriter(ArrowType type) {
    MinorType minorType = Types.getMinorTypeForArrowType(type);
    switch (minorType) {
    case MAP:
      return getMapWriter();
    case LIST:
      return getListWriter();
    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.typeParams??>
    case ${name?upper_case}:
      return get${name}Writer();
        <#else>
    case ${name?upper_case}:
      ArrowType.${name} ${uncappedName}Type = (ArrowType.${name}) type;
      return get${name}Writer(<#list minor.typeParams as typeParam>${uncappedName}Type.${typeParam.name}<#sep>, </#list>);
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

  private ${name}Writer ${uncappedName}Writer;

        <#if !minor.typeParams??>
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

  @Override
  public void write(${name}Holder holder) {
    data.setType(idx(), MinorType.${name?upper_case});
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
  }

  @Override
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    data.setType(idx(), MinorType.${name?upper_case});
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }

        <#else>
  public ${name}Writer get${name}Writer() {
    // returns existing writer
    ${name}Writer writer = ${uncappedName}Writer;
    assert writer != null;
    return writer;
  }

  public ${name}Writer get${name}Writer(<#list minor.typeParams as typeParam>${typeParam.type} ${typeParam.name}<#sep>, </#list>) {
    if (${uncappedName}Writer == null) {
      ${uncappedName}Writer = new ${name}WriterImpl(data.get${name}Vector(<#list minor.typeParams as typeParam>${typeParam.name}<#sep>, </#list>));
      ${uncappedName}Writer.setPosition(idx());
      writers.add(${uncappedName}Writer);
    }
    return ${uncappedName}Writer;
  }

  public ${name}Writer as${name}(<#list minor.typeParams as typeParam>${typeParam.type} ${typeParam.name}<#sep>, </#list>) {
    data.setType(idx(), MinorType.${name?upper_case});
    return get${name}Writer(<#list minor.typeParams as typeParam>${typeParam.name}<#sep>, </#list>);
  }

  @Override
  public void write(${name}Holder holder) {
    data.setType(idx(), MinorType.${name?upper_case});
    get${name}Writer().setPosition(idx());
    get${name}Writer(<#list minor.typeParams as typeParam>holder.${typeParam.name}<#sep>, </#list>).write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
  }

  @Override
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    data.setType(idx(), MinorType.${name?upper_case});
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
        </#if>
    </#list>
  </#list>

  public void writeNull() {
  }

  @Override
  public MapWriter map() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().map();
  }

  @Override
  public ListWriter list() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().list();
  }

  @Override
  public ListWriter list(String name) {
    data.setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().list(name);
  }

  @Override
  public MapWriter map(String name) {
    data.setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().map(name);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
    <#if !minor.typeParams?? >
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    data.setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().${lowerName}();
  }
    <#else>
  @Override
  public ${capName}Writer ${lowerName}(String name, <#list minor.typeParams as typeParam>${typeParam.type} ${typeParam.name}<#sep>, </#list>) {
    data.setType(idx(), MinorType.MAP);
    getMapWriter().setPosition(idx());
    return getMapWriter().${lowerName}(name, <#list minor.typeParams as typeParam>${typeParam.name}<#sep>, </#list>);
  }

  @Override
  public ${capName}Writer ${lowerName}(<#list minor.typeParams as typeParam>${typeParam.type} ${typeParam.name}<#sep>, </#list>) {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().${lowerName}(<#list minor.typeParams as typeParam>${typeParam.name}<#sep>, </#list>);
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
