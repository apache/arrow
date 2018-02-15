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

import org.apache.arrow.vector.complex.impl.NullableStructWriterFactory;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types.MinorType;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class UnionWriter extends AbstractFieldWriter implements FieldWriter {

  UnionVector data;
  private StructWriter structWriter;
  private UnionListWriter listWriter;
  private List<BaseWriter> writers = Lists.newArrayList();
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

  BaseWriter getWriter(MinorType minorType) {
    switch (minorType) {
    case STRUCT:
      return getStructWriter();
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
      <#if !minor.typeParams?? >

  private ${name}Writer ${name?uncap_first}Writer;

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

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if !minor.typeParams?? >
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
