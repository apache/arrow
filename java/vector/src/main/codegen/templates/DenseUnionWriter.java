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

import org.apache.arrow.vector.complex.impl.NullableStructWriterFactory;
import org.apache.arrow.vector.types.Types;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/DenseUnionWriter.java" />


<#include "/@includes/license.ftl" />

        package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
        import org.apache.arrow.vector.complex.writer.BaseWriter;
        import org.apache.arrow.vector.types.Types.MinorType;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class DenseUnionWriter extends AbstractFieldWriter implements FieldWriter {

  DenseUnionVector data;

  private BaseWriter[] writers = new BaseWriter[Byte.MAX_VALUE + 1];
  private final NullableStructWriterFactory nullableStructWriterFactory;

  public DenseUnionWriter(DenseUnionVector vector) {
    this(vector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public DenseUnionWriter(DenseUnionVector vector, NullableStructWriterFactory nullableStructWriterFactory) {
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
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getStructWriter(idx()).start();
  }

  @Override
  public void end() {
    getStructWriter(idx()).end();
  }

  @Override
  public void startList() {
    int typeId = data.getTypeValue(idx());
    getListWriter(idx()).startList();
    data.setType(idx(), typeId);
  }

  @Override
  public void endList() {
    getListWriter(idx()).endList();
  }

  private StructWriter getStructWriter(int typeId) {
    StructWriter structWriter = (StructWriter) writers[typeId];
    if (structWriter == null) {
      structWriter = nullableStructWriterFactory.build(data.getStruct(typeId));
      structWriter.setPosition(idx());
      writers[typeId] = structWriter;
    }
    return structWriter;
  }

  public StructWriter asStruct(int typeId) {
    data.setType(idx(), typeId);
    return getStructWriter(typeId);
  }

  private ListWriter getListWriter(int typeId) {
    ListWriter listWriter = (ListWriter) writers[typeId];
    if (listWriter == null) {
      listWriter = new UnionListWriter(data.getList(idx()), nullableStructWriterFactory);
      listWriter.setPosition(idx());
      writers[typeId] = listWriter;
    }
    return listWriter;
  }

  public ListWriter asList(int typeId) {
    data.setType(idx(), typeId);
    return getListWriter(typeId);
  }

  BaseWriter getWriter(MinorType minorType, int typeId) {
    switch (minorType) {
      case STRUCT:
        return getStructWriter(typeId);
      case LIST:
        return getListWriter(typeId);
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
      writers[idx()] = ${uncappedName}Writer;
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
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getListWriter(typeId).setPosition(idx());
    return getListWriter(typeId).struct();
  }

  @Override
  public ListWriter list() {
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getListWriter(typeId).setPosition(idx());
    return getListWriter(typeId).list();
  }

  @Override
  public ListWriter list(String name) {
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getStructWriter(typeId).setPosition(idx());
    return getStructWriter(typeId).list(name);
  }

  @Override
  public StructWriter struct(String name) {
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getStructWriter(typeId).setPosition(idx());
    return getStructWriter(typeId).struct(name);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if !minor.typeParams?? >
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getStructWriter(typeId).setPosition(idx());
    return getStructWriter(typeId).${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    int typeId = data.getTypeValue(idx());
    data.setType(idx(), typeId);
    getListWriter(typeId).setPosition(idx());
    return getListWriter(typeId).${lowerName}();
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
