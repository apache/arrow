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

import org.apache.arrow.vector.complex.StructVector;
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
    byte typeId = data.getTypeId(idx());
    getStructWriter((byte) idx()).start();
  }

  @Override
  public void end() {
    byte typeId = data.getTypeId(idx());
    getStructWriter(typeId).end();
  }

  @Override
  public void startList() {
    byte typeId = data.getTypeId(idx());
    getListWriter(typeId).startList();
  }

  @Override
  public void endList() {
    byte typeId = data.getTypeId(idx());
    getListWriter(typeId).endList();
  }

  private StructWriter getStructWriter(byte typeId) {
    StructWriter structWriter = (StructWriter) writers[typeId];
    if (structWriter == null) {
      structWriter = nullableStructWriterFactory.build((StructVector) data.getVectorByType(typeId));
      writers[typeId] = structWriter;
    }
    return structWriter;
  }

  public StructWriter asStruct(byte typeId) {
    data.setTypeId(idx(), typeId);
    return getStructWriter(typeId);
  }

  private ListWriter getListWriter(byte typeId) {
    ListWriter listWriter = (ListWriter) writers[typeId];
    if (listWriter == null) {
      listWriter = new UnionListWriter((ListVector) data.getVectorByType(typeId), nullableStructWriterFactory);
      writers[typeId] = listWriter;
    }
    return listWriter;
  }

  public ListWriter asList(byte typeId) {
    data.setTypeId(idx(), typeId);
    return getListWriter(typeId);
  }

  private MapWriter getMapWriter(byte typeId) {
    MapWriter mapWriter = (MapWriter) writers[typeId];
    if (mapWriter == null) {
      mapWriter = new UnionMapWriter((MapVector) data.getVectorByType(typeId));
      writers[typeId] = mapWriter;
    }
    return mapWriter;
  }

  public MapWriter asMap(byte typeId) {
    data.setTypeId(idx(), typeId);
    return getMapWriter(typeId);
  }

  BaseWriter getWriter(byte typeId) {
    MinorType minorType = data.getVectorByType(typeId).getMinorType();
    switch (minorType) {
      case STRUCT:
        return getStructWriter(typeId);
      case LIST:
        return getListWriter(typeId);
      case MAP:
        return getMapWriter(typeId);
    <#list vv.types as type>
      <#list type.minor as minor>
        <#assign name = minor.class?cap_first />
        <#assign fields = minor.fields!type.fields />
        <#assign uncappedName = name?uncap_first/>
        <#if !minor.typeParams?? || minor.class?starts_with("Decimal")>
      case ${name?upper_case}:
      return get${name}Writer(typeId);
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
      <#if !minor.typeParams?? || minor.class?starts_with("Decimal")>

  private ${name}Writer get${name}Writer(byte typeId) {
    ${name}Writer writer = (${name}Writer) writers[typeId];
    if (writer == null) {
      writer = new ${name}WriterImpl((${name}Vector) data.getVectorByType(typeId));
      writers[typeId] = writer;
    }
    return writer;
  }

  public ${name}Writer as${name}(byte typeId) {
    data.setTypeId(idx(), typeId);
    return get${name}Writer(typeId);
  }

  @Override
  public void write(${name}Holder holder) {
    throw new UnsupportedOperationException();
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>, byte typeId<#if minor.class?starts_with("Decimal")>, ArrowType arrowType</#if>) {
    data.setTypeId(idx(), typeId);
    get${name}Writer(typeId).setPosition(data.getOffset(idx()));
    get${name}Writer(typeId).write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list><#if minor.class?starts_with("Decimal")>, arrowType</#if>);
  }
      </#if>
    </#list>
  </#list>

  public void writeNull() {
  }

  @Override
  public StructWriter struct() {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getListWriter(typeId).setPosition(data.getOffset(idx()));
    return getListWriter(typeId).struct();
  }

  @Override
  public ListWriter list() {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getListWriter(typeId).setPosition(data.getOffset(idx()));
    return getListWriter(typeId).list();
  }

  @Override
  public ListWriter list(String name) {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getStructWriter(typeId).setPosition(data.getOffset(idx()));
    return getStructWriter(typeId).list(name);
  }

  @Override
  public MapWriter map() {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getListWriter(typeId).setPosition(data.getOffset(idx()));
    return getMapWriter(typeId).map();
  }

  @Override
  public MapWriter map(String name) {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getStructWriter(typeId).setPosition(data.getOffset(idx()));
    return getStructWriter(typeId).map(name);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getStructWriter(typeId).setPosition(data.getOffset(idx()));
    return getStructWriter(typeId).map(name, keysSorted);
  }

  @Override
  public StructWriter struct(String name) {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getStructWriter(typeId).setPosition(data.getOffset(idx()));
    return getStructWriter(typeId).struct(name);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if !minor.typeParams?? || minor.class?starts_with("Decimal") >
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getStructWriter(typeId).setPosition(data.getOffset(idx()));
    return getStructWriter(typeId).${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getListWriter(typeId).setPosition(data.getOffset(idx()));
    return getListWriter(typeId).${lowerName}();
  }
  </#if>
  <#if minor.class?starts_with("Decimal")>
  public ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    byte typeId = data.getTypeId(idx());
    data.setTypeId(idx(), typeId);
    getStructWriter(typeId).setPosition(data.getOffset(idx()));
    return getStructWriter(typeId).${lowerName}(name<#list minor.typeParams as typeParam>, ${typeParam.name}</#list>);
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
