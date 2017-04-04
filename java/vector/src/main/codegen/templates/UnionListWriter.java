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

import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionListWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

/**
 * Writer for ListVector and fixedSizeListVector. If multiple types are written, list will be promoted
 * to a union type.
 */
@SuppressWarnings("unused")
public class UnionListWriter extends AbstractFieldWriter {

  // only one of listVector/fixedSizeListVector will be set
  private final FixedSizeListVector fixedSizeListVector;
  private final ListVector listVector;
  // will only be set if listVector is set
  private final UInt4Vector offsets;

  // for convenience, is whichever list vector is set
  private final FieldVector vector;

  private PromotableWriter writer;
  private boolean inMap = false;
  private String mapName;
  private int lastIndex = 0;

  public UnionListWriter(ListVector vector) {
    this(vector, NullableMapWriterFactory.getNullableMapWriterFactoryInstance());
  }

  public UnionListWriter(ListVector vector, NullableMapWriterFactory nullableMapWriterFactory) {
    this.vector = vector;
    this.listVector = vector;
    this.fixedSizeListVector = null;
    this.offsets = vector.getOffsetVector();
    this.writer = new PromotableWriter(vector.getDataVector(), vector, nullableMapWriterFactory);
  }

  public UnionListWriter(FixedSizeListVector vector) {
    this(vector, NullableMapWriterFactory.getNullableMapWriterFactoryInstance());
  }

  public UnionListWriter(FixedSizeListVector vector, NullableMapWriterFactory nullableMapWriterFactory) {
    this.vector = vector;
    this.fixedSizeListVector = vector;
    this.listVector = null;
    this.offsets = null;
    this.writer = new PromotableWriter(vector.getDataVector(), vector, nullableMapWriterFactory);
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
    return null;
  }

  public void setValueCount(int count) {
    vector.getMutator().setValueCount(count);
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.class?starts_with("Decimal")>

  @Override
  public ${name}Writer <#if uncappedName == "int">integer<#else>${uncappedName}</#if>() {
    return this;
  }

  @Override
  public ${name}Writer <#if uncappedName == "int">integer<#else>${uncappedName}</#if>(String name) {
//    assert inMap;
    mapName = name;
    return writer.<#if uncappedName == "int">integer<#else>${uncappedName}</#if>(name);
  }

  </#if>

  </#list></#list>

  @Override
  public MapWriter map() {
    inMap = true;
    return this;
  }

  @Override
  public ListWriter list() {
    return writer;
  }

  @Override
  public ListWriter list(int size) {
    // prime the writer with the fixed size list
    writer.getWriter(new FixedSizeList(size));
    return writer;
  }

  @Override
  public ListWriter list(String name) {
    return writer.list(name);
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public void startList() {
    if (listVector != null) {
      listVector.getMutator().startNewValue(idx());
      writer.setPosition(offsets.getAccessor().get(idx() + 1));
    } else if (fixedSizeListVector != null) {
      fixedSizeListVector.getMutator().setNotNull(idx());
      writer.setPosition(idx() * fixedSizeListVector.getListSize());
    }
  }

  @Override
  public void endList() {
    if (listVector != null) {
      offsets.getMutator().set(idx() + 1, writer.idx());
    }
    setPosition(idx() + 1);
  }

  @Override
  public void start() {
//    assert inMap;
    writer.start();
  }

  @Override
  public void end() {
//    if (inMap) {
    writer.end();
    inMap = false;
//    }
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.class?starts_with("Decimal")>

  @Override
  public void write${name}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
//    assert !inMap;
    writer.write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    writer.setPosition(writer.idx()+1);
  }

  </#if>

  </#list></#list>

}
