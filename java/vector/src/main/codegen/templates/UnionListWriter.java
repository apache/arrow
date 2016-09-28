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

@SuppressWarnings("unused")
public class UnionListWriter extends AbstractFieldWriter {

  private ListVector vector;
  private UInt4Vector offsets;
  private PromotableWriter writer;
  private boolean inMap = false;
  private String mapName;
  private int lastIndex = 0;

  public UnionListWriter(ListVector vector) {
    this.vector = vector;
    this.writer = new PromotableWriter(vector.getDataVector(), vector);
    this.offsets = vector.getOffsetVector();
  }

  public UnionListWriter(ListVector vector, AbstractFieldWriter parent) {
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
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    ${name}Writer ${uncappedName}Writer = writer.<#if uncappedName == "int">integer<#else>${uncappedName}</#if>(name);
    return ${uncappedName}Writer;
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
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    writer.setPosition(nextOffset);
    return writer;
  }

  @Override
  public ListWriter list(String name) {
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    ListWriter listWriter = writer.list(name);
    return listWriter;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public void startList() {
    vector.getMutator().startNewValue(idx());
  }

  @Override
  public void endList() {

  }

  @Override
  public void start() {
//    assert inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    offsets.getMutator().setSafe(idx() + 1, nextOffset);
    writer.setPosition(nextOffset);
    writer.start();
  }

  @Override
  public void end() {
//    if (inMap) {
      writer.end();
      inMap = false;
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
//    }
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.class?starts_with("Decimal")>

  @Override
  public void write${name}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
//    assert !inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    vector.getMutator().setNotNull(idx());
    writer.setPosition(nextOffset);
    writer.write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
  }

  </#if>

  </#list></#list>

}
