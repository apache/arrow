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

<@pp.dropOutputFile />
<#list ["Nullable", "Single"] as mode>
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${mode}MapWriter.java" />
<#assign index = "idx()">
<#if mode == "Single">
<#assign containerClass = "MapVector" />
<#else>
<#assign containerClass = "NullableMapVector" />
</#if>

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import java.util.Map;

import org.apache.arrow.vector.holders.RepeatedMapHolder;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;

import com.google.common.collect.Maps;

/*
 * This class is generated using FreeMarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ${mode}MapWriter extends AbstractFieldWriter {

  protected final ${containerClass} container;
  private final Map<String, FieldWriter> fields = Maps.newHashMap();

  public ${mode}MapWriter(${containerClass} container) {
    <#if mode == "Single">
    if (container instanceof NullableMapVector) {
      throw new IllegalArgumentException("Invalid container: " + container);
    }
    </#if>
    this.container = container;
    for (Field child : container.getField().getChildren()) {
      switch (Types.getMinorTypeForArrowType(child.getType())) {
      case MAP:
        map(child.getName());
        break;
      case LIST:
        list(child.getName());
        break;
      case UNION:
        UnionWriter writer = new UnionWriter(container.addOrGet(child.getName(), MinorType.UNION, UnionVector.class));
        fields.put(child.getName().toLowerCase(), writer);
        break;
<#list vv.types as type><#list type.minor as minor>
<#assign lowerName = minor.class?uncap_first />
<#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
<#assign upperName = minor.class?upper_case />
      case ${upperName}:
        <#if lowerName == "decimal" >
        Decimal decimal = (Decimal)child.getType();
        decimal(child.getName(), decimal.getScale(), decimal.getPrecision());
        <#else>
        ${lowerName}(child.getName());
       </#if>
        break;
</#list></#list>
      }
    }
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  @Override
  public boolean isEmptyMap() {
    return 0 == container.size();
  }

  @Override
  public Field getField() {
      return container.getField();
  }

  @Override
  public MapWriter map(String name) {
      FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null){
      int vectorCount=container.size();
      NullableMapVector vector = container.addOrGet(name, MinorType.MAP, NullableMapVector.class);
      writer = new PromotableWriter(vector, container);
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.MAP);
      }
    }
    return writer;
  }

  @Override
  public void close() throws Exception {
    clear();
    container.close();
  }

  @Override
  public void allocate() {
    container.allocateNew();
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
  }

  @Override
  public void clear() {
    container.clear();
    for(final FieldWriter w : fields.values()) {
      w.clear();
    }
  }

  @Override
  public ListWriter list(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    int vectorCount = container.size();
    if(writer == null) {
      writer = new PromotableWriter(container.addOrGet(name, MinorType.LIST, ListVector.class), container);
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.LIST);
      }
    }
    return writer;
  }

  public void setValueCount(int count) {
    container.getMutator().setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for(final FieldWriter w: fields.values()) {
      w.setPosition(index);
    }
  }

  @Override
  public void start() {
    <#if mode == "Single">
    <#else>
    container.getMutator().setIndexDefined(idx());
    </#if>
  }

  @Override
  public void end() {
    setPosition(idx()+1);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#assign vectName = capName />
  <#assign vectName = "Nullable${capName}" />

  <#if minor.class?starts_with("Decimal") >
  public ${minor.class}Writer ${lowerName}(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  public ${minor.class}Writer ${lowerName}(String name, int scale, int precision) {
  <#else>
  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
  </#if>
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      ${vectName}Vector v = container.addOrGet(name, MinorType.${upperName}, ${vectName}Vector.class<#if minor.class == "Decimal"> , new int[] {precision, scale}</#if>);
      writer = new PromotableWriter(v, container);
      vector = v;
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(name.toLowerCase(), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.${upperName});
      }
    }
    return writer;
  }

  </#list></#list>

}
</#list>
