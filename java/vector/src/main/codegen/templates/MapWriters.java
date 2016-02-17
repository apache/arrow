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
<#list ["Single", "Repeated"] as mode>
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${mode}MapWriter.java" />
<#if mode == "Single">
<#assign containerClass = "MapVector" />
<#assign index = "idx()">
<#else>
<#assign containerClass = "RepeatedMapVector" />
<#assign index = "currentChildIndex">
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
  <#if mode == "Repeated">private int currentChildIndex = 0;</#if>

  private final boolean unionEnabled;

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent, boolean unionEnabled) {
    super(parent);
    this.container = container;
    this.unionEnabled = unionEnabled;
  }

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent) {
    this(container, parent, false);
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
  public MaterializedField getField() {
      return container.getField();
  }

  @Override
  public MapWriter map(String name) {
      FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null){
      int vectorCount=container.size();
        MapVector vector = container.addOrGet(name, MapVector.TYPE, MapVector.class);
      if(!unionEnabled){
        writer = new SingleMapWriter(vector, this);
      } else {
        writer = new PromotableWriter(vector, container);
      }
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(${index});
      fields.put(name.toLowerCase(), writer);
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
      if (!unionEnabled){
        writer = new SingleListWriter(name,container,this);
      } else{
        writer = new PromotableWriter(container.addOrGet(name, Types.optional(MinorType.LIST), ListVector.class), container);
      }
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(${index});
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }

  <#if mode == "Repeated">
  public void start() {
      // update the repeated vector to state that there is current+1 objects.
    final RepeatedMapHolder h = new RepeatedMapHolder();
    final RepeatedMapVector map = (RepeatedMapVector) container;
    final RepeatedMapVector.Mutator mutator = map.getMutator();

    // Make sure that the current vector can support the end position of this list.
    if(container.getValueCapacity() <= idx()) {
      mutator.setValueCount(idx()+1);
    }

    map.getAccessor().get(idx(), h);
    if (h.start >= h.end) {
      container.getMutator().startNewValue(idx());
    }
    currentChildIndex = container.getMutator().add(idx());
    for(final FieldWriter w : fields.values()) {
      w.setPosition(currentChildIndex);
    }
  }


  public void end() {
    // noop
  }
  <#else>

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
  }

  @Override
  public void end() {
  }

  </#if>

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
    final MajorType ${upperName}_TYPE = new MajorType(MinorType.${upperName}, DataMode.OPTIONAL, scale, precision, null, null);
  <#else>
  private static final MajorType ${upperName}_TYPE = Types.optional(MinorType.${upperName});
  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
  </#if>
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        ${vectName}Vector v = container.addOrGet(name, ${upperName}_TYPE, ${vectName}Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        ${vectName}Vector v = container.addOrGet(name, ${upperName}_TYPE, ${vectName}Vector.class);
        writer = new ${vectName}WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(${index});
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }

  </#list></#list>

}
</#list>
