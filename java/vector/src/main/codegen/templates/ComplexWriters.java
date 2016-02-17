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
<#list vv.types as type>
<#list type.minor as minor>
<#list ["", "Nullable", "Repeated"] as mode>
<#assign name = mode + minor.class?cap_first />
<#assign eName = name />
<#assign javaType = (minor.javaType!type.javaType) />
<#assign fields = minor.fields!type.fields />

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${eName}WriterImpl.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using FreeMarker on the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ${eName}WriterImpl extends AbstractFieldWriter {

  private final ${name}Vector.Mutator mutator;
  final ${name}Vector vector;

  public ${eName}WriterImpl(${name}Vector vector, AbstractFieldWriter parent) {
    super(parent);
    this.mutator = vector.getMutator();
    this.vector = vector;
  }

  @Override
  public MaterializedField getField() {
    return vector.getField();
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void close() {
    vector.close();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  protected int idx() {
    return super.idx();
  }

  <#if mode == "Repeated">

  public void write(${minor.class?cap_first}Holder h) {
    mutator.addSafe(idx(), h);
    vector.getMutator().setValueCount(idx()+1);
  }

  public void write(Nullable${minor.class?cap_first}Holder h) {
    mutator.addSafe(idx(), h);
    vector.getMutator().setValueCount(idx()+1);
  }

  <#if !(minor.class == "Decimal9" || minor.class == "Decimal18" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense")>
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    mutator.addSafe(idx(), <#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    vector.getMutator().setValueCount(idx()+1);
  }
  </#if>

  public void setPosition(int idx) {
    super.setPosition(idx);
    mutator.startNewValue(idx);
  }


  <#else>

  public void write(${minor.class}Holder h) {
    mutator.setSafe(idx(), h);
    vector.getMutator().setValueCount(idx()+1);
  }

  public void write(Nullable${minor.class}Holder h) {
    mutator.setSafe(idx(), h);
    vector.getMutator().setValueCount(idx()+1);
  }

  <#if !(minor.class == "Decimal9" || minor.class == "Decimal18" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense")>
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    mutator.setSafe(idx(), <#if mode == "Nullable">1, </#if><#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    vector.getMutator().setValueCount(idx()+1);
  }

  <#if mode == "Nullable">

  public void writeNull() {
    mutator.setNull(idx());
    vector.getMutator().setValueCount(idx()+1);
  }
  </#if>
  </#if>
  </#if>
}

<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/writer/${eName}Writer.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.writer;

<#include "/@includes/vv_imports.ftl" />
@SuppressWarnings("unused")
public interface ${eName}Writer extends BaseWriter {
  public void write(${minor.class}Holder h);

  <#if !(minor.class == "Decimal9" || minor.class == "Decimal18" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense")>
  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>);
  </#if>
}

</#list>
</#list>
</#list>
