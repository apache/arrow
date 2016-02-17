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

import org.apache.drill.common.types.TypeProtos.MinorType;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/AbstractPromotableFieldWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * A FieldWriter which delegates calls to another FieldWriter. The delegate FieldWriter can be promoted to a new type
 * when necessary. Classes that extend this class are responsible for handling promotion.
 *
 * This class is generated using freemarker and the ${.template_name} template.
 *
 */
@SuppressWarnings("unused")
abstract class AbstractPromotableFieldWriter extends AbstractFieldWriter {
  AbstractPromotableFieldWriter(FieldWriter parent) {
    super(parent);
  }

  /**
   * Retrieve the FieldWriter, promoting if it is not a FieldWriter of the specified type
   * @param type
   * @return
   */
  abstract protected FieldWriter getWriter(MinorType type);

  /**
   * Return the current FieldWriter
   * @return
   */
  abstract protected FieldWriter getWriter();

  @Override
  public void start() {
    getWriter(MinorType.MAP).start();
  }

  @Override
  public void end() {
    getWriter(MinorType.MAP).end();
  }

  @Override
  public void startList() {
    getWriter(MinorType.LIST).startList();
  }

  @Override
  public void endList() {
    getWriter(MinorType.LIST).endList();
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#if !minor.class?starts_with("Decimal") >
  @Override
  public void write(${name}Holder holder) {
    getWriter(MinorType.${name?upper_case}).write(holder);
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    getWriter(MinorType.${name?upper_case}).write${minor.class}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }

  </#if>
  </#list></#list>

  public void writeNull() {
  }

  @Override
  public MapWriter map() {
    return getWriter(MinorType.LIST).map();
  }

  @Override
  public ListWriter list() {
    return getWriter(MinorType.LIST).list();
  }

  @Override
  public MapWriter map(String name) {
    return getWriter(MinorType.MAP).map(name);
  }

  @Override
  public ListWriter list(String name) {
    return getWriter(MinorType.MAP).list(name);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if !minor.class?starts_with("Decimal") >

  @Override
  public ${capName}Writer ${lowerName}(String name) {
    return getWriter(MinorType.MAP).${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    return getWriter(MinorType.LIST).${lowerName}();
  }

  </#if>
  </#list></#list>

  public void copyReader(FieldReader reader) {
    getWriter().copyReader(reader);
  }

  public void copyReaderToField(String name, FieldReader reader) {
    getWriter().copyReaderToField(name, reader);
  }
}
