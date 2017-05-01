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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/writer/BaseWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.writer;

<#include "/@includes/vv_imports.ftl" />

/*
 * File generated from ${.template_name} using FreeMarker.
 */
@SuppressWarnings("unused")
public interface BaseWriter extends AutoCloseable, Positionable {
  int getValueCapacity();

  public interface MapWriter extends BaseWriter {

    Field getField();

    /**
     * Whether this writer is a map writer and is empty (has no children).
     *
     * <p>
     *   Intended only for use in determining whether to add dummy vector to
     *   avoid empty (zero-column) schema, as in JsonReader.
     * </p>
     * @return whether the map is empty
     */
    boolean isEmptyMap();

    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    <#if minor.class?starts_with("Decimal") >
    ${capName}Writer ${lowerName}(String name, int scale, int precision);
    </#if>
    ${capName}Writer ${lowerName}(String name);
    </#list></#list>

    void copyReaderToField(String name, FieldReader reader);
    MapWriter map(String name);
    ListWriter list(String name);
    void start();
    void end();
  }

  public interface ListWriter extends BaseWriter {
    void startList();
    void endList();
    MapWriter map();
    ListWriter list();
    void copyReader(FieldReader reader);

    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    ${capName}Writer ${lowerName}();
    </#list></#list>
  }

  public interface ScalarWriter extends
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first /> ${name}Writer, </#list></#list> BaseWriter {}

  public interface ComplexWriter {
    void allocate();
    void clear();
    void copyReader(FieldReader reader);
    MapWriter rootAsMap();
    ListWriter rootAsList();

    void setPosition(int index);
    void setValueCount(int count);
    void reset();
  }

  public interface MapOrListWriter {
    void start();
    void end();
    MapOrListWriter map(String name);
    MapOrListWriter listoftmap(String name);
    MapOrListWriter list(String name);
    boolean isMapWriter();
    boolean isListWriter();
    VarCharWriter varChar(String name);
    IntWriter integer(String name);
    BigIntWriter bigInt(String name);
    Float4Writer float4(String name);
    Float8Writer float8(String name);
    BitWriter bit(String name);
    VarBinaryWriter binary(String name);
  }
}
