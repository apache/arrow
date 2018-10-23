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

  public interface StructWriter extends BaseWriter {

    Field getField();

    /**
     * Whether this writer is a struct writer and is empty (has no children).
     *
     * <p>
     *   Intended only for use in determining whether to add dummy vector to
     *   avoid empty (zero-column) schema, as in JsonReader.
     * </p>
     * @return whether the struct is empty
     */
    boolean isEmptyStruct();

    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    <#if minor.typeParams?? >
    ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>);
    </#if>
    ${capName}Writer ${lowerName}(String name);
    </#list></#list>

    void copyReaderToField(String name, FieldReader reader);
    StructWriter struct(String name);
    ListWriter list(String name);
    void start();
    void end();
  }

  public interface ListWriter extends BaseWriter {
    void startList();
    void endList();
    StructWriter struct();
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
    StructWriter rootAsStruct();
    ListWriter rootAsList();

    void setPosition(int index);
    void setValueCount(int count);
    void reset();
  }

  public interface StructOrListWriter {
    void start();
    void end();
    StructOrListWriter struct(String name);
    StructOrListWriter listoftstruct(String name);
    StructOrListWriter list(String name);
    boolean isStructWriter();
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
