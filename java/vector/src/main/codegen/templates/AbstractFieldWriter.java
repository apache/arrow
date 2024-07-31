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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/AbstractFieldWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 * Note that changes to the AbstractFieldWriter template should also get reflected in the
 * AbstractPromotableFieldWriter, ComplexWriters, UnionFixedSizeListWriter, UnionListWriter
 * and UnionWriter templates and the PromotableWriter concrete code.
 */
@SuppressWarnings("unused")
abstract class AbstractFieldWriter extends AbstractBaseWriter implements FieldWriter {

  protected boolean addVectorAsNullable = true;

  /**
   * Set flag to control the FieldType.nullable property when a writer creates a new vector.
   * If true then vectors created will be nullable, this is the default behavior. If false then
   * vectors created will be non-nullable.
   *
   * @param nullable Whether or not to create nullable vectors (default behavior is true)
   */
  public void setAddVectorAsNullable(boolean nullable) {
    addVectorAsNullable = nullable;
  }

  @Override
  public void start() {
    throw new IllegalStateException(String.format("You tried to start when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void end() {
    throw new IllegalStateException(String.format("You tried to end when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void startList() {
    throw new IllegalStateException(String.format("You tried to start a list when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void endList() {
    throw new IllegalStateException(String.format("You tried to end a list when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void startMap() {
    throw new IllegalStateException(String.format("You tried to start a map when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void endMap() {
    throw new IllegalStateException(String.format("You tried to end a map when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void startEntry() {
    throw new IllegalStateException(String.format("You tried to start a map entry when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public MapWriter key() {
    throw new IllegalStateException(String.format("You tried to start a map key when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public MapWriter value() {
    throw new IllegalStateException(String.format("You tried to start a map value when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  @Override
  public void endEntry() {
    throw new IllegalStateException(String.format("You tried to end a map entry when you are using a ValueWriter of type %s.", this.getClass().getSimpleName()));
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
  @Override
  public void write(${name}Holder holder) {
    fail("${name}");
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    fail("${name}");
  }

  <#if minor.class?starts_with("Decimal")>
  public void write${minor.class}(${friendlyType} value) {
    fail("${name}");
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>, ArrowType arrowType) {
    fail("${name}");
  }

  public void writeBigEndianBytesTo${minor.class}(byte[] value) {
    fail("${name}");
  }

  public void writeBigEndianBytesTo${minor.class}(byte[] value, ArrowType arrowType) {
    fail("${name}");
  }
  </#if>

  <#if minor.class?ends_with("VarBinary")>
  public void write${minor.class}(byte[] value) {
    fail("${name}");
  }

  public void write${minor.class}(byte[] value, int offset, int length) {
    fail("${name}");
  }

  public void write${minor.class}(ByteBuffer value) {
    fail("${name}");
  }

  public void write${minor.class}(ByteBuffer value, int offset, int length) {
    fail("${name}");
  }
  </#if>

  <#if minor.class?ends_with("VarChar")>
  public void write${minor.class}(${friendlyType} value) {
    fail("${name}");
  }

  public void write${minor.class}(String value) {
    fail("${name}");
  }
  </#if>

  </#list></#list>

  public void writeNull() {
    fail("${name}");
  }

  /**
   * This implementation returns {@code false}.
   * <p>  
   *   Must be overridden by struct writers.
   * </p>  
   */
  @Override
  public boolean isEmptyStruct() {
    return false;
  }

  @Override
  public StructWriter struct() {
    fail("Struct");
    return null;
  }

  @Override
  public ListWriter list() {
    fail("List");
    return null;
  }

  @Override
  public MapWriter map() {
    fail("Map");
    return null;
  }

  @Override
  public StructWriter struct(String name) {
    fail("Struct");
    return null;
  }

  @Override
  public ListWriter list(String name) {
    fail("List");
    return null;
  }

  @Override
  public MapWriter map(String name) {
    fail("Map");
    return null;
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    fail("Map");
    return null;
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    fail("Map");
    return null;
  }
  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if minor.typeParams?? >

  @Override
  public ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    fail("${capName}(" + <#list minor.typeParams as typeParam>"${typeParam.name}: " + ${typeParam.name} + ", " + </#list>")");
    return null;
  }
  </#if>

  @Override
  public ${capName}Writer ${lowerName}(String name) {
    fail("${capName}");
    return null;
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    fail("${capName}");
    return null;
  }

  </#list></#list>

  public void copyReader(FieldReader reader) {
    fail("Copy FieldReader");
  }

  public void copyReaderToField(String name, FieldReader reader) {
    fail("Copy FieldReader to STring");
  }

  private void fail(String name) {
    throw new IllegalArgumentException(String.format("You tried to write a %s type when you are using a ValueWriter of type %s.", name, this.getClass().getSimpleName()));
  }
}
