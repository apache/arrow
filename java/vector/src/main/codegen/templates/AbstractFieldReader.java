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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/AbstractFieldReader.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
abstract class AbstractFieldReader extends AbstractBaseReader implements FieldReader{

  AbstractFieldReader(){
    super();
  }

  /**
   * Returns true if the current value of the reader is not null
   * @return whether the current value is set
   */
  public boolean isSet() {
    return true;
  }

  @Override
  public Field getField() {
    fail("getField");
    return null;
  }

  <#list ["Object", "BigDecimal", "Integer", "Long", "Boolean",
          "Character", "DateTime", "Period", "Double", "Float",
          "Text", "String", "Byte", "Short", "byte[]"] as friendlyType>
  <#assign safeType=friendlyType />
  <#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>
  public ${friendlyType} read${safeType}(int arrayIndex) {
    fail("read${safeType}(int arrayIndex)");
    return null;
  }

  public ${friendlyType} read${safeType}() {
    fail("read${safeType}()");
    return null;
  }

  </#list>
  public void copyAsValue(MapWriter writer) {
    fail("CopyAsValue MapWriter");
  }

  public void copyAsField(String name, MapWriter writer) {
    fail("CopyAsField MapWriter");
  }

  public void copyAsField(String name, ListWriter writer) {
    fail("CopyAsFieldList");
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign boxedType = (minor.boxedType!type.boxedType) />
  public void read(${name}Holder holder) {
    fail("${name}");
  }

  public void read(Nullable${name}Holder holder) {
    fail("${name}");
  }

  public void read(int arrayIndex, ${name}Holder holder) {
    fail("Repeated${name}");
  }

  public void read(int arrayIndex, Nullable${name}Holder holder) {
    fail("Repeated${name}");
  }

  public void copyAsValue(${name}Writer writer) {
    fail("CopyAsValue${name}");
  }

  public void copyAsField(String name, ${name}Writer writer) {
    fail("CopyAsField${name}");
  }

  </#list></#list>
  public FieldReader reader(String name) {
    fail("reader(String name)");
    return null;
  }

  public FieldReader reader() {
    fail("reader()");
    return null;
  }

  public int size() {
    fail("size()");
    return -1;
  }

  private void fail(String name) {
    throw new IllegalArgumentException(String.format("You tried to read a [%s] type when you are using a field reader of type [%s].", name, this.getClass().getSimpleName()));
  }
}



