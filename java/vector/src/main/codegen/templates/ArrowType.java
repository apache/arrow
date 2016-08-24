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

import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;

import java.util.Objects;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/types/pojo/ArrowType.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.types.pojo;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.Type;

import java.util.Objects;

public abstract class ArrowType {

  public abstract byte getTypeType();
  public abstract int getType(FlatBufferBuilder builder);
  public abstract <T> T accept(ArrowTypeVisitor<T> visitor);

  public static interface ArrowTypeVisitor<T> {
  <#list arrowTypes.types as type>
    T visit(${type.name} type);
  </#list>
  }

  <#list arrowTypes.types as type>
  <#assign name = type.name>
  <#assign fields = type.fields>
  public static class ${name} extends ArrowType {
    public static final byte TYPE_TYPE = Type.${name};
    <#if type.fields?size == 0>
    public static final ${name} INSTANCE = new ${name}();
    </#if>

    <#list fields as field>
    <#assign fieldName = field.name>
    <#assign fieldType = field.type>
    ${fieldType} ${fieldName};
    </#list>

    <#if type.fields?size != 0>
    public ${type.name}(<#list type.fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
      <#list type.fields as field>
      this.${field.name} = ${field.name};
      </#list>
    }
    </#if>

    @Override
    public byte getTypeType() {
      return TYPE_TYPE;
    }

    @Override
    public int getType(FlatBufferBuilder builder) {
      <#list type.fields as field>
      <#if field.type == "String">
      int ${field.name} = builder.createString(this.${field.name});
      </#if>
      </#list>
      org.apache.arrow.flatbuf.${type.name}.start${type.name}(builder);
      <#list type.fields as field>
      org.apache.arrow.flatbuf.${type.name}.add${field.name?cap_first}(builder, ${field.name});
      </#list>
      return org.apache.arrow.flatbuf.${type.name}.end${type.name}(builder);
    }

    <#list fields as field>
    public ${field.type} get${field.name?cap_first}() {
      return ${field.name};
    }
    </#list>

    public String toString() {
      return "${name}{"
      <#list fields as field>
      + ", " + ${field.name}
      </#list>
      + "}";
    }

    @Override
    public int hashCode() {
      return Objects.hash(<#list type.fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ${type.name})) {
        return false;
      }
      <#if type.fields?size == 0>
      return true;
      <#else>
      ${type.name} that = (${type.name}) obj;
      return
      <#list type.fields as field>Objects.equals(this.${field.name}, that.${field.name}) <#if field_has_next>&&<#else>;</#if>
      </#list>
      </#if>
    }

    @Override
    public <T> T accept(ArrowTypeVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }
  </#list>

  public static org.apache.arrow.vector.types.pojo.ArrowType getTypeForField(org.apache.arrow.flatbuf.Field field) {
    switch(field.typeType()) {
    <#list arrowTypes.types as type>
    <#assign name = type.name>
    <#assign nameLower = type.name?lower_case>
    <#assign fields = type.fields>
    case Type.${type.name}:
      org.apache.arrow.flatbuf.${type.name} ${nameLower}Type = (org.apache.arrow.flatbuf.${type.name}) field.type(new org.apache.arrow.flatbuf.${type.name}());
      return new ${type.name}(<#list type.fields as field>${nameLower}Type.${field.name}()<#if field_has_next>, </#if></#list>);
    </#list>
    default:
      throw new UnsupportedOperationException("Unsupported type: " + field.typeType());
    }
  }

  public static Int getInt(org.apache.arrow.flatbuf.Field field) {
    org.apache.arrow.flatbuf.Int intType = (org.apache.arrow.flatbuf.Int) field.type(new org.apache.arrow.flatbuf.Int());
    return new Int(intType.bitWidth(), intType.isSigned());
  }
}


