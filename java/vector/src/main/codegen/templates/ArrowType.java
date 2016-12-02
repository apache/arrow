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
<@pp.changeOutputFile name="/org/apache/arrow/vector/types/pojo/ArrowType.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.types.pojo;

import com.google.flatbuffers.FlatBufferBuilder;

import java.util.Objects;

import org.apache.arrow.flatbuf.Type;

import org.apache.arrow.vector.types.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Arrow types
 **/
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "name")
@JsonSubTypes({
<#list arrowTypes.types as type>
  @JsonSubTypes.Type(value = ArrowType.${type.name?remove_ending("_")}.class, name = "${type.name?remove_ending("_")?lower_case}"),
</#list>
})
public abstract class ArrowType {

  public static enum ArrowTypeID {
    <#list arrowTypes.types as type>
    <#assign name = type.name>
    ${name?remove_ending("_")}(Type.${name}),
    </#list>
    NONE(Type.NONE);
  
    private final byte flatbufType;

    public byte getFlatbufID() {
      return this.flatbufType;
    }

    private ArrowTypeID(byte flatbufType) {
      this.flatbufType = flatbufType;
    }
  }

  @JsonIgnore
  public abstract ArrowTypeID getTypeID();
  public abstract int getType(FlatBufferBuilder builder);
  public abstract <T> T accept(ArrowTypeVisitor<T> visitor);

  /**
   * to visit the ArrowTypes
   * <code>
   *   type.accept(new ArrowTypeVisitor<Type>() {
   *   ...
   *   });
   * </code>
   */
  public static interface ArrowTypeVisitor<T> {
  <#list arrowTypes.types as type>
    T visit(${type.name?remove_ending("_")} type);
  </#list>
  }

  <#list arrowTypes.types as type>
  <#assign name = type.name?remove_ending("_")>
  <#assign fields = type.fields>
  public static class ${name} extends ArrowType {
    public static final ArrowTypeID TYPE_TYPE = ArrowTypeID.${name};
    <#if type.fields?size == 0>
    public static final ${name} INSTANCE = new ${name}();
    </#if>

    <#list fields as field>
    <#assign fieldType = field.valueType!field.type>
    ${fieldType} ${field.name};
    </#list>

    <#if type.fields?size != 0>
    @JsonCreator
    public ${type.name}(
    <#list type.fields as field>
    <#assign fieldType = field.valueType!field.type>
      @JsonProperty("${field.name}") ${fieldType} ${field.name}<#if field_has_next>, </#if>
    </#list>
    ) {
      <#list type.fields as field>
      this.${field.name} = ${field.name};
      </#list>
    }
    </#if>

    @Override
    public ArrowTypeID getTypeID() {
      return TYPE_TYPE;
    }

    @Override
    public int getType(FlatBufferBuilder builder) {
      <#list type.fields as field>
      <#if field.type == "String">
      int ${field.name} = this.${field.name} == null ? -1 : builder.createString(this.${field.name});
      </#if>
      <#if field.type == "int[]">
      int ${field.name} = this.${field.name} == null ? -1 : org.apache.arrow.flatbuf.${type.name}.create${field.name?cap_first}Vector(builder, this.${field.name});
      </#if>
      </#list>
      org.apache.arrow.flatbuf.${type.name}.start${type.name}(builder);
      <#list type.fields as field>
      <#if field.type == "String" || field.type == "int[]">
      if (this.${field.name} != null) {
        org.apache.arrow.flatbuf.${type.name}.add${field.name?cap_first}(builder, ${field.name});
      }
      <#else>
      org.apache.arrow.flatbuf.${type.name}.add${field.name?cap_first}(builder, this.${field.name}<#if field.valueType??>.getFlatbufID()</#if>);
      </#if>
      </#list>
      return org.apache.arrow.flatbuf.${type.name}.end${type.name}(builder);
    }

    <#list fields as field>
    <#assign fieldType = field.valueType!field.type>
    public ${fieldType} get${field.name?cap_first}() {
      return ${field.name};
    }
    </#list>

    public String toString() {
      return "${name}"
      <#if fields?size != 0>
        + "("
      <#list fields as field>
        +   <#if field.type == "int[]">java.util.Arrays.toString(${field.name})<#else>${field.name}</#if><#if field_has_next> + ", " </#if>
      </#list>
        + ")"
      </#if>
      ;
    }

    @Override
    public int hashCode() {
      return Objects.hash(<#list type.fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ${name})) {
        return false;
      }
      <#if type.fields?size == 0>
      return true;
      <#else>
      ${type.name} that = (${type.name}) obj;
      return <#list type.fields as field>Objects.deepEquals(this.${field.name}, that.${field.name}) <#if field_has_next>&&<#else>;</#if>
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
    <#assign name = type.name?remove_ending("_")>
    <#assign nameLower = type.name?lower_case>
    <#assign fields = type.fields>
    case Type.${type.name}: {
      org.apache.arrow.flatbuf.${type.name} ${nameLower}Type = (org.apache.arrow.flatbuf.${type.name}) field.type(new org.apache.arrow.flatbuf.${type.name}());
      <#list type.fields as field>
      <#if field.type == "int[]">
      ${field.type} ${field.name} = new int[${nameLower}Type.${field.name}Length()];
      for (int i = 0; i< ${field.name}.length; ++i) {
        ${field.name}[i] = ${nameLower}Type.${field.name}(i);
      }
      <#else>
      ${field.type} ${field.name} = ${nameLower}Type.${field.name}();
      </#if>
      </#list>
      return new ${name}(<#list type.fields as field><#if field.valueType??>${field.valueType}.fromFlatbufID(${field.name})<#else>${field.name}</#if><#if field_has_next>, </#if></#list>);
    }
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


