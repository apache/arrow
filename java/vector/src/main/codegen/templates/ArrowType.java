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
import org.apache.arrow.flatbuf.Type;

import java.io.IOException;
import java.util.Objects;

import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.UnionMode;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.IntervalUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Arrow types
 **/
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "name")
@JsonSubTypes({
<#list arrowTypes.types as type>
  @JsonSubTypes.Type(value = ArrowType.${type.name}.class, name = "${type.name?remove_ending("_")?lower_case}"),
</#list>
})
public abstract class ArrowType {

  private static class FloatingPointPrecisionSerializer extends JsonSerializer<Short> {
    @Override
    public void serialize(Short precision,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(Precision.name(precision));
    }
  }

  private static class FloatingPointPrecisionDeserializer extends JsonDeserializer<Short> {
    @Override
    public Short deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      String name = p.getText();
      switch(name) {
        case "HALF":
          return Precision.HALF;
        case "SINGLE":
          return Precision.SINGLE;
        case "DOUBLE":
          return Precision.DOUBLE;
        default:
          throw new IllegalArgumentException("unknown precision: " + name);
      }
    }
  }

  private static class UnionModeSerializer extends JsonSerializer<Short> {
    @Override
    public void serialize(Short mode,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(UnionMode.name(mode));
    }
  }

  private static class UnionModeDeserializer extends JsonDeserializer<Short> {
    @Override
    public Short deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      String name = p.getText();
      switch(name) {
        case "Sparse":
          return UnionMode.Sparse;
        case "Dense":
          return UnionMode.Dense;
        default:
          throw new IllegalArgumentException("unknown union mode: " + name);
      }
    }
  }

  private static class TimestampUnitSerializer extends JsonSerializer<Short> {
    @Override
    public void serialize(Short unit,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(TimeUnit.name(unit));
    }
  }

  private static class TimestampUnitDeserializer extends JsonDeserializer<Short> {
    @Override
    public Short deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      String name = p.getText();
      switch(name) {
        case "SECOND":
          return TimeUnit.SECOND;
        case "MILLISECOND":
          return TimeUnit.MILLISECOND;
        case "MICROSECOND":
          return TimeUnit.MICROSECOND;
        case "NANOSECOND":
          return TimeUnit.NANOSECOND;
        default:
          throw new IllegalArgumentException("unknown time unit: " + name);
      }
    }
  }

  private static class IntervalUnitSerializer extends JsonSerializer<Short> {
    @Override
    public void serialize(Short unit,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(IntervalUnit.name(unit));
    }
  }

  private static class IntervalUnitDeserializer extends JsonDeserializer<Short> {
    @Override
    public Short deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      String name = p.getText();
      switch(name) {
        case "YEAR_MONTH":
          return IntervalUnit.YEAR_MONTH;
        case "DAY_TIME":
          return IntervalUnit.DAY_TIME;
        default:
          throw new IllegalArgumentException("unknown interval unit: " + name);
      }
    }
  }

  @JsonIgnore
  public abstract byte getTypeType();
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
    ${field.type} ${field.name};
    </#list>

    <#if type.fields?size != 0>
    @JsonCreator
    public ${type.name}(
    <#list type.fields as field>
      <#if field.type == "short"> @JsonDeserialize(using = ${type.name}${field.name?cap_first}Deserializer.class) </#if>@JsonProperty("${field.name}") ${field.type} ${field.name}<#if field_has_next>, </#if>
    </#list>
    ) {
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
      org.apache.arrow.flatbuf.${type.name}.add${field.name?cap_first}(builder, this.${field.name});
      </#if>
      </#list>
      return org.apache.arrow.flatbuf.${type.name}.end${type.name}(builder);
    }

    <#list fields as field>
      <#if field.type == "short">
    @JsonSerialize(using = ${type.name}${field.name?cap_first}Serializer.class)
      </#if>
    public ${field.type} get${field.name?cap_first}() {
      return ${field.name};
    }
    </#list>

    public String toString() {
      return "${name}{"
      <#list fields as field>
        + <#if field.type == "int[]">java.util.Arrays.toString(${field.name})<#else>${field.name}</#if><#if field_has_next> + ", " </#if>
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
    <#assign name = type.name>
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
      return new ${type.name}(<#list type.fields as field>${field.name}<#if field_has_next>, </#if></#list>);
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


