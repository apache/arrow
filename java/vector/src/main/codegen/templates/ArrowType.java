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
<@pp.changeOutputFile name="/org/apache/arrow/vector/types/pojo/ArrowType.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.types.pojo;

import com.google.errorprone.annotations.Immutable;
import com.google.flatbuffers.FlatBufferBuilder;

import java.util.Objects;

import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.FieldVector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Arrow types
 * Source code generated using FreeMarker template ${.template_name}
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

@Immutable
public abstract class ArrowType {

  public static abstract class PrimitiveType extends ArrowType {

    private PrimitiveType() {
    }

    @Override
    public boolean isComplex() {
      return false;
    }
  }

  public static abstract class ComplexType extends ArrowType {

    private ComplexType() {
    }

    @Override
    public boolean isComplex() {
      return true;
    }
  }

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
  @JsonIgnore
  public abstract boolean isComplex();
  public abstract int getType(FlatBufferBuilder builder);
  public abstract <T> T accept(ArrowTypeVisitor<T> visitor);

  /**
   * to visit the ArrowTypes
   * <code>
   *   type.accept(new ArrowTypeVisitor&lt;Type&gt;() {
   *   ...
   *   });
   * </code>
   */
  public static interface ArrowTypeVisitor<T> {
  <#list arrowTypes.types as type>
    T visit(${type.name?remove_ending("_")} type);
  </#list>
    default T visit(ExtensionType type) {
      return type.storageType().accept(this);
    }
  }

  /**
   * to visit the Complex ArrowTypes and bundle Primitive ones in one case
   */
  public static abstract class ComplexTypeVisitor<T> implements ArrowTypeVisitor<T> {

    public T visit(PrimitiveType type) {
      throw new UnsupportedOperationException("Unexpected Primitive type: " + type);
    }

  <#list arrowTypes.types as type>
    <#if !type.complex>
    public final T visit(${type.name?remove_ending("_")} type) {
      return visit((PrimitiveType) type);
    }
    </#if>
  </#list>
  }

  /**
   * to visit the Primitive ArrowTypes and bundle Complex ones under one case
   */
  public static abstract class PrimitiveTypeVisitor<T> implements ArrowTypeVisitor<T> {

    public T visit(ComplexType type) {
      throw new UnsupportedOperationException("Unexpected Complex type: " + type);
    }

  <#list arrowTypes.types as type>
    <#if type.complex>
    public final T visit(${type.name?remove_ending("_")} type) {
      return visit((ComplexType) type);
    }
    </#if>
  </#list>
  }

  <#list arrowTypes.types as type>
  <#assign name = type.name?remove_ending("_")>
  <#assign fields = type.fields>
  public static class ${name} extends <#if type.complex>ComplexType<#else>PrimitiveType</#if> {
    public static final ArrowTypeID TYPE_TYPE = ArrowTypeID.${name};
    <#if type.fields?size == 0>
    public static final ${name} INSTANCE = new ${name}();
    <#else>

    <#list fields as field>
    <#assign fieldType = field.valueType!field.type>
    ${fieldType} ${field.name};
    </#list>


    <#if type.name == "Decimal">
    // Needed to support golden file integration tests.
    @JsonCreator
    public static Decimal createDecimal(
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale,
      @JsonProperty("bitWidth") Integer bitWidth) {

      return new Decimal(precision, scale, bitWidth == null ? 128 : bitWidth);
    }

    /**
     * Construct Decimal with 128 bits.
     * 
     * This is kept mainly for the sake of backward compatibility.
     * Please use {@link org.apache.arrow.vector.types.pojo.ArrowType.Decimal#Decimal(int, int, int)} instead.
     *
     * @deprecated This API will be removed in a future release.
     */
    @Deprecated
    public Decimal(int precision, int scale) {
      this(precision, scale, 128);
    }

    <#else>
    @JsonCreator
    </#if>
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

    <#list fields as field>
    <#assign fieldType = field.valueType!field.type>
    public ${fieldType} get${field.name?cap_first}() {
      return ${field.name};
    }
    </#list>
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
      return java.util.Arrays.deepHashCode(new Object[] {<#list type.fields as field>${field.name}<#if field_has_next>, </#if></#list>});
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

  /**
   * A user-defined data type that wraps an underlying storage type.
   */
  public abstract static class ExtensionType extends ComplexType {
    /** The on-wire type for this user-defined type. */
    public abstract ArrowType storageType();
    /** The name of this user-defined type. Used to identify the type during serialization. */
    public abstract String extensionName();
    /** Check equality of this type to another user-defined type. */
    public abstract boolean extensionEquals(ExtensionType other);
    /** Save any metadata for this type. */
    public abstract String serialize();
    /** Given saved metadata and the underlying storage type, construct a new instance of the user type. */
    public abstract ArrowType deserialize(ArrowType storageType, String serializedData);
    /** Construct a vector for the user type. */
    public abstract FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator);

    /** The field metadata key storing the name of the extension type. */
    public static final String EXTENSION_METADATA_KEY_NAME = "ARROW:extension:name";
    /** The field metadata key storing metadata for the extension type. */
    public static final String EXTENSION_METADATA_KEY_METADATA = "ARROW:extension:metadata";

    @Override
    public ArrowTypeID getTypeID() {
      return storageType().getTypeID();
    }

    @Override
    public int getType(FlatBufferBuilder builder) {
      return storageType().getType(builder);
    }

    public String toString() {
      return "ExtensionType(" + extensionName() + ", " + storageType().toString() + ")";
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.deepHashCode(new Object[] {storageType(), extensionName()});
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ExtensionType)) {
        return false;
      }
      return this.extensionEquals((ExtensionType) obj);
    }

    @Override
    public <T> T accept(ArrowTypeVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  private static final int defaultDecimalBitWidth = 128;

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
      <#if type.name == "Decimal">
      if (bitWidth != defaultDecimalBitWidth && bitWidth != 256) {
        throw new IllegalArgumentException("Library only supports 128-bit and 256-bit decimal values");
      }
      </#if>
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


