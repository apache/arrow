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
<#list ["Nullable", "Single"] as mode>
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/${mode}StructWriter.java" />
<#assign index = "idx()">
<#if mode == "Single">
<#assign containerClass = "NonNullableStructVector" />
<#else>
<#assign containerClass = "StructVector" />
</#if>

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import java.util.Map;
import java.util.HashMap;

import org.apache.arrow.vector.holders.RepeatedStructHolder;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/*
 * This class is generated using FreeMarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ${mode}StructWriter extends AbstractFieldWriter {

  protected final ${containerClass} container;
  private int initialCapacity;
  private final Map<String, FieldWriter> fields = new HashMap<>();
  public ${mode}StructWriter(${containerClass} container) {
    <#if mode == "Single">
    if (container instanceof StructVector) {
      throw new IllegalArgumentException("Invalid container: " + container);
    }
    </#if>
    this.container = container;
    this.initialCapacity = 0;
    for (Field child : container.getField().getChildren()) {
      MinorType minorType = Types.getMinorTypeForArrowType(child.getType());
      addVectorAsNullable = child.isNullable();
      switch (minorType) {
      case STRUCT:
        struct(child.getName());
        break;
      case LIST:
        list(child.getName());
        break;
      case MAP: {
        ArrowType.Map arrowType = (ArrowType.Map) child.getType();
        map(child.getName(), arrowType.getKeysSorted());
        break;
      }
      case DENSEUNION: {
        FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.DENSEUNION.getType(), null, null);
        DenseUnionWriter writer = new DenseUnionWriter(container.addOrGet(child.getName(), fieldType, DenseUnionVector.class), getNullableStructWriterFactory());
        fields.put(handleCase(child.getName()), writer);
        break;
      }
      case UNION:
        FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.UNION.getType(), null, null);
        UnionWriter writer = new UnionWriter(container.addOrGet(child.getName(), fieldType, UnionVector.class), getNullableStructWriterFactory());
        fields.put(handleCase(child.getName()), writer);
        break;
<#list vv.types as type><#list type.minor as minor>
<#assign lowerName = minor.class?uncap_first />
<#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
<#assign upperName = minor.class?upper_case />
      case ${upperName}: {
        <#if minor.typeParams?? >
        ${minor.arrowType} arrowType = (${minor.arrowType})child.getType();
        ${lowerName}(child.getName()<#list minor.typeParams as typeParam>, arrowType.get${typeParam.name?cap_first}()</#list>);
        <#else>
        ${lowerName}(child.getName());
        </#if>
        break;
      }
</#list></#list>
        default:
          throw new UnsupportedOperationException("Unknown type: " + minorType);
      }
    }
  }

  protected String handleCase(final String input) {
    return input.toLowerCase();
  }

  protected NullableStructWriterFactory getNullableStructWriterFactory() {
    return NullableStructWriterFactory.getNullableStructWriterFactoryInstance();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  public void setInitialCapacity(int initialCapacity) {
    this.initialCapacity = initialCapacity;
    container.setInitialCapacity(initialCapacity);
  }

  @Override
  public boolean isEmptyStruct() {
    return 0 == container.size();
  }

  @Override
  public Field getField() {
      return container.getField();
  }

  @Override
  public StructWriter struct(String name) {
    String finalName = handleCase(name);
    FieldWriter writer = fields.get(finalName);
    if(writer == null){
      int vectorCount=container.size();
      FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.STRUCT.getType(), null, null);
      StructVector vector = container.addOrGet(name, fieldType, StructVector.class);
      writer = new PromotableWriter(vector, container, getNullableStructWriterFactory());
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(finalName, writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.STRUCT);
      }
    }
    return writer;
  }

  @Override
  public void close() throws Exception {
    clear();
    container.close();
  }

  @Override
  public void allocate() {
    container.allocateNew();
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
  }

  @Override
  public void clear() {
    container.clear();
    for(final FieldWriter w : fields.values()) {
      w.clear();
    }
  }

  @Override
  public ListWriter list(String name) {
    String finalName = handleCase(name);
    FieldWriter writer = fields.get(finalName);
    int vectorCount = container.size();
    if(writer == null) {
      FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.LIST.getType(), null, null);
      writer = new PromotableWriter(container.addOrGet(name, fieldType, ListVector.class), container, getNullableStructWriterFactory());
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(finalName, writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.LIST);
      }
    }
    return writer;
  }

  @Override
  public MapWriter map(String name) {
    return map(name, false);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      MapVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new ArrowType.Map(keysSorted)
          ,null, null),
          MapVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.MAP, new ArrowType.Map(keysSorted));
      }
    }
    return writer;
  }

  public void setValueCount(int count) {
    container.setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for(final FieldWriter w: fields.values()) {
      w.setPosition(index);
    }
  }

  <#if mode="Nullable">
  @Override
  public void writeNull() {
    container.setNull(idx());
    setValueCount(idx()+1);
    super.setPosition(idx()+1);
  }
  </#if>

  @Override
  public void start() {
    <#if mode == "Single">
    <#else>
    container.setIndexDefined(idx());
    </#if>
  }

  @Override
  public void end() {
    setPosition(idx()+1);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#assign vectName = capName />

  <#if minor.typeParams?? >
  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public ${minor.class}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
  <#else>
  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
  </#if>
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      ${vectName}Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
          <#if minor.typeParams??>
            <#if minor.arrowTypeConstructorParams??>
              <#assign constructorParams = minor.arrowTypeConstructorParams />
            <#else>
              <#assign constructorParams = [] />
              <#list minor.typeParams?reverse as typeParam>
                <#assign constructorParams = constructorParams + [ typeParam.name ] />
              </#list>
            </#if>    
            new ${minor.arrowType}(${constructorParams?join(", ")}<#if minor.class?starts_with("Decimal")>, ${vectName}Vector.TYPE_WIDTH * 8</#if>)
          <#else>
            MinorType.${upperName}.getType()
          </#if>
          ,null, null),
          ${vectName}Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      } 
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        <#if minor.class?starts_with("Decimal")>
        ((PromotableWriter)writer).getWriter(MinorType.${upperName}<#if minor.class?starts_with("Decimal")>, new ${minor.arrowType}(precision, scale, ${vectName}Vector.TYPE_WIDTH * 8)</#if>);
        <#elseif is_timestamp_tz(minor.class) || minor.class == "Duration" || minor.class == "FixedSizeBinary">
          <#if minor.arrowTypeConstructorParams??>
            <#assign constructorParams = minor.arrowTypeConstructorParams />
          <#else>
            <#assign constructorParams = [] />
            <#list minor.typeParams?reverse as typeParam>
              <#assign constructorParams = constructorParams + [ typeParam.name ] />
            </#list>
          </#if>
        ArrowType arrowType = new ${minor.arrowType}(${constructorParams?join(", ")});
        ((PromotableWriter)writer).getWriter(MinorType.${upperName}, arrowType);
        <#else>
        ((PromotableWriter)writer).getWriter(MinorType.${upperName});
        </#if>
      }
    }
    return writer;
  }

  </#list></#list>

}
</#list>
