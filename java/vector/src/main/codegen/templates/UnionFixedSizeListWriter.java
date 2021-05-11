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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.writer.Decimal256Writer;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.holders.Decimal256Holder;
import org.apache.arrow.vector.holders.DecimalHolder;


import java.lang.UnsupportedOperationException;
import java.math.BigDecimal;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionFixedSizeListWriter.java" />


<#include "/@includes/license.ftl" />

    package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
public class UnionFixedSizeListWriter extends AbstractFieldWriter {

  protected FixedSizeListVector vector;
  protected PromotableWriter writer;
  private boolean inStruct = false;
  private String structName;
  private final int listSize;

  public UnionFixedSizeListWriter(FixedSizeListVector vector) {
    this(vector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public UnionFixedSizeListWriter(FixedSizeListVector vector, NullableStructWriterFactory nullableStructWriterFactory) {
    this.vector = vector;
    this.writer = new PromotableWriter(vector.getDataVector(), vector, nullableStructWriterFactory);
    this.listSize = vector.getListSize();
  }

  public UnionFixedSizeListWriter(FixedSizeListVector vector, AbstractFieldWriter parent) {
    this(vector);
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  public void setValueCount(int count) {
    vector.setValueCount(count);
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void close() throws Exception {
    vector.close();
    writer.close();
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
  }
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#if uncappedName == "int" ><#assign uncappedName = "integer" /></#if>
  <#if !minor.typeParams?? >

  @Override
  public ${name}Writer ${uncappedName}() {
    return this;
  }

  @Override
  public ${name}Writer ${uncappedName}(String name) {
    structName = name;
    return writer.${uncappedName}(name);
  }
  </#if>
  </#list></#list>

  @Override
  public DecimalWriter decimal() {
    return this;
  }

  @Override
  public DecimalWriter decimal(String name, int scale, int precision) {
    return writer.decimal(name, scale, precision);
  }

  @Override
  public DecimalWriter decimal(String name) {
    return writer.decimal(name);
  }


  @Override
  public Decimal256Writer decimal256() {
    return this;
  }

  @Override
  public Decimal256Writer decimal256(String name, int scale, int precision) {
    return writer.decimal256(name, scale, precision);
  }

  @Override
  public Decimal256Writer decimal256(String name) {
    return writer.decimal256(name);
  }

  @Override
  public StructWriter struct() {
    inStruct = true;
    return this;
  }

  @Override
  public ListWriter list() {
    return writer;
  }

  @Override
  public ListWriter list(String name) {
    ListWriter listWriter = writer.list(name);
    return listWriter;
  }

  @Override
  public StructWriter struct(String name) {
    StructWriter structWriter = writer.struct(name);
    return structWriter;
  }

  @Override
  public MapWriter map() {
    return writer;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    writer.map(keysSorted);
    return writer;
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    MapWriter mapWriter = writer.map(name, keysSorted);
    return mapWriter;
  }

  @Override
  public void startList() {
    int start = vector.startNewValue(idx());
    writer.setPosition(start);
  }

  @Override
  public void endList() {
    setPosition(idx() + 1);
  }

  @Override
  public void start() {
    writer.start();
  }

  @Override
  public void end() {
    writer.end();
    inStruct = false;
  }

  @Override
  public void write(DecimalHolder holder) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.write(holder);
    writer.setPosition(writer.idx() + 1);
  }
 
  @Override
  public void write(Decimal256Holder holder) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.write(holder);
    writer.setPosition(writer.idx() + 1);
  }


  @Override
  public void writeNull() {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeNull();
  }

  public void writeDecimal(long start, ArrowBuf buffer, ArrowType arrowType) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeDecimal(start, buffer, arrowType);
    writer.setPosition(writer.idx() + 1);
  }

  public void writeDecimal(BigDecimal value) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeDecimal(value);
    writer.setPosition(writer.idx() + 1);
  }

  public void writeBigEndianBytesToDecimal(byte[] value, ArrowType arrowType) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeBigEndianBytesToDecimal(value, arrowType);
    writer.setPosition(writer.idx() + 1);
  }

  public void writeDecimal256(long start, ArrowBuf buffer, ArrowType arrowType) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeDecimal256(start, buffer, arrowType);
    writer.setPosition(writer.idx() + 1);
  }

  public void writeDecimal256(BigDecimal value) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeDecimal256(value);
    writer.setPosition(writer.idx() + 1);
  }

  public void writeBigEndianBytesToDecimal256(byte[] value, ArrowType arrowType) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.writeBigEndianBytesToDecimal256(value, arrowType);
    writer.setPosition(writer.idx() + 1);
  }


  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.typeParams?? >
  @Override
  public void write${name}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
    writer.setPosition(writer.idx() + 1);
  }

  public void write(${name}Holder holder) {
    if (writer.idx() >= (idx() + 1) * listSize) {
      throw new IllegalStateException(String.format("values at index %s is greater than listSize %s", idx(), listSize));
    }
    writer.write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
    writer.setPosition(writer.idx() + 1);
  }

      </#if>
    </#list>
  </#list>
}
