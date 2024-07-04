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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/PromotableWriter.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

import java.util.Locale;
<#include "/@includes/vv_imports.ftl" />


public class PromotableWriter extends AbstractPromotableFieldWriter {

  protected final AbstractStructVector parentContainer;
  protected final ListVector listVector;
  protected final ListViewVector listViewVector;
  protected final FixedSizeListVector fixedListVector;
  protected final LargeListVector largeListVector;
  protected final NullableStructWriterFactory nullableStructWriterFactory;
  protected int position;
  protected static final int MAX_DECIMAL_PRECISION = 38;
  protected static final int MAX_DECIMAL256_PRECISION = 76;

  protected enum State {
    UNTYPED,
    SINGLE,
    UNION
  }

  protected MinorType type;
  protected ValueVector vector;
  protected UnionVector unionVector;
  protected State state;
  protected FieldWriter writer;

  /**
   * Constructs a new instance.
   *
   * @param v The vector to write.
   * @param parentContainer The parent container for the vector.
   */
  public PromotableWriter(ValueVector v, AbstractStructVector parentContainer) {
    this(v, parentContainer, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param parentContainer The parent container for the vector.
   * @param nullableStructWriterFactory The factory to create the delegate writer.
   */
  public PromotableWriter(
      ValueVector v,
      AbstractStructVector parentContainer,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.parentContainer = parentContainer;
    this.listVector = null;
    this.listViewVector = null;
    this.fixedListVector = null;
    this.largeListVector = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param listVector The vector that serves as a parent of v.
   */
  public PromotableWriter(ValueVector v, ListVector listVector) {
    this(v, listVector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param fixedListVector The vector that serves as a parent of v.
   */
  public PromotableWriter(ValueVector v, FixedSizeListVector fixedListVector) {
    this(v, fixedListVector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param largeListVector The vector that serves as a parent of v.
   */
  public PromotableWriter(ValueVector v, LargeListVector largeListVector) {
    this(v, largeListVector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param listVector The vector that serves as a parent of v.
   * @param nullableStructWriterFactory The factory to create the delegate writer.
   */
  public PromotableWriter(
      ValueVector v,
      ListVector listVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.listVector = listVector;
    this.listViewVector = null;
    this.parentContainer = null;
    this.fixedListVector = null;
    this.largeListVector = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param listViewVector The vector that serves as a parent of v.
   * @param nullableStructWriterFactory The factory to create the delegate writer.
   */
  public PromotableWriter(
      ValueVector v,
      ListViewVector listViewVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.listViewVector = listViewVector;
    this.listVector = null;
    this.parentContainer = null;
    this.fixedListVector = null;
    this.largeListVector = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param fixedListVector The vector that serves as a parent of v.
   * @param nullableStructWriterFactory The factory to create the delegate writer.
   */
  public PromotableWriter(
      ValueVector v,
      FixedSizeListVector fixedListVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.fixedListVector = fixedListVector;
    this.parentContainer = null;
    this.listVector = null;
    this.listViewVector = null;
    this.largeListVector = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  /**
   * Constructs a new instance.
   *
   * @param v The vector to initialize the writer with.
   * @param largeListVector The vector that serves as a parent of v.
   * @param nullableStructWriterFactory The factory to create the delegate writer.
   */
  public PromotableWriter(
      ValueVector v,
      LargeListVector largeListVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    this.largeListVector = largeListVector;
    this.fixedListVector = null;
    this.parentContainer = null;
    this.listVector = null;
    this.listViewVector = null;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
    init(v);
  }

  private void init(ValueVector v) {
    if (v instanceof UnionVector) {
      state = State.UNION;
      unionVector = (UnionVector) v;
      writer = new UnionWriter(unionVector, nullableStructWriterFactory);
    } else if (v instanceof NullVector) {
      state = State.UNTYPED;
    } else {
      setWriter(v);
    }
  }

  @Override
  public void setAddVectorAsNullable(boolean nullable) {
    super.setAddVectorAsNullable(nullable);
    if (writer instanceof AbstractFieldWriter) {
      ((AbstractFieldWriter) writer).setAddVectorAsNullable(nullable);
    }
  }

  protected void setWriter(ValueVector v) {
    state = State.SINGLE;
    vector = v;
    type = v.getMinorType();
    switch (type) {
      case STRUCT:
        writer = nullableStructWriterFactory.build((StructVector) vector);
        break;
      case LIST:
        writer = new UnionListWriter((ListVector) vector, nullableStructWriterFactory);
        break;
      case LISTVIEW:
        writer = new UnionListViewWriter((ListViewVector) vector, nullableStructWriterFactory);
        break;
      case MAP:
        writer = new UnionMapWriter((MapVector) vector);
        break;
      case UNION:
        writer = new UnionWriter((UnionVector) vector, nullableStructWriterFactory);
        break;
      default:
        writer = type.getNewFieldWriter(vector);
        break;
    }
  }

  @Override
  public StructWriter struct() {
    return getWriter(MinorType.LIST).struct();
  }

  @Override
  public ListWriter list() {
    return getWriter(MinorType.LIST).list();
  }

  @Override
  public ListWriter listView() {
    return getWriter(MinorType.LISTVIEW).listView();
  }

  @Override
  public MapWriter map() {
    return getWriter(MinorType.LIST).map();
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    return getWriter(MinorType.MAP, new ArrowType.Map(keysSorted));
  }

  @Override
  public StructWriter struct(String name) {
    return getWriter(MinorType.STRUCT).struct(name);
  }

  @Override
  public ListWriter list(String name) {
    return getWriter(MinorType.STRUCT).list(name);
  }

  @Override
  public ListWriter listView(String name) {
    return getWriter(MinorType.STRUCT).listView(name);
  }

  @Override
  public MapWriter map(String name) {
    return getWriter(MinorType.STRUCT).map(name);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    return getWriter(MinorType.STRUCT).map(name, keysSorted);
  }

  @Override
  public void writeNull() {
    FieldWriter w = getWriter();
    if (w != null) {
      w.writeNull();
    }
    setPosition(idx() + 1);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    FieldWriter w = getWriter();
    if (w == null) {
      position = index;
    } else {
      w.setPosition(index);
    }
  }

  protected boolean requiresArrowType(MinorType type) {
    return type == MinorType.DECIMAL
        || type == MinorType.MAP
        || type == MinorType.DURATION
        || type == MinorType.FIXEDSIZEBINARY
        || (type.name().startsWith("TIMESTAMP") && type.name().endsWith("TZ"));
  }

  @Override
  protected FieldWriter getWriter(MinorType type, ArrowType arrowType) {
    if (state == State.UNION) {
      if (requiresArrowType(type)) {
        ((UnionWriter) writer).getWriter(type, arrowType);
      } else {
        ((UnionWriter) writer).getWriter(type);
      }
    } else if (state == State.UNTYPED) {
      if (type == null) {
        // ???
        return null;
      }
      if (arrowType == null) {
        arrowType = type.getType();
      }
      FieldType fieldType = new FieldType(addVectorAsNullable, arrowType, null, null);
      ValueVector v;
      if (listVector != null) {
        v = listVector.addOrGetVector(fieldType).getVector();
      } else if (fixedListVector != null) {
        v = fixedListVector.addOrGetVector(fieldType).getVector();
      } else if (listViewVector != null) {
        v = listViewVector.addOrGetVector(fieldType).getVector();
      } else {
        v = largeListVector.addOrGetVector(fieldType).getVector();
      }
      v.allocateNew();
      setWriter(v);
      writer.setPosition(position);
    } else if (type != this.type) {
      promoteToUnion();
      if (requiresArrowType(type)) {
        ((UnionWriter) writer).getWriter(type, arrowType);
      } else {
        ((UnionWriter) writer).getWriter(type);
      }
    }
    return writer;
  }

  @Override
  public boolean isEmptyStruct() {
    return writer.isEmptyStruct();
  }

  @Override
  protected FieldWriter getWriter() {
    return writer;
  }

  protected FieldWriter promoteToUnion() {
    String name = vector.getField().getName();
    TransferPair tp =
        vector.getTransferPair(
            vector.getMinorType().name().toLowerCase(Locale.ROOT), vector.getAllocator());
    tp.transfer();
    if (parentContainer != null) {
      // TODO allow dictionaries in complex types
      unionVector = parentContainer.addOrGetUnion(name);
      unionVector.allocateNew();
    } else if (listVector != null) {
      unionVector = listVector.promoteToUnion();
    } else if (fixedListVector != null) {
      unionVector = fixedListVector.promoteToUnion();
    } else if (largeListVector != null) {
      unionVector = largeListVector.promoteToUnion();
    } else if (listViewVector != null) {
      unionVector = listViewVector.promoteToUnion();
    }
    unionVector.addVector((FieldVector) tp.getTo());
    writer = new UnionWriter(unionVector, nullableStructWriterFactory);
    writer.setPosition(idx());
    for (int i = 0; i <= idx(); i++) {
      unionVector.setType(i, vector.getMinorType());
    }
    vector = null;
    state = State.UNION;
    return writer;
  }

  @Override
  public void write(DecimalHolder holder) {
    getWriter(
        MinorType.DECIMAL,
        new ArrowType.Decimal(MAX_DECIMAL_PRECISION, holder.scale, /*bitWidth=*/ 128))
        .write(holder);
  }

  @Override
  public void writeDecimal(long start, ArrowBuf buffer, ArrowType arrowType) {
    getWriter(
        MinorType.DECIMAL,
        new ArrowType.Decimal(
            MAX_DECIMAL_PRECISION,
            ((ArrowType.Decimal) arrowType).getScale(),
            /*bitWidth=*/ 128))
        .writeDecimal(start, buffer, arrowType);
  }

  @Override
  public void writeDecimal(BigDecimal value) {
    getWriter(
        MinorType.DECIMAL,
        new ArrowType.Decimal(MAX_DECIMAL_PRECISION, value.scale(), /*bitWidth=*/ 128))
        .writeDecimal(value);
  }

  @Override
  public void writeBigEndianBytesToDecimal(byte[] value, ArrowType arrowType) {
    getWriter(
        MinorType.DECIMAL,
        new ArrowType.Decimal(
            MAX_DECIMAL_PRECISION,
            ((ArrowType.Decimal) arrowType).getScale(),
            /*bitWidth=*/ 128))
        .writeBigEndianBytesToDecimal(value, arrowType);
  }

  @Override
  public void write(Decimal256Holder holder) {
    getWriter(
        MinorType.DECIMAL256,
        new ArrowType.Decimal(MAX_DECIMAL256_PRECISION, holder.scale, /*bitWidth=*/ 256))
        .write(holder);
  }

  @Override
  public void writeDecimal256(long start, ArrowBuf buffer, ArrowType arrowType) {
    getWriter(
        MinorType.DECIMAL256,
        new ArrowType.Decimal(
            MAX_DECIMAL256_PRECISION,
            ((ArrowType.Decimal) arrowType).getScale(),
            /*bitWidth=*/ 256))
        .writeDecimal256(start, buffer, arrowType);
  }

  @Override
  public void writeDecimal256(BigDecimal value) {
    getWriter(
        MinorType.DECIMAL256,
        new ArrowType.Decimal(MAX_DECIMAL256_PRECISION, value.scale(), /*bitWidth=*/ 256))
        .writeDecimal256(value);
  }

  @Override
  public void writeBigEndianBytesToDecimal256(byte[] value, ArrowType arrowType) {
    getWriter(
        MinorType.DECIMAL256,
        new ArrowType.Decimal(
            MAX_DECIMAL256_PRECISION,
            ((ArrowType.Decimal) arrowType).getScale(),
            /*bitWidth=*/ 256))
        .writeBigEndianBytesToDecimal256(value, arrowType);
  }

  @Override
  public void writeVarBinary(byte[] value) {
    getWriter(MinorType.VARBINARY).writeVarBinary(value);
  }

  @Override
  public void writeVarBinary(byte[] value, int offset, int length) {
    getWriter(MinorType.VARBINARY).writeVarBinary(value, offset, length);
  }

  @Override
  public void writeVarBinary(ByteBuffer value) {
    getWriter(MinorType.VARBINARY).writeVarBinary(value);
  }

  @Override
  public void writeVarBinary(ByteBuffer value, int offset, int length) {
    getWriter(MinorType.VARBINARY).writeVarBinary(value, offset, length);
  }

  @Override
  public void writeLargeVarBinary(byte[] value) {
    getWriter(MinorType.LARGEVARBINARY).writeLargeVarBinary(value);
  }

  @Override
  public void writeLargeVarBinary(byte[] value, int offset, int length) {
    getWriter(MinorType.LARGEVARBINARY).writeLargeVarBinary(value, offset, length);
  }

  @Override
  public void writeLargeVarBinary(ByteBuffer value) {
    getWriter(MinorType.LARGEVARBINARY).writeLargeVarBinary(value);
  }

  @Override
  public void writeLargeVarBinary(ByteBuffer value, int offset, int length) {
    getWriter(MinorType.LARGEVARBINARY).writeLargeVarBinary(value, offset, length);
  }

  @Override
  public void writeVarChar(Text value) {
    getWriter(MinorType.VARCHAR).writeVarChar(value);
  }

  @Override
  public void writeVarChar(String value) {
    getWriter(MinorType.VARCHAR).writeVarChar(value);
  }

  @Override
  public void writeLargeVarChar(Text value) {
    getWriter(MinorType.LARGEVARCHAR).writeLargeVarChar(value);
  }

  @Override
  public void writeLargeVarChar(String value) {
    getWriter(MinorType.LARGEVARCHAR).writeLargeVarChar(value);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />

  <#if minor.typeParams?? >
  @Override
  public ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>) {
    return getWriter(MinorType.STRUCT).${lowerName}(name<#list minor.typeParams as typeParam>, ${typeParam.name}</#list>);
  }

  </#if>
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    return getWriter(MinorType.STRUCT).${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    return getWriter(MinorType.LIST).${lowerName}();
  }

  </#list></#list>


  @Override
  public void allocate() {
    getWriter().allocate();
  }

  @Override
  public void clear() {
    getWriter().clear();
  }

  @Override
  public Field getField() {
    return getWriter().getField();
  }

  @Override
  public int getValueCapacity() {
    return getWriter().getValueCapacity();
  }

  @Override
  public void close() throws Exception {
    getWriter().close();
  }

  public void setState(State state) {
    this.state = state;
  }

  public void setType(MinorType type) {
    this.type = type;
  }

  public void setUnionVector(UnionVector unionVector) {
    this.unionVector = unionVector;
  }

  public void setVector(ValueVector vector) {
    this.vector = vector;
  }

  public void setWriter(FieldWriter writer) {
    this.writer = writer;
  }

  public PromotableViewWriter promote() {
    PromotableViewWriter promotableViewWriter = new PromotableViewWriter(unionVector, parentContainer, nullableStructWriterFactory);
    promotableViewWriter.setPosition(position);
    promotableViewWriter.setWriter(writer);
    promotableViewWriter.setState(state);
    promotableViewWriter.setUnionVector(unionVector);
    promotableViewWriter.setType(MinorType.LISTVIEW);
    return promotableViewWriter;
  }
}