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
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/PromotableViewWriter.java" />

<#include "/@includes/license.ftl" />

    package org.apache.arrow.vector.complex.impl;

import java.util.Locale;
<#include "/@includes/vv_imports.ftl" />


public class PromotableViewWriter extends PromotableWriter {

  public PromotableViewWriter(ValueVector v, FixedSizeListVector fixedListVector) {
    super(v, fixedListVector);
  }

  public PromotableViewWriter(ValueVector v, FixedSizeListVector fixedListVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    super(v, fixedListVector, nullableStructWriterFactory);
  }

  public PromotableViewWriter(ValueVector v, LargeListVector largeListVector) {
    super(v, largeListVector);
  }

  public PromotableViewWriter(ValueVector v, LargeListVector largeListVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    super(v, largeListVector, nullableStructWriterFactory);
  }

  public PromotableViewWriter(ValueVector v, ListVector listVector) {
    super(v, listVector);
  }

  public PromotableViewWriter(ValueVector v, ListVector listVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    super(v, listVector, nullableStructWriterFactory);
  }

  public PromotableViewWriter(ValueVector v, ListViewVector listViewVector,
      NullableStructWriterFactory nullableStructWriterFactory) {
    super(v, listViewVector, nullableStructWriterFactory);
  }

  public PromotableViewWriter(ValueVector v, AbstractStructVector parentContainer) {
    super(v, parentContainer);
  }

  public PromotableViewWriter(ValueVector v, AbstractStructVector parentContainer,
      NullableStructWriterFactory nullableStructWriterFactory) {
    super(v, parentContainer, nullableStructWriterFactory);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />

  @Override
  public ${capName}Writer ${lowerName}() {
    return getWriter(MinorType.LISTVIEW).${lowerName}();
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
}