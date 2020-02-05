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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.holders.DecimalHolder;

import java.lang.UnsupportedOperationException;
import java.math.BigDecimal;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/UnionMapWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

/**
 * <p>Writer for MapVectors. This extends UnionListWriter to simplify writing map entries to a list
 * of struct elements, with "key" and "value" fields. The procedure for writing a map begin with
 * {@link #startMap()} followed by {@link #startEntry()}. An entry is written by using the
 * {@link #key()} writer to write the key, then the {@link #value()} writer to write a value. After
 * writing the value, call {@link #endEntry()} to complete the entry. Each map can have 1 or more
 * entries. When done writing entries, call {@link #endMap()} to complete the map.
 *
 * <p>NOTE: the MapVector can have NULL values by not writing to position. If a map is started with
 * {@link #startMap()}, then it must have a key written. The value of a map entry can be NULL by
 * not using the {@link #value()} writer.
 *
 * <p>Example to write the following map to position 5 of a vector
 * <pre>{@code
 *   // {
 *   //   1 -> 3,
 *   //   2 -> 4,
 *   //   3 -> NULL
 *   // }
 *
 *   UnionMapWriter writer = ...
 *
 *   writer.setPosition(5);
 *   writer.startMap();
 *   writer.startEntry();
 *   writer.key().integer().writeInt(1);
 *   writer.value().integer().writeInt(3);
 *   writer.endEntry();
 *   writer.startEntry();
 *   writer.key().integer().writeInt(2);
 *   writer.value().integer().writeInt(4);
 *   writer.endEntry();
 *   writer.startEntry();
 *   writer.key().integer().writeInt(3);
 *   writer.endEntry();
 *   writer.endMap();
 * </pre>
 * </p>
 */
@SuppressWarnings("unused")
public class UnionMapWriter extends UnionListWriter {

  /**
   * Current mode for writing map entries, set by calling {@link #key()} or {@link #value()}
   * and reset with a call to {@link #endEntry()}. With KEY mode, a struct writer with field
   * named "key" is returned. With VALUE mode, a struct writer with field named "value" is
   * returned. In OFF mode, the writer will behave like a standard UnionListWriter
   */
  private enum MapWriteMode {
    OFF,
    KEY,
    VALUE,
  }

  private MapWriteMode mode = MapWriteMode.OFF;
  private StructWriter entryWriter;

  public UnionMapWriter(MapVector vector) {
    super(vector);
    entryWriter = struct();
  }

  /** Start writing a map that consists of 1 or more entries. */
  public void startMap() {
    startList();
  }

  /** Complete the map. */
  public void endMap() {
    endList();
  }

  /**
   * Start a map entry that should be followed by calls to {@link #key()} and {@link #value()}
   * writers. Call {@link #endEntry()} to complete the entry.
   */
  public void startEntry() {
    writer.setAddVectorAsNullable(false);
    entryWriter.start();
  }

  /** Complete the map entry. */
  public void endEntry() {
    entryWriter.end();
    mode = MapWriteMode.OFF;
    writer.setAddVectorAsNullable(true);
  }

  /** Return the key writer that is used to write to the "key" field. */
  public UnionMapWriter key() {
    writer.setAddVectorAsNullable(false);
    mode = MapWriteMode.KEY;
    return this;
  }

  /** Return the value writer that is used to write to the "value" field. */
  public UnionMapWriter value() {
    writer.setAddVectorAsNullable(true);
    mode = MapWriteMode.VALUE;
    return this;
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#if uncappedName == "int" ><#assign uncappedName = "integer" /></#if>
  <#if !minor.typeParams?? >
  @Override
  public ${name}Writer ${uncappedName}() {
    switch (mode) {
      case KEY:
        return entryWriter.${uncappedName}(MapVector.KEY_NAME);
      case VALUE:
        return entryWriter.${uncappedName}(MapVector.VALUE_NAME);
      default:
        return this;
    }
  }

  </#if>
  </#list></#list>
  @Override
  public DecimalWriter decimal() {
    switch (mode) {
      case KEY:
        return entryWriter.decimal(MapVector.KEY_NAME);
      case VALUE:
        return entryWriter.decimal(MapVector.VALUE_NAME);
      default:
        return this;
    }
  }

  @Override
  public StructWriter struct() {
    switch (mode) {
      case KEY:
        return entryWriter.struct(MapVector.KEY_NAME);
      case VALUE:
        return entryWriter.struct(MapVector.VALUE_NAME);
      default:
        return super.struct();
    }
  }

  @Override
  public ListWriter list() {
    switch (mode) {
      case KEY:
        return entryWriter.list(MapVector.KEY_NAME);
      case VALUE:
        return entryWriter.list(MapVector.VALUE_NAME);
      default:
        return super.list();
    }
  }
}
