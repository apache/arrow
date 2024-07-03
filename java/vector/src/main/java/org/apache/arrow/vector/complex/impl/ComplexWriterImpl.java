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
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StateTool;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Field;

/** Concrete implementation of {@link ComplexWriter}. */
public class ComplexWriterImpl extends AbstractFieldWriter implements ComplexWriter {

  private NullableStructWriter structRoot;
  private UnionListWriter listRoot;
  private UnionMapWriter mapRoot;
  private final NonNullableStructVector container;

  Mode mode = Mode.INIT;
  private final String name;
  private final boolean unionEnabled;
  private final NullableStructWriterFactory nullableStructWriterFactory;

  private enum Mode {
    INIT,
    STRUCT,
    LIST,
    MAP
  }

  /**
   * Constructs a new instance.
   *
   * @param name The name of the writer (for tracking).
   * @param container A container for the data field to be written.
   * @param unionEnabled Unused.
   * @param caseSensitive Whether field names are case-sensitive (if false field names will be
   *     lowercase.
   */
  public ComplexWriterImpl(
      String name, NonNullableStructVector container, boolean unionEnabled, boolean caseSensitive) {
    this.name = name;
    this.container = container;
    this.unionEnabled = unionEnabled;
    nullableStructWriterFactory =
        caseSensitive
            ? NullableStructWriterFactory.getNullableCaseSensitiveStructWriterFactoryInstance()
            : NullableStructWriterFactory.getNullableStructWriterFactoryInstance();
  }

  public ComplexWriterImpl(String name, NonNullableStructVector container, boolean unionEnabled) {
    this(name, container, unionEnabled, false);
  }

  public ComplexWriterImpl(String name, NonNullableStructVector container) {
    this(name, container, false);
  }

  @Override
  public Field getField() {
    return container.getField();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  private void check(Mode... modes) {
    StateTool.check(mode, modes);
  }

  @Override
  public void reset() {
    setPosition(0);
  }

  @Override
  public void close() throws Exception {
    clear();
    structRoot.close();
    if (listRoot != null) {
      listRoot.close();
    }
  }

  @Override
  public void clear() {
    switch (mode) {
      case STRUCT:
        structRoot.clear();
        break;
      case LIST:
        listRoot.clear();
        break;
      case MAP:
        mapRoot.clear();
        break;
      default:
        break;
    }
  }

  @Override
  public void setValueCount(int count) {
    switch (mode) {
      case STRUCT:
        structRoot.setValueCount(count);
        break;
      case LIST:
        listRoot.setValueCount(count);
        break;
      case MAP:
        mapRoot.setValueCount(count);
        break;
      default:
        break;
    }
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    switch (mode) {
      case STRUCT:
        structRoot.setPosition(index);
        break;
      case LIST:
        listRoot.setPosition(index);
        break;
      case MAP:
        mapRoot.setPosition(index);
        break;
      default:
        break;
    }
  }

  /**
   * Returns a StructWriter, initializing it necessary from the constructor this instance was
   * constructed with.
   */
  public StructWriter directStruct() {
    Preconditions.checkArgument(name == null);

    switch (mode) {
      case INIT:
        structRoot = nullableStructWriterFactory.build((StructVector) container);
        structRoot.setPosition(idx());
        mode = Mode.STRUCT;
        break;

      case STRUCT:
        break;

      default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return structRoot;
  }

  @Override
  public StructWriter rootAsStruct() {
    switch (mode) {
      case INIT:
        // TODO allow dictionaries in complex types
        StructVector struct = container.addOrGetStruct(name);
        structRoot = nullableStructWriterFactory.build(struct);
        structRoot.setPosition(idx());
        mode = Mode.STRUCT;
        break;

      case STRUCT:
        break;

      default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return structRoot;
  }

  @Override
  public void allocate() {
    if (structRoot != null) {
      structRoot.allocate();
    } else if (listRoot != null) {
      listRoot.allocate();
    }
  }

  @Override
  public ListWriter rootAsList() {
    switch (mode) {
      case INIT:
        int vectorCount = container.size();
        // TODO allow dictionaries in complex types
        ListVector listVector = container.addOrGetList(name);
        if (container.size() > vectorCount) {
          listVector.allocateNew();
        }
        listRoot = new UnionListWriter(listVector, nullableStructWriterFactory);
        listRoot.setPosition(idx());
        mode = Mode.LIST;
        break;

      case LIST:
        break;

      default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return listRoot;
  }

  @Override
  public MapWriter rootAsMap(boolean keysSorted) {
    switch (mode) {
      case INIT:
        int vectorCount = container.size();
        // TODO allow dictionaries in complex types
        MapVector mapVector = container.addOrGetMap(name, keysSorted);
        if (container.size() > vectorCount) {
          mapVector.allocateNew();
        }
        mapRoot = new UnionMapWriter(mapVector);
        mapRoot.setPosition(idx());
        mode = Mode.MAP;
        break;

      case MAP:
        break;

      default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return mapRoot;
  }
}
