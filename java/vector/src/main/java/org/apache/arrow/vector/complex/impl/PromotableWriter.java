/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.AbstractMapVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

/**
 * This FieldWriter implementation delegates all FieldWriter API calls to an inner FieldWriter. This inner field writer
 * can start as a specific type, and this class will promote the writer to a UnionWriter if a call is made that the specifically
 * typed writer cannot handle. A new UnionVector is created, wrapping the original vector, and replaces the original vector
 * in the parent vector, which can be either an AbstractMapVector or a ListVector.
 */
public class PromotableWriter extends AbstractPromotableFieldWriter {

  private final AbstractMapVector parentContainer;
  private final ListVector listVector;
  private final NullableMapWriterFactory nullableMapWriterFactory;
  private int position;

  private enum State {
    UNTYPED, SINGLE, UNION
  }

  private MinorType type;
  private ValueVector vector;
  private UnionVector unionVector;
  private State state;
  private FieldWriter writer;

  public PromotableWriter(ValueVector v, AbstractMapVector parentContainer) {
    this(v, parentContainer, NullableMapWriterFactory.getNullableMapWriterFactoryInstance());
  }

  public PromotableWriter(ValueVector v, AbstractMapVector parentContainer, NullableMapWriterFactory nullableMapWriterFactory) {
    this.parentContainer = parentContainer;
    this.listVector = null;
    this.nullableMapWriterFactory = nullableMapWriterFactory;
    init(v);
  }

  public PromotableWriter(ValueVector v, ListVector listVector) {
    this(v, listVector, NullableMapWriterFactory.getNullableMapWriterFactoryInstance());
  }

  public PromotableWriter(ValueVector v, ListVector listVector, NullableMapWriterFactory nullableMapWriterFactory) {
    this.listVector = listVector;
    this.parentContainer = null;
    this.nullableMapWriterFactory = nullableMapWriterFactory;
    init(v);
  }

  private void init(ValueVector v) {
    if (v instanceof UnionVector) {
      state = State.UNION;
      unionVector = (UnionVector) v;
      writer = new UnionWriter(unionVector, nullableMapWriterFactory);
    } else if (v instanceof ZeroVector) {
      state = State.UNTYPED;
    } else {
      setWriter(v);
    }
  }

  private void setWriter(ValueVector v) {
    state = State.SINGLE;
    vector = v;
    type = v.getMinorType();
    switch (type) {
      case MAP:
        writer = nullableMapWriterFactory.build((NullableMapVector) vector);
        break;
      case LIST:
        writer = new UnionListWriter((ListVector) vector, nullableMapWriterFactory);
        break;
      case UNION:
        writer = new UnionWriter((UnionVector) vector, nullableMapWriterFactory);
        break;
      default:
        writer = type.getNewFieldWriter(vector);
        break;
    }
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

  protected FieldWriter getWriter(MinorType type) {
    if (state == State.UNION) {
      ((UnionWriter)writer).getWriter(type);
    } else if (state == State.UNTYPED) {
      if (type == null) {
        // ???
        return null;
      }
      ValueVector v = listVector.addOrGetVector(type).getVector();
      v.allocateNew();
      setWriter(v);
      writer.setPosition(position);
    } else if (type != this.type) {
      promoteToUnion();
      ((UnionWriter)writer).getWriter(type);
    }
    return writer;
  }

  @Override
  public boolean isEmptyMap() {
    return writer.isEmptyMap();
  }

  protected FieldWriter getWriter() {
    return getWriter(type);
  }

  private FieldWriter promoteToUnion() {
    String name = vector.getField().getName();
    TransferPair tp = vector.getTransferPair(vector.getMinorType().name().toLowerCase(), vector.getAllocator());
    tp.transfer();
    if (parentContainer != null) {
      unionVector = parentContainer.addOrGet(name, MinorType.UNION, UnionVector.class);
      unionVector.allocateNew();
    } else if (listVector != null) {
      unionVector = listVector.promoteToUnion();
    }
    unionVector.addVector((FieldVector)tp.getTo());
    writer = new UnionWriter(unionVector, nullableMapWriterFactory);
    writer.setPosition(idx());
    for (int i = 0; i <= idx(); i++) {
      unionVector.getMutator().setType(i, vector.getMinorType());
    }
    vector = null;
    state = State.UNION;
    return writer;
  }

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
