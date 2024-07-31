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

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.types.Types.MinorType;

/** Reader for fixed size list vectors. */
public class UnionFixedSizeListReader extends AbstractFieldReader {

  private final FixedSizeListVector vector;
  private final ValueVector data;
  private final int listSize;

  private int currentOffset;

  /** Constructs a new instance that reads data in <code>vector</code>. */
  public UnionFixedSizeListReader(FixedSizeListVector vector) {
    this.vector = vector;
    this.data = vector.getDataVector();
    this.listSize = vector.getListSize();
  }

  @Override
  public boolean isSet() {
    return !vector.isNull(idx());
  }

  @Override
  public FieldReader reader() {
    return data.getReader();
  }

  @Override
  public Object readObject() {
    return vector.getObject(idx());
  }

  @Override
  public MinorType getMinorType() {
    return vector.getMinorType();
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    data.getReader().setPosition(index * listSize);
    currentOffset = 0;
  }

  @Override
  public void read(int index, UnionHolder holder) {
    setPosition(idx());
    for (int i = -1; i < index; i++) {
      if (!next()) {
        throw new IndexOutOfBoundsException("Requested " + index + ", size " + listSize);
      }
    }
    holder.reader = data.getReader();
    holder.isSet = vector.isNull(idx()) ? 0 : 1;
  }

  @Override
  public int size() {
    return listSize;
  }

  @Override
  public boolean next() {
    if (currentOffset < listSize) {
      data.getReader().setPosition(idx() * listSize + currentOffset++);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void copyAsValue(ListWriter writer) {
    ComplexCopier.copy(this, (FieldWriter) writer);
  }
}
