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
import org.apache.arrow.vector.complex.BaseRepeatedValueViewVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

/** {@link FieldReader} for listview of union types. */
public class UnionListViewReader extends AbstractFieldReader {

  private final ListViewVector vector;
  private final ValueVector data;
  private int currentOffset;
  private int size;

  /**
   * Constructor for UnionListViewReader.
   *
   * @param vector the vector to read from
   */
  public UnionListViewReader(ListViewVector vector) {
    this.vector = vector;
    this.data = vector.getDataVector();
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  @Override
  public boolean isSet() {
    return !vector.isNull(idx());
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    if (vector.getOffsetBuffer().capacity() == 0) {
      currentOffset = 0;
      size = 0;
    } else {
      currentOffset =
          vector.getOffsetBuffer().getInt(index * (long) BaseRepeatedValueViewVector.OFFSET_WIDTH);
      size = vector.getSizeBuffer().getInt(index * (long) BaseRepeatedValueViewVector.SIZE_WIDTH);
    }
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
    return MinorType.LISTVIEW;
  }

  @Override
  public void read(int index, UnionHolder holder) {
    setPosition(idx());
    for (int i = -1; i < index; i++) {
      next();
    }
    holder.reader = data.getReader();
    holder.isSet = data.getReader().isSet() ? 1 : 0;
  }

  @Override
  public int size() {
    return Math.max(size, 0);
  }

  @Override
  public boolean next() {
    // Here, the currentOffSet keeps track of the current position in the vector inside the list at
    // set position.
    // And, size keeps track of the elements count in the list, so to make sure we traverse
    // the full list, we need to check if the currentOffset is less than the currentOffset + size
    if (currentOffset < currentOffset + size) {
      data.getReader().setPosition(currentOffset++);
      return true;
    } else {
      return false;
    }
  }
}
