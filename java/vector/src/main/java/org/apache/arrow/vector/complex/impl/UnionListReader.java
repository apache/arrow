/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.UnionHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

public class UnionListReader extends AbstractFieldReader {

  private ListVector vector;
  private ValueVector data;
  private UInt4Vector offsets;

  public UnionListReader(ListVector vector) {
    this.vector = vector;
    this.data = vector.getDataVector();
    this.offsets = vector.getOffsetVector();
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  @Override
  public boolean isSet() {
    return !vector.getAccessor().isNull(idx());
  }

  private int currentOffset;
  private int maxOffset;

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    currentOffset = offsets.getAccessor().get(index) - 1;
    maxOffset = offsets.getAccessor().get(index + 1);
  }

  @Override
  public FieldReader reader() {
    return data.getReader();
  }

  @Override
  public Object readObject() {
    return vector.getAccessor().getObject(idx());
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.LIST;
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
    int size = maxOffset - currentOffset - 1;
    return size < 0 ? 0 : size;
  }

  @Override
  public boolean next() {
    if (currentOffset + 1 < maxOffset) {
      data.getReader().setPosition(++currentOffset);
      return true;
    } else {
      return false;
    }
  }

  public void copyAsValue(ListWriter writer) {
    ComplexCopier.copy(this, (FieldWriter) writer);
  }
}
