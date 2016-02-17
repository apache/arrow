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
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;

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
  public boolean isSet() {
    return true;
  }

  MajorType type = new MajorType(MinorType.LIST, DataMode.OPTIONAL);

  public MajorType getType() {
    return type;
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
  public void read(int index, UnionHolder holder) {
    setPosition(idx());
    for (int i = -1; i < index; i++) {
      next();
    }
    holder.reader = data.getReader();
    holder.isSet = data.getReader().isSet() ? 1 : 0;
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
