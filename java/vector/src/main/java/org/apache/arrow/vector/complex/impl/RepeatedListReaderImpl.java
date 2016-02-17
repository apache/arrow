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


import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.RepeatedListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.holders.RepeatedListHolder;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;

public class RepeatedListReaderImpl extends AbstractFieldReader{
  private static final int NO_VALUES = Integer.MAX_VALUE - 1;
  private static final MajorType TYPE = new MajorType(MinorType.LIST, DataMode.REPEATED);
  private final String name;
  private final RepeatedListVector container;
  private FieldReader reader;

  public RepeatedListReaderImpl(String name, RepeatedListVector container) {
    super();
    this.name = name;
    this.container = container;
  }

  @Override
  public MajorType getType() {
    return TYPE;
  }

  @Override
  public void copyAsValue(ListWriter writer) {
    if (currentOffset == NO_VALUES) {
      return;
    }
    RepeatedListWriter impl = (RepeatedListWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), container);
  }

  @Override
  public void copyAsField(String name, MapWriter writer) {
    if (currentOffset == NO_VALUES) {
      return;
    }
    RepeatedListWriter impl = (RepeatedListWriter) writer.list(name);
    impl.container.copyFromSafe(idx(), impl.idx(), container);
  }

  private int currentOffset;
  private int maxOffset;

  @Override
  public void reset() {
    super.reset();
    currentOffset = 0;
    maxOffset = 0;
    if (reader != null) {
      reader.reset();
    }
    reader = null;
  }

  @Override
  public int size() {
    return maxOffset - currentOffset;
  }

  @Override
  public void setPosition(int index) {
    if (index < 0 || index == NO_VALUES) {
      currentOffset = NO_VALUES;
      return;
    }

    super.setPosition(index);
    RepeatedListHolder h = new RepeatedListHolder();
    container.getAccessor().get(index, h);
    if (h.start == h.end) {
      currentOffset = NO_VALUES;
    } else {
      currentOffset = h.start-1;
      maxOffset = h.end;
      if(reader != null) {
        reader.setPosition(currentOffset);
      }
    }
  }

  @Override
  public boolean next() {
    if (currentOffset +1 < maxOffset) {
      currentOffset++;
      if (reader != null) {
        reader.setPosition(currentOffset);
      }
      return true;
    } else {
      currentOffset = NO_VALUES;
      return false;
    }
  }

  @Override
  public Object readObject() {
    return container.getAccessor().getObject(idx());
  }

  @Override
  public FieldReader reader() {
    if (reader == null) {
      ValueVector child = container.getChild(name);
      if (child == null) {
        reader = NullReader.INSTANCE;
      } else {
        reader = child.getReader();
      }
      reader.setPosition(currentOffset);
    }
    return reader;
  }

  public boolean isSet() {
    return true;
  }

}
