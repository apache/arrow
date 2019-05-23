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

import java.util.Iterator;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.UnionHolder;

/**
 * Base class providing common functionality for {@link FieldReader} implementations.
 *
 * <p>This includes tracking the current index and throwing implementations of optional methods.
 */
abstract class AbstractBaseReader implements FieldReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBaseReader.class);

  private int index;

  public AbstractBaseReader() {
    super();
  }

  @Override
  public int getPosition() {
    return index;
  }

  public void setPosition(int index) {
    this.index = index;
  }

  protected int idx() {
    return index;
  }

  @Override
  public void reset() {
    index = 0;
  }

  @Override
  public Iterator<String> iterator() {
    throw new IllegalStateException("The current reader doesn't support reading as a map.");
  }

  @Override
  public boolean next() {
    throw new IllegalStateException("The current reader doesn't support getting next information.");
  }

  @Override
  public int size() {
    throw new IllegalStateException("The current reader doesn't support getting size information.");
  }

  @Override
  public void read(UnionHolder holder) {
    holder.reader = this;
    holder.isSet = this.isSet() ? 1 : 0;
  }

  @Override
  public void read(int index, UnionHolder holder) {
    throw new IllegalStateException("The current reader doesn't support reading union type");
  }

  @Override
  public void copyAsValue(UnionWriter writer) {
    throw new IllegalStateException("The current reader doesn't support reading union type");
  }

  @Override
  public void copyAsValue(ListWriter writer) {
    ComplexCopier.copy(this, (FieldWriter) writer);
  }
}
