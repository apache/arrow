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

package org.apache.arrow.vector;

import org.apache.arrow.vector.compare.VectorVisitor;

/**
 * A zero length vector of any type.
 */
public final class ZeroVector extends NullVector {
  public static final ZeroVector INSTANCE = new ZeroVector();

  public ZeroVector() {
  }

  @Override
  public int getValueCount() {
    return 0;
  }

  @Override
  public void setValueCount(int valueCount) {
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public int hashCode(int index) {
    return 0;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }

  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return getField().getName();
  }
}
