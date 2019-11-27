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

package org.apache.arrow.adapter.jdbc.consumer;

import org.apache.arrow.vector.ValueVector;

/**
 * Base class for all consumers.
 * @param <V> vector type.
 */
public abstract class BaseConsumer<V extends ValueVector> implements JdbcConsumer<V> {

  protected V vector;

  protected final int columnIndexInResultSet;

  protected int currentIndex;

  /**
   * Constructs a new consumer.
   * @param vector the underlying vector for the consumer.
   * @param index the column id for the consumer.
   */
  public BaseConsumer(V vector, int index) {
    this.vector = vector;
    this.columnIndexInResultSet = index;
  }

  @Override
  public void close() throws Exception {
    this.vector.close();
  }

  @Override
  public void resetValueVector(V vector) {
    this.vector = vector;
    this.currentIndex = 0;
  }
}
