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
 * Base class for all jdbc consumers.
 * @param <T> vector type.
 */
public abstract class BaseJdbcConsumer<T extends ValueVector> implements JdbcConsumer<T> {

  protected final int columnIndexInResultSet;
  protected T vector;
  protected int currentIndex;
  protected final boolean nullable;

  /**
   * Constructs a base jdbc consumer.
   * @param vector the vector to consume.
   * @param index the column index of the vector.
   * @param nullable if the column if nullable.
   */
  public BaseJdbcConsumer(T vector, int index, boolean nullable) {
    this.columnIndexInResultSet = index;
    this.vector = vector;
    this.nullable = nullable;
  }

  @Override
  public void resetValueVector(T vector) {
    this.vector = vector;
    this.currentIndex = 0;
  }

  @Override
  public void close() throws Exception {
    this.vector.close();
  }
}
