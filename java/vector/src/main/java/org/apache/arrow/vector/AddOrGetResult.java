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

import org.apache.arrow.util.Preconditions;

/**
 * Tuple class containing a vector and whether it was created.
 *
 * @param <V> The type of vector the result is for.
 */
public class AddOrGetResult<V extends ValueVector> {
  private final V vector;
  private final boolean created;

  /** Constructs a new object. */
  public AddOrGetResult(V vector, boolean created) {
    this.vector = Preconditions.checkNotNull(vector);
    this.created = created;
  }

  /** Returns the vector. */
  public V getVector() {
    return vector;
  }

  /** Returns whether the vector is created. */
  public boolean isCreated() {
    return created;
  }
}
