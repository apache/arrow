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

import org.apache.arrow.vector.complex.StructVector;

/**
 * A factory for {@link NullableStructWriter} instances. The factory allows for configuring if field
 * names should be considered case-sensitive.
 */
public class NullableStructWriterFactory {
  private final boolean caseSensitive;
  private static final NullableStructWriterFactory nullableStructWriterFactory =
      new NullableStructWriterFactory(false);
  private static final NullableStructWriterFactory nullableCaseSensitiveWriterFactory =
      new NullableStructWriterFactory(true);

  public NullableStructWriterFactory(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  /** Creates a new instance. */
  public NullableStructWriter build(StructVector container) {
    return this.caseSensitive
        ? new NullableCaseSensitiveStructWriter(container)
        : new NullableStructWriter(container);
  }

  public static NullableStructWriterFactory getNullableStructWriterFactoryInstance() {
    return nullableStructWriterFactory;
  }

  public static NullableStructWriterFactory getNullableCaseSensitiveStructWriterFactoryInstance() {
    return nullableCaseSensitiveWriterFactory;
  }
}
