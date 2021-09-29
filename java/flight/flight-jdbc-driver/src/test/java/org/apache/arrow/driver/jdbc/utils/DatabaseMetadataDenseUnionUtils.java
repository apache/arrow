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

package org.apache.arrow.driver.jdbc.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.apache.arrow.driver.jdbc.ArrowDatabaseMetadata;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

/**
 * Utility class for testing {@link ArrowDatabaseMetadata} as well as its interactions with
 * {@link SqlInfo} and {@link DenseUnionVector} instances.
 */
public final class DatabaseMetadataDenseUnionUtils {

  private DatabaseMetadataDenseUnionUtils() {
    // Prevent instantiation.
  }

  /**
   * Sets the "info_name" field of the provided {@code root} as described in the FlightSQL specification.
   *
   * @param root  the {@link VectorSchemaRoot} from which to fetch the {@link UInt4Vector}.
   * @param index the index to {@link UInt4Vector#setSafe}.
   * @param info  the {@link SqlInfo} from which to get the {@link SqlInfo#getNumber}.
   */
  public static void setInfoName(final VectorSchemaRoot root, final int index, final SqlInfo info) {
    final UInt4Vector infoName = (UInt4Vector) root.getVector("info_name");
    infoName.setSafe(index, info.getNumber());
  }

  /**
   * Sets the "value" field of the provide {@code root} as described in the FlightSQL specification.
   *
   * @param root       the {@link VectorSchemaRoot} from which to fetch the {@link DenseUnionVector}.
   * @param index      the index to {@link DenseUnionVector#setSafe}.
   * @param typeId     the {@link DenseUnionVector#registerNewTypeId} output for the given type to be registered.
   * @param dataSetter the {@link Consumer}&lt;{@link DenseUnionVector}&gt; that should decide
   *                   which {@link DenseUnionVector#setSafe} to use.
   */
  public static void setValues(final VectorSchemaRoot root, final int index, final byte typeId,
                               final Consumer<DenseUnionVector> dataSetter) {
    final DenseUnionVector values = (DenseUnionVector) root.getVector("value");
    values.setTypeId(index, typeId);
    dataSetter.accept(values);
  }

  /**
   * Gets a {@link NullableVarCharHolder} from the provided {@code string} using the provided {@code buf}.
   *
   * @param string the {@link StandardCharsets#UTF_8}-encoded text input to store onto the holder.
   * @param buf    the {@link ArrowBuf} from which to create the new holder.
   * @return a new {@link NullableVarCharHolder} with the provided input data {@code string}.
   */
  public static NullableVarCharHolder getHolderForUtf8(final String string, final ArrowBuf buf) {
    final byte[] bytes = string.getBytes(UTF_8);
    buf.setBytes(0, bytes);
    final NullableVarCharHolder holder = new NullableVarCharHolder();
    holder.buffer = buf;
    holder.end = bytes.length;
    holder.isSet = 1;
    return holder;
  }

  /**
   * Executes the given action on an ad-hoc, newly created instance of {@link ArrowBuf}.
   *
   * @param executor the action to take.
   */
  public static void onCreateArrowBuf(final Consumer<ArrowBuf> executor) {
    try (final BufferAllocator allocator = new RootAllocator();
         final ArrowBuf buf = allocator.buffer(1024)) {
      executor.accept(buf);
    }
  }

  /**
   * Sets the data {@code value} for a {@link StandardCharsets#UTF_8}-encoded field.
   *
   * @param root    the {@link VectorSchemaRoot} from which to fetch the {@link DenseUnionVector}.
   * @param index   the index to use for {@link DenseUnionVector#setSafe}
   * @param sqlInfo the {@link SqlInfo} to use.
   * @param value   the input value.
   */
  public static void setDataForUtf8Field(final VectorSchemaRoot root, final int index,
                                         final SqlInfo sqlInfo, final String value) {
    setInfoName(root, index, sqlInfo);
    onCreateArrowBuf(buf -> {
      final Consumer<DenseUnionVector> producer =
          values -> values.setSafe(index, getHolderForUtf8(value, buf));
      setValues(root, index, (byte) 0, producer);
    });
  }
}
