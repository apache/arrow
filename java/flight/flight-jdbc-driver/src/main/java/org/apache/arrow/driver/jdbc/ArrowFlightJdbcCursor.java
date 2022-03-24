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

package org.apache.arrow.driver.jdbc;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Arrow Flight Jdbc's Cursor class.
 */
public class ArrowFlightJdbcCursor extends AbstractCursor {

  private static final Logger LOGGER;
  private final VectorSchemaRoot root;
  private final int rowCount;
  private int currentRow = -1;

  static {
    LOGGER = LoggerFactory.getLogger(ArrowFlightJdbcCursor.class);
  }

  public ArrowFlightJdbcCursor(VectorSchemaRoot root) {
    this.root = root;
    rowCount = root.getRowCount();
  }

  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> columns,
                                        Calendar localCalendar,
                                        ArrayImpl.Factory factory) {
    final List<FieldVector> fieldVectors = root.getFieldVectors();

    return IntStream.range(0, fieldVectors.size()).mapToObj(root::getVector)
        .map(this::createAccessor)
        .collect(Collectors.toCollection(() -> new ArrayList<>(fieldVectors.size())));
  }

  private Accessor createAccessor(FieldVector vector) {
    return ArrowFlightJdbcAccessorFactory.createAccessor(vector, this::getCurrentRow,
        (boolean wasNull) -> {
          // AbstractCursor creates a boolean array of length 1 to hold the wasNull value
          this.wasNull[0] = wasNull;
        });
  }

  /**
   * ArrowFlightJdbcAccessors do not use {@link AbstractCursor.Getter}, as it would box primitive types and cause
   * performance issues. Each Accessor implementation works directly on Arrow Vectors.
   */
  @Override
  protected Getter createGetter(int column) {
    throw new UnsupportedOperationException("Not allowed.");
  }

  @Override
  public boolean next() {
    currentRow++;
    return currentRow < rowCount;
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(root);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private int getCurrentRow() {
    return currentRow;
  }
}
