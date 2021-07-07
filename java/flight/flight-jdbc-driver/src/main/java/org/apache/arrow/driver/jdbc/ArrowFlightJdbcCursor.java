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



import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcBaseIntVectorAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.numeric.ArrowFlightJdbcFloatingPointVectorAccessor;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
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

    final List<Accessor> accessors = new ArrayList<>();
    for (int i = 0; i < fieldVectors.size(); i++) {
      FieldVector vector = root.getVector(i);
      accessors.add(createAccessor(vector));
    }

    return accessors;
  }

  private Accessor createAccessor(FieldVector vector) {
    if (vector instanceof UInt1Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt1Vector) vector, this::getCurrentRow);
    } else if (vector instanceof UInt2Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt2Vector) vector, this::getCurrentRow);
    } else if (vector instanceof UInt4Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt4Vector) vector, this::getCurrentRow);
    } else if (vector instanceof UInt8Vector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((UInt8Vector) vector, this::getCurrentRow);
    } else if (vector instanceof TinyIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((TinyIntVector) vector, this::getCurrentRow);
    } else if (vector instanceof SmallIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((SmallIntVector) vector, this::getCurrentRow);
    } else if (vector instanceof IntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((IntVector) vector, this::getCurrentRow);
    } else if (vector instanceof BigIntVector) {
      return new ArrowFlightJdbcBaseIntVectorAccessor((BigIntVector) vector, this::getCurrentRow);
    } else if (vector instanceof Float4Vector) {
      return new ArrowFlightJdbcFloatingPointVectorAccessor((Float4Vector) vector, this::getCurrentRow);
    } else if (vector instanceof Float8Vector) {
      return new ArrowFlightJdbcFloatingPointVectorAccessor((Float8Vector) vector, this::getCurrentRow);
    }

    throw new UnsupportedOperationException();
  }

  @Override
  protected Getter createGetter(int column) {
    return new AbstractGetter() {
      @Override
      public Object getObject() throws SQLException {
        throw new UnsupportedOperationException();
      }
    };
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

  public int getCurrentRow() {
    return currentRow;
  }
}
