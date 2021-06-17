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
import java.util.List;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Arrow Flight Jdbc's Cursor class.
 */
public class ArrowFlightJdbcCursor extends AbstractCursor {

  private static final Logger LOGGER;
  private final List<FieldVector> fieldVectorList;
  private final int rowCount;
  private int currentRow = -1;

  static {
    LOGGER = LoggerFactory.getLogger(ArrowFlightJdbcCursor.class);
  }

  public ArrowFlightJdbcCursor(VectorSchemaRoot root) {
    fieldVectorList = root.getFieldVectors();
    rowCount = root.getRowCount();
  }

  @Override
  protected Getter createGetter(int column) {
    return new AbstractGetter() {
      @Override
      public Object getObject() throws SQLException {
        return fieldVectorList.get(column).getObject(currentRow);
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
      AutoCloseables.close(fieldVectorList);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }
}
