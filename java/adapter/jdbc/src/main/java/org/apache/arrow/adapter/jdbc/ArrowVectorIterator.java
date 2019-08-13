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

package org.apache.arrow.adapter.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * VectorSchemaRoot iterator for partially converting JDBC data.
 */
public class ArrowVectorIterator {

  private final ResultSet resultSet;
  private final JdbcToArrowConfig config;

  private final Schema schema;

  private final int partialLimit;

  /**
   * The last vector value count.
   */
  private int preCount;

  /**
   * Construct an instance.
   */
  public ArrowVectorIterator(ResultSet resultSet, JdbcToArrowConfig config) throws SQLException {
    this.resultSet = resultSet;
    this.config = config;
    this.partialLimit = config.getPartialLimit();
    this.schema = JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), config);
  }

  /**
   * Whether the {@link ResultSet} has data to read into a new vector.
   */
  public boolean hasNext() throws SQLException {

    // ResultSet does not has API like hasNext, if use next() and previous() to check follow data, user must set
    // ResultSet.TYPE_SCROLL_INSENSITIVE otherwise, previous() will throw exception.
    // So we use preCount and partialLimit to check follow data which is a little hack.
    if (preCount == 0 && resultSet.isBeforeFirst() != resultSet.isAfterLast()) {
      return true;
    } else if (preCount < partialLimit) {
      return false;
    } else if (preCount == partialLimit && resultSet.isLast()) {
      return false;
    }
    return true;
  }

  /**
   * Get the next converted vector.
   */
  public VectorSchemaRoot next() throws SQLException, IOException {
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, config.getAllocator());
    JdbcToArrowUtils.jdbcToArrowVectors(resultSet, root, config);
    preCount = root.getRowCount();
    return root;
  }
}
