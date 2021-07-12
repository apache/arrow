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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowUtils.isColumnNullable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.arrow.adapter.jdbc.consumer.CompositeJdbcConsumer;
import org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * VectorSchemaRoot iterator for partially converting JDBC data.
 */
public class ArrowVectorIterator implements Iterator<VectorSchemaRoot>, AutoCloseable {

  private final ResultSet resultSet;
  private final JdbcToArrowConfig config;

  private final Schema schema;
  private final ResultSetMetaData rsmd;

  private final JdbcConsumer[] consumers;
  final CompositeJdbcConsumer compositeConsumer;

  private VectorSchemaRoot nextBatch;

  private final int targetBatchSize;

  /**
   * Construct an instance.
   */
  private ArrowVectorIterator(ResultSet resultSet, JdbcToArrowConfig config) throws SQLException {
    this.resultSet = resultSet;
    this.config = config;
    this.schema = JdbcToArrowUtils.jdbcToArrowSchema(resultSet.getMetaData(), config);
    this.targetBatchSize = config.getTargetBatchSize();

    rsmd = resultSet.getMetaData();
    consumers = new JdbcConsumer[rsmd.getColumnCount()];
    this.compositeConsumer = new CompositeJdbcConsumer(consumers);
  }

  private void initialize() throws SQLException {
    // create consumers
    for (int i = 1; i <= consumers.length; i++) {
      ArrowType arrowType = config.getJdbcToArrowTypeConverter()
          .apply(new JdbcFieldInfo(resultSet.getMetaData(), i));
      consumers[i - 1] = JdbcToArrowUtils.getConsumer(
          arrowType, i, isColumnNullable(resultSet, i), null, config);
    }

    load(createVectorSchemaRoot());
  }

  /**
   * Create a ArrowVectorIterator to partially convert data.
   */
  public static ArrowVectorIterator create(
      ResultSet resultSet,
      JdbcToArrowConfig config)
      throws SQLException {

    ArrowVectorIterator iterator = new ArrowVectorIterator(resultSet, config);
    try {
      iterator.initialize();
      return iterator;
    } catch (Exception e) {
      iterator.close();
      throw new RuntimeException("Error occurred while creating iterator.", e);
    }
  }

  private void consumeData(VectorSchemaRoot root) {
    // consume data
    try {
      int readRowCount = 0;
      if (targetBatchSize == JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE) {
        while (resultSet.next()) {
          ValueVectorUtility.ensureCapacity(root, readRowCount + 1);
          compositeConsumer.consume(resultSet);
          readRowCount++;
        }
      } else {
        while (readRowCount < targetBatchSize && resultSet.next()) {
          compositeConsumer.consume(resultSet);
          readRowCount++;
        }
      }


      root.setRowCount(readRowCount);
    } catch (Exception e) {
      compositeConsumer.close();
      throw new RuntimeException("Error occurred while consuming data.", e);
    }
  }

  private VectorSchemaRoot createVectorSchemaRoot() {
    VectorSchemaRoot root = null;
    try {
      root = VectorSchemaRoot.create(schema, config.getAllocator());
      if (config.getTargetBatchSize() != JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE) {
        ValueVectorUtility.preAllocate(root, config.getTargetBatchSize());
      }
    } catch (Exception e) {
      if (root != null) {
        root.close();
      }
      throw new RuntimeException("Error occurred while creating schema root.", e);
    }
    return root;
  }

  // Loads the next schema root or null if no more rows are available.
  private void load(VectorSchemaRoot root) throws SQLException {

    for (int i = 1; i <= consumers.length; i++) {
      consumers[i - 1].resetValueVector(root.getVector(rsmd.getColumnLabel(i)));
    }

    consumeData(root);

    if (root.getRowCount() == 0) {
      root.close();
      nextBatch = null;
    } else {
      nextBatch = root;
    }
  }

  @Override
  public boolean hasNext() {
    return nextBatch != null;
  }

  /**
   * Gets the next vector. The user is responsible for freeing its resources.
   */
  @Override
  public VectorSchemaRoot next() {
    Preconditions.checkArgument(hasNext());
    VectorSchemaRoot returned = nextBatch;
    try {
      load(createVectorSchemaRoot());
    } catch (Exception e) {
      close();
      throw new RuntimeException("Error occurred while getting next schema root.", e);
    }
    return returned;
  }

  /**
   * Clean up resources.
   */
  @Override
  public void close() {
    if (nextBatch != null) {
      nextBatch.close();
    }
    compositeConsumer.close();
  }
}
