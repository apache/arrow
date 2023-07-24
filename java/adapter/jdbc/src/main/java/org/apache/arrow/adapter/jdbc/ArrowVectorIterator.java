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
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
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

  // this is used only if resuing vector schema root is enabled.
  private VectorSchemaRoot nextBatch;

  private final int targetBatchSize;

  // This is used to track whether the ResultSet has been fully read, and is needed spcifically for cases where there
  // is a ResultSet having zero rows (empty):
  private boolean readComplete = false;

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
    this.nextBatch = config.isReuseVectorSchemaRoot() ? createVectorSchemaRoot() : null;
  }

  /**
   * Create a ArrowVectorIterator to partially convert data.
   */
  public static ArrowVectorIterator create(
      ResultSet resultSet,
      JdbcToArrowConfig config)
      throws SQLException {
    ArrowVectorIterator iterator = null;
    try {
      iterator = new ArrowVectorIterator(resultSet, config);
    } catch (Throwable e) {
      AutoCloseables.close(e, iterator);
      throw new RuntimeException("Error occurred while creating iterator.", e);
    }
    return iterator;
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
        readComplete = true;
      } else {
        while ((readRowCount < targetBatchSize) && !readComplete) {
          if (resultSet.next()) {
            compositeConsumer.consume(resultSet);
            readRowCount++;
          } else {
            readComplete = true;
          }
        }
      }

      root.setRowCount(readRowCount);
    } catch (Throwable e) {
      compositeConsumer.close();
      throw new RuntimeException("Error occurred while consuming data.", e);
    }
  }

  private VectorSchemaRoot createVectorSchemaRoot() throws SQLException {
    VectorSchemaRoot root = null;
    try {
      root = VectorSchemaRoot.create(schema, config.getAllocator());
      if (config.getTargetBatchSize() != JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE) {
        ValueVectorUtility.preAllocate(root, config.getTargetBatchSize());
      }
    } catch (Throwable e) {
      if (root != null) {
        root.close();
      }
      throw new RuntimeException("Error occurred while creating schema root.", e);
    }
    initialize(root);
    return root;
  }

  private void initialize(VectorSchemaRoot root) throws SQLException {
    for (int i = 1; i <= consumers.length; i++) {
      final JdbcFieldInfo columnFieldInfo = JdbcToArrowUtils.getJdbcFieldInfoForColumn(rsmd, i, config);
      ArrowType arrowType = config.getJdbcToArrowTypeConverter().apply(columnFieldInfo);
      consumers[i - 1] = JdbcToArrowUtils.getConsumer(
          arrowType, i, isColumnNullable(resultSet.getMetaData(), i, columnFieldInfo), root.getVector(i - 1), config);
    }
  }

  // Loads the next schema root or null if no more rows are available.
  private void load(VectorSchemaRoot root) {
    for (int i = 0; i < consumers.length; i++) {
      FieldVector vec = root.getVector(i);
      if (config.isReuseVectorSchemaRoot()) {
        // if we are reusing the vector schema root,
        // we must reset the vector before populating it with data.
        vec.reset();
      }
      consumers[i].resetValueVector(vec);
    }

    consumeData(root);
  }

  @Override
  public boolean hasNext() {
    return !readComplete;
  }

  /**
   * Gets the next vector.
   * If {@link JdbcToArrowConfig#isReuseVectorSchemaRoot()} is false,
   * the client is responsible for freeing its resources.
   */
  @Override
  public VectorSchemaRoot next() {
    Preconditions.checkArgument(hasNext());
    try {
      VectorSchemaRoot ret = config.isReuseVectorSchemaRoot() ? nextBatch : createVectorSchemaRoot();
      load(ret);
      return ret;
    } catch (Exception e) {
      close();
      throw new RuntimeException("Error occurred while getting next schema root.", e);
    }
  }

  /**
   * Clean up resources ONLY WHEN THE {@link VectorSchemaRoot} HOLDING EACH BATCH IS REUSED. If a new VectorSchemaRoot
   * is created for each batch, each root must be closed manually by the client code.
   */
  @Override
  public void close() {
    if (config.isReuseVectorSchemaRoot()) {
      nextBatch.close();
      compositeConsumer.close();
    }
  }
}
