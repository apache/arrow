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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.consumer.exceptions.JdbcConsumerException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Composite consumer which hold all consumers.
 * It manages the consume and cleanup process.
 */
public class CompositeJdbcConsumer implements JdbcConsumer {

  private final JdbcConsumer[] consumers;

  /**
   * Construct an instance.
   */
  public CompositeJdbcConsumer(JdbcConsumer[] consumers) {
    this.consumers = consumers;
  }

  @Override
  public void consume(ResultSet rs) throws SQLException, IOException {
    for (int i = 0; i < consumers.length; i++) {
      try {
        consumers[i].consume(rs);
      } catch (Exception e) {
        if (consumers[i] instanceof BaseConsumer) {
          BaseConsumer consumer = (BaseConsumer) consumers[i];
          JdbcFieldInfo fieldInfo = new JdbcFieldInfo(rs.getMetaData(), consumer.columnIndexInResultSet);
          ArrowType arrowType = consumer.vector.getMinorType().getType();
          throw new JdbcConsumerException("Exception while consuming JDBC value", e, fieldInfo, arrowType);
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public void close() {

    try {
      // clean up
      AutoCloseables.close(consumers);
    } catch (Exception e) {
      throw new RuntimeException("Error occurred while releasing resources.", e);
    }

  }

  @Override
  public void resetValueVector(ValueVector vector) {

  }

  /**
   * Reset inner consumers through vectors in the vector schema root.
   */
  public void resetVectorSchemaRoot(VectorSchemaRoot root) {
    assert root.getFieldVectors().size() == consumers.length;
    for (int i = 0; i < consumers.length; i++) {
      consumers[i].resetValueVector(root.getVector(i));
    }
  }
}

