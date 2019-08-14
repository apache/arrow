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

import org.apache.arrow.vector.ValueVector;

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
    for (JdbcConsumer consumer : consumers) {
      consumer.consume(rs);
    }
  }

  @Override
  public void close() {
    // clean up
    for (JdbcConsumer consumer : consumers) {
      consumer.close();
    }
  }

  @Override
  public void resetValueVector(ValueVector vector) {

  }
}
