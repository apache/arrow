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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.complex.impl.BigIntWriterImpl;
import org.apache.arrow.vector.complex.writer.BigIntWriter;

/**
 * Consumer which consume bigint type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.BigIntVector}.
 */
public class BigIntConsumer implements JdbcConsumer {

  private final BigIntWriter writer;
  private final int columnIndexInResultSet;

  /**
   * Instantiate a BigIntConsumer.
   */
  public BigIntConsumer(BigIntVector vector, int index) {
    this.writer = new BigIntWriterImpl(vector);
    this.columnIndexInResultSet = index;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    long value = resultSet.getLong(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      writer.writeBigInt(value);
    }
    writer.setPosition(writer.getPosition() + 1);
  }
}
