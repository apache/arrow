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

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.complex.impl.BitWriterImpl;
import org.apache.arrow.vector.complex.writer.BitWriter;

/**
 * Consumer which consume bit type values from {@link ResultSet}.
 * Write the data to {@link BitVector}.
 */
public class BitConsumer implements JdbcConsumer {

  private final BitWriter writer;
  private final int columnIndexInResultSet;

  /**
   * Instantiate a BitConsumer.
   */
  public BitConsumer(BitVector vector, int index) {
    this.writer = new BitWriterImpl(vector);
    this.columnIndexInResultSet = index;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    boolean value = resultSet.getBoolean(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      writer.writeBit(value ? 1 : 0);
    }
    writer.setPosition(writer.getPosition() + 1);
  }
}
