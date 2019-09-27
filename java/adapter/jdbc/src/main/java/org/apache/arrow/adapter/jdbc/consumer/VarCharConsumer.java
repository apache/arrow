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

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.VarCharVector;

/**
 * Consumer which consume varchar type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarCharVector}.
 */
public class VarCharConsumer extends BaseJdbcConsumer<VarCharVector> {

  /**
   * Instantiate a VarCharConsumer.
   */
  public VarCharConsumer(VarCharVector vector, int index, boolean nullable) {
    super(vector, index, nullable);
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    String value = resultSet.getString(columnIndexInResultSet);
    if (!nullable || !resultSet.wasNull()) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

      vector.setSafe(currentIndex, bytes);
    }
    currentIndex++;
  }
}
