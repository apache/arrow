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

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.complex.impl.Float4WriterImpl;
import org.apache.arrow.vector.complex.writer.Float4Writer;

/**
 * Consumer which consume float type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.Float4Vector}.
 */
public class FloatConsumer implements JdbcConsumer<Float4Vector> {

  private Float4Writer writer;
  private final int columnIndexInResultSet;

  /**
   * Instantiate a FloatConsumer.
   */
  public FloatConsumer(Float4Vector vector, int index) {
    this.writer = new Float4WriterImpl(vector);
    this.columnIndexInResultSet = index;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    float value = resultSet.getFloat(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      writer.writeFloat4(value);
    }
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void resetValueVector(Float4Vector vector) {
    this.writer = new Float4WriterImpl(vector);
  }
}
