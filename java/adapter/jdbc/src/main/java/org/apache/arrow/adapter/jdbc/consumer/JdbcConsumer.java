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
 * An abstraction that is used to consume values from {@link ResultSet}.
 * @param <T> The vector within consumer or its delegate, used for partially consume purpose.
 */
public interface JdbcConsumer<T extends ValueVector> extends AutoCloseable {

  /**
   * Consume a specific type value from {@link ResultSet} and write it to vector.
   */
  void consume(ResultSet resultSet) throws SQLException, IOException;

  /**
   * Close this consumer, do some clean work such as clear reuse ArrowBuf.
   */
  @Override
  void close() throws Exception;

  /**
   * Reset the vector within consumer for partial read purpose.
   */
  void resetValueVector(T vector);
}
