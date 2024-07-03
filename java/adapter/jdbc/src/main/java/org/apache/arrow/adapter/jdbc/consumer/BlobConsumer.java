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
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.arrow.vector.VarBinaryVector;

/**
 * Consumer which consume blob type values from {@link ResultSet}. Write the data to {@link
 * VarBinaryVector}.
 */
public class BlobConsumer extends BaseConsumer<VarBinaryVector> {

  private BinaryConsumer delegate;

  private final boolean nullable;

  /** Creates a consumer for {@link VarBinaryVector}. */
  public static BlobConsumer createConsumer(BinaryConsumer delegate, int index, boolean nullable) {
    return new BlobConsumer(delegate, index, nullable);
  }

  /** Instantiate a BlobConsumer. */
  public BlobConsumer(BinaryConsumer delegate, int index, boolean nullable) {
    super(null, index);
    this.delegate = delegate;
    this.nullable = nullable;
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException, IOException {
    Blob blob = resultSet.getBlob(columnIndexInResultSet);
    if (blob != null) {
      delegate.consume(blob.getBinaryStream());
    }
    delegate.moveWriterPosition();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public void resetValueVector(VarBinaryVector vector) {
    delegate = BinaryConsumer.createConsumer(vector, columnIndexInResultSet, nullable);
  }
}
