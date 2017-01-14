/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.stream;

import org.apache.arrow.flatbuf.StreamHeader;
import org.apache.arrow.vector.schema.FBSerializable;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.flatbuffers.FlatBufferBuilder;

public class ArrowStreamHeader implements FBSerializable {
  private final Schema schema;
  private final int totalBatches;

  public ArrowStreamHeader(Schema schema, int totalBatches) {
    this.schema = schema;
    this.totalBatches = totalBatches;
  }

  public ArrowStreamHeader(Schema schema) {
    this(schema, -1);
  }

  public ArrowStreamHeader(StreamHeader header) {
    this(Schema.convertSchema(header.schema()), header.totalBatches());
  }

  public Schema getSchema() { return schema; }
  public int getTotalBatches() { return totalBatches; }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    int schemaIndex = schema.getSchema(builder);
    StreamHeader.startStreamHeader(builder);
    StreamHeader.addSchema(builder, schemaIndex);
    StreamHeader.addTotalBatches(builder, getTotalBatches());
    return StreamHeader.endStreamHeader(builder);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    result = prime * result + getTotalBatches();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ArrowStreamHeader other = (ArrowStreamHeader) obj;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    return totalBatches == other.totalBatches;
  }
}
