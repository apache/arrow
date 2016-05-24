/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.types.pojo;


import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static org.apache.arrow.vector.types.pojo.ArrowType.getTypeForField;
import static org.apache.arrow.vector.types.pojo.Field.convertField;

public class Schema {
  private List<Field> fields;

  public Schema(List<Field> fields) {
    this.fields = ImmutableList.copyOf(fields);
  }

  public int getSchema(FlatBufferBuilder builder) {
    int[] fieldOffsets = new int[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      fieldOffsets[i] = fields.get(i).getField(builder);
    }
    int fieldsOffset = org.apache.arrow.flatbuf.Schema.createFieldsVector(builder, fieldOffsets);
    org.apache.arrow.flatbuf.Schema.startSchema(builder);
    org.apache.arrow.flatbuf.Schema.addFields(builder, fieldsOffset);
    return org.apache.arrow.flatbuf.Schema.endSchema(builder);
  }

  public List<Field> getFields() {
    return fields;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fields);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Schema)) {
      return false;
    }
    return Objects.equals(this.fields, ((Schema) obj).fields);
  }

  public static Schema convertSchema(org.apache.arrow.flatbuf.Schema schema) {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    for (int i = 0; i < schema.fieldsLength(); i++) {
      childrenBuilder.add(convertField(schema.fields(i)));
    }
    List<Field> fields = childrenBuilder.build();
    return new Schema(fields);
  }
}
