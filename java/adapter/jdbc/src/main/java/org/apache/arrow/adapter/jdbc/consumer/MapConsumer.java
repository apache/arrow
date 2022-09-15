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
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.util.ObjectMapperFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Consumer which consume map type values from {@link ResultSet}.
 * Write the data into {@link org.apache.arrow.vector.complex.MapVector}.
 */
public class MapConsumer extends BaseConsumer<MapVector> {


  private final UnionMapWriter writer;
  private final ObjectMapper objectMapper = ObjectMapperFactory.newObjectMapper();
  private final TypeReference<Map<String, String>> typeReference = new TypeReference<Map<String, String>>() {};
  private int currentRow;

  /**
   * Creates a consumer for {@link MapVector}.
   */
  public static MapConsumer createConsumer(MapVector mapVector, int index, boolean nullable) {
    return new MapConsumer(mapVector, index);
  }

  /**
   * Instantiate a MapConsumer.
   */
  public MapConsumer(MapVector vector, int index) {
    super(vector, index);
    writer = vector.getWriter();
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException, IOException {
    Object map = resultSet.getObject(columnIndexInResultSet);
    writer.setPosition(currentRow++);
    if (map != null) {
      if (map instanceof String) {
        writeJavaMapIntoVector(objectMapper.readValue((String) map, typeReference));
      } else if (map instanceof Map) {
        writeJavaMapIntoVector((Map<String, String>) map);
      } else {
        throw new IllegalArgumentException("Unknown type of map type column from JDBC " + map.getClass().getName());
      }
    } else {
      writer.writeNull();
    }
  }

  private void writeJavaMapIntoVector(Map<String, String> map) {
    BufferAllocator allocator = vector.getAllocator();
    writer.startMap();
    map.forEach((key, value) -> {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
      try (
              ArrowBuf keyBuf = allocator.buffer(keyBytes.length);
              ArrowBuf valueBuf = valueBytes != null ? allocator.buffer(valueBytes.length) : null;
      ) {
        writer.startEntry();
        keyBuf.writeBytes(keyBytes);
        writer.key().varChar().writeVarChar(0, keyBytes.length, keyBuf);
        if (valueBytes != null) {
          valueBuf.writeBytes(valueBytes);
          writer.value().varChar().writeVarChar(0, valueBytes.length, valueBuf);
        } else {
          writer.value().varChar().writeNull();
        }
        writer.endEntry();
      }
    });
    writer.endMap();
  }
}

