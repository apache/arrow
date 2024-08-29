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

package org.apache.arrow.vector.types.pojo;

import static org.apache.arrow.vector.types.pojo.Schema.METADATA_KEY;
import static org.apache.arrow.vector.types.pojo.Schema.METADATA_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.junit.Test;

public class TestField {

  private static Field field(String name, boolean nullable, ArrowType type, Map<String, String> metadata) {
    return new Field(name, new FieldType(nullable, type, null, metadata), Collections.emptyList());
  }

  @Test
  public void testMetadata() throws IOException {
    Map<String, String> metadata = new HashMap<>(1);
    metadata.put("testKey", "testValue");

    Schema schema = new Schema(Collections.singletonList(
        field("a", false, new Int(8, true), metadata)
    ));

    String json = schema.toJson();
    Schema actual = Schema.fromJSON(json);

    jsonContains(json, "\"" + METADATA_KEY + "\" : \"testKey\"", "\"" + METADATA_VALUE + "\" : \"testValue\"");

    Map<String, String> actualMetadata = actual.getFields().get(0).getMetadata();
    assertEquals(1, actualMetadata.size());
    assertEquals("testValue", actualMetadata.get("testKey"));
  }

  private void jsonContains(String json, String... strings) {
    for (String string : strings) {
      assertTrue(json + " contains " + string, json.contains(string));
    }
  }
}
