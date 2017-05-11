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
package org.apache.arrow.vector.file;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.google.flatbuffers.FlatBufferBuilder;

public class TestArrowFooter {

  @Test
  public void test() {
    Schema schema = new Schema(asList(
        new Field("a", FieldType.nullable(new ArrowType.Int(8, true)), Collections.<Field>emptyList())
        ));
    ArrowFooter footer = new ArrowFooter(schema, Collections.<ArrowBlock>emptyList(), Collections.<ArrowBlock>emptyList());
    ArrowFooter newFooter = roundTrip(footer);
    assertEquals(footer, newFooter);

    List<ArrowBlock> ids = new ArrayList<>();
    ids.add(new ArrowBlock(0, 1, 2));
    ids.add(new ArrowBlock(4, 5, 6));
    footer = new ArrowFooter(schema, ids, ids);
    assertEquals(footer, roundTrip(footer));
  }


  private ArrowFooter roundTrip(ArrowFooter footer) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int i = footer.writeTo(builder);
    builder.finish(i);
    ByteBuffer dataBuffer = builder.dataBuffer();
    ArrowFooter newFooter = new ArrowFooter(Footer.getRootAsFooter(dataBuffer));
    return newFooter;
  }

}
