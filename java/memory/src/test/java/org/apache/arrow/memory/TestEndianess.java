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

package org.apache.arrow.memory;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.netty.buffer.ByteBuf;

public class TestEndianess {

  @Test
  public void testLittleEndian() {
    final BufferAllocator a = new RootAllocator(10000);
    final ByteBuf b = a.buffer(4);
    b.setInt(0, 35);
    assertEquals(b.getByte(0), 35);
    assertEquals(b.getByte(1), 0);
    assertEquals(b.getByte(2), 0);
    assertEquals(b.getByte(3), 0);
    b.release();
    a.close();
  }

}
