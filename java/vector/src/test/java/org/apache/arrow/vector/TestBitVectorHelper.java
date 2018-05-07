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

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.PooledByteBufAllocatorL;

public class TestBitVectorHelper {
    @Test
    public void testGetNullCount() throws Exception {
        ArrowBuf validityBuffer = new ArrowBuf(
                null, null, new PooledByteBufAllocatorL().empty,
                null, null, 0, 3, true);
        validityBuffer.setByte(0, 22);

        int count =  BitVectorHelper.getNullCount(validityBuffer, 3);
        assertEquals(count, 1);
    }
}
