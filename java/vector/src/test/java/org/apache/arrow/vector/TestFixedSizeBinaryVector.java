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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.NullableFixedSizeBinaryHolder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class TestFixedSizeBinaryVector {
    private static final int numValues = 123;
    private static final int typeWidth = 9;
    private static final int smallTypeWidth = 6;
    private static final int largeTypeWidth = 12;

    private static byte[][] values;
    static {
        values = new byte[numValues][typeWidth];
        for (int i = 0; i < numValues; i++) {
            for (int j = 0; j < typeWidth; j++) {
                values[i][j] = ((byte) i);
            }
        }
    }

    private ArrowBuf[] bufs = new ArrowBuf[numValues];
    private FixedSizeBinaryHolder[] holders = new FixedSizeBinaryHolder[numValues];
    private NullableFixedSizeBinaryHolder[] nullableHolders = new NullableFixedSizeBinaryHolder[numValues];

    private static byte[] smallValue;
    static {
        smallValue = new byte[smallTypeWidth];
        for (int i = 0; i < smallTypeWidth; i++) {
            smallValue[i] = ((byte) i);
        }
    }

    private ArrowBuf smallBuf;
    private FixedSizeBinaryHolder smallHolder;
    private NullableFixedSizeBinaryHolder smallNullableHolder;

    private static byte[] largeValue;
    static {
        largeValue = new byte[largeTypeWidth];
        for (int i = 0; i < largeTypeWidth; i++) {
            largeValue[i] = ((byte) i);
        }
    }

    private ArrowBuf largeBuf;
    private FixedSizeBinaryHolder largeHolder;
    private NullableFixedSizeBinaryHolder largeNullableHolder;

    private FixedSizeBinaryVector.Mutator mutator;
    private FixedSizeBinaryVector.Accessor accessor;

    private static void failWithException(String message) throws Exception {
        throw new Exception(message);
    }

    @Before
    public void setUp() throws Exception {
        BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        FixedSizeBinaryVector fixedSizeBinaryVector = new FixedSizeBinaryVector("fixedSizeBinary", allocator, typeWidth);
        fixedSizeBinaryVector.allocateNew();
        mutator = fixedSizeBinaryVector.getMutator();
        accessor = fixedSizeBinaryVector.getAccessor();

        for (int i = 0; i < numValues; i++) {
            bufs[i] = allocator.buffer(typeWidth);
            bufs[i].setBytes(0, values[i]);

            holders[i] = new FixedSizeBinaryHolder();
            holders[i].byteWidth = typeWidth;
            holders[i].index = i;
            holders[i].buffer = bufs[i];

            nullableHolders[i] = new NullableFixedSizeBinaryHolder();
            nullableHolders[i].byteWidth = typeWidth;
            nullableHolders[i].index = i;
            nullableHolders[i].buffer = bufs[i];
        }

        smallBuf = allocator.buffer(smallTypeWidth);
        smallBuf.setBytes(0, smallValue);

        smallHolder = new FixedSizeBinaryHolder();
        smallHolder.byteWidth = smallTypeWidth;
        smallHolder.index = 0;
        smallHolder.buffer = smallBuf;

        smallNullableHolder = new NullableFixedSizeBinaryHolder();
        smallNullableHolder.byteWidth = smallTypeWidth;
        smallNullableHolder.index = 0;
        smallNullableHolder.buffer = smallBuf;

        largeBuf = allocator.buffer(largeTypeWidth);
        largeBuf.setBytes(0, largeValue);

        largeHolder = new FixedSizeBinaryHolder();
        largeHolder.byteWidth = largeTypeWidth;
        largeHolder.index = 0;
        largeHolder.buffer = largeBuf;

        largeNullableHolder = new NullableFixedSizeBinaryHolder();
        largeNullableHolder.byteWidth = largeTypeWidth;
        largeNullableHolder.index = 0;
        largeNullableHolder.buffer = largeBuf;
    }

    @Test
    public void testSetUsingByteArray() {
        for (int i = 0; i < numValues; i++) {
            mutator.set(i, values[i]);
        }
        mutator.setValueCount(numValues);
        for (int i = 0; i < numValues; i++) {
            assertArrayEquals(values[i], accessor.getObject(i));
        }
    }

    @Test
    public void testSetUsingHolder() {
        for (int i = 0; i < numValues; i++) {
            mutator.set(i, holders[i]);
        }
        mutator.setValueCount(numValues);
        for (int i = 0; i < numValues; i++) {
            assertArrayEquals(values[i], accessor.getObject(i));
        }
    }

    @Test
    public void testSetUsingNullableHolder() {
        for (int i = 0; i < numValues; i++) {
            mutator.set(i, nullableHolders[i]);
        }
        mutator.setValueCount(numValues);
        for (int i = 0; i < numValues; i++) {
            assertArrayEquals(values[i], accessor.getObject(i));
        }
    }

    @Test
    public void testMutatorSetWithInvalidInput() throws Exception {
        String errorMsg = "input data needs to be at least " + typeWidth + " bytes";

        // test small inputs
        try {
            mutator.set(0, smallValue);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.set(0, smallHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.set(0, smallNullableHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.set(0, smallBuf);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        // test large inputs
        mutator.set(0, largeValue);
        mutator.set(0, largeHolder);
        mutator.set(0, largeNullableHolder);
        mutator.set(0, largeBuf);

        // test holders with wrong indices
        try {
            mutator.set(0, holders[3]);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.set(0, nullableHolders[3]);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }
    }

    @Test
    public void setMutatorSetSafeWithInvalidInput() throws Exception {
        String errorMsg = "input data needs to be at least " + typeWidth + " bytes";

        // test small inputs
        try {
            mutator.setSafe(0, smallValue);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.setSafe(0, smallHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.setSafe(0, smallNullableHolder);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.setSafe(0, smallBuf);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        // test large inputs
        mutator.setSafe(0, largeValue);
        mutator.setSafe(0, largeHolder);
        mutator.setSafe(0, largeNullableHolder);
        mutator.setSafe(0, largeBuf);

        // test holders with wrong indices
        try {
            mutator.setSafe(0, holders[3]);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }

        try {
            mutator.setSafe(0, nullableHolders[3]);
            failWithException(errorMsg);
        } catch (AssertionError ignore) {
        }
    }
}
