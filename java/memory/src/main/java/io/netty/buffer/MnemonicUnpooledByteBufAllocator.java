/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;

import org.apache.mnemonic.CommonAllocator;

/**
 * Simplistic {@link ByteBufAllocator} implementation that does not pool anything.
 */
public final class MnemonicUnpooledByteBufAllocator<A extends CommonAllocator<A>> extends AbstractByteBufAllocator {

    private A mcalloc;

    /**
     * Default instance
     */
    //        public static final UnpooledByteBufAllocator DEFAULT =
    //      new UnpooledByteBufAllocator(PlatformDependent.directBufferPreferred());

    /**
     * Create a new instance
     *
     * @param preferDirect {@code true} if {@link #buffer(int)} should try to allocate a direct buffer rather than
     *                     a heap buffer
     */
    public MnemonicUnpooledByteBufAllocator(boolean preferDirect, A mcallocator) {
        super(preferDirect);
	this.mcalloc = mcallocator;
    }

    public A getAllocator() {
	return this.mcalloc;
    }

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            return new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity);
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
	    assert null != this.mcalloc;
            ByteBuf buf;
            if (PlatformDependent.hasUnsafe()) {
                buf = new MnemonicUnpooledUnsafeDirectByteBuf<A>(this, initialCapacity, maxCapacity);
            } else {
                buf = new MnemonicUnpooledDirectByteBuf<A>(this, initialCapacity, maxCapacity);
            }

            return toLeakAwareBuffer(buf);
        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }
}
