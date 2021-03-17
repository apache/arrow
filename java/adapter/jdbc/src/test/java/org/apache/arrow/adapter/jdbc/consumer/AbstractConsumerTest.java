package org.apache.arrow.adapter.jdbc.consumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;


public abstract class AbstractConsumerTest {

    protected BufferAllocator allocator;

    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void tearDown() {
        allocator.close();
    }

}
