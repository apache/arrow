package org.apache.arrow.adapter.jdbc;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for {@link ArrowDataFetcher}.
 */
public class ArrowDataFetcherTest {

    @Test
    public void commaSeparatedQueryColumnsTest() {
        try {
            ArrowDataFetcher.commaSeparatedQueryColumns(null);
        } catch (AssertionError error) {
            assertTrue(true);
        }
        assertEquals(" one ", ArrowDataFetcher.commaSeparatedQueryColumns("one"));
        assertEquals(" one, two ", ArrowDataFetcher.commaSeparatedQueryColumns("one", "two"));
        assertEquals(" one, two, three ", ArrowDataFetcher.commaSeparatedQueryColumns("one", "two", "three"));
    }
}
