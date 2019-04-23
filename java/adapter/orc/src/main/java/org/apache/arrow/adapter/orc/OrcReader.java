package org.apache.arrow.adapter.orc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

public class OrcReader extends ArrowReader {

    protected OrcReader(BufferAllocator allocator) {
        super(allocator);
    }

    /**
     * Load the next ArrowRecordBatch to the vector schema root if available.
     *
     * @return true if a batch was read, false on EOS
     * @throws IOException on error
     */
    @Override
    public  boolean loadNextBatch() throws IOException {
        return false;
    }

    /**
     * Return the number of bytes read from the ReadChannel.
     *
     * @return number of bytes read
     */
    @Override
    public long bytesRead() {
        return 0;
    }

    /**
     * Close the underlying read source.
     *
     * @throws IOException on error
     */
    @Override
    protected void closeReadSource() throws IOException {

    }

    /**
     * Read the Schema from the source, will be invoked at the beginning the initialization.
     *
     * @return the read Schema
     * @throws IOException on error
     */
    @Override
    protected Schema readSchema() throws IOException {
        return null;
    }

    /**
     * Read a dictionary batch from the source, will be invoked after the schema has been read and
     * called N times, where N is the number of dictionaries indicated by the schema Fields.
     *
     * @return the read ArrowDictionaryBatch
     * @throws IOException on error
     */
    @Override
    protected ArrowDictionaryBatch readDictionary() throws IOException {
        return null;
    }
}
