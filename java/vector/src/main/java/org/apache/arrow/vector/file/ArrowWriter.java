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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class ArrowWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

    private final Schema schema;
    private final List<FieldVector> vectors;
    private final WriteChannel out;
    private final List<ArrowDictionaryBatch> dictionaries;

    private final List<ArrowBlock> dictionaryBlocks = new ArrayList<>();
    private final List<ArrowBlock> recordBlocks = new ArrayList<>();

    private boolean started = false;
    private boolean ended = false;

    private boolean allocated = false;

    /**
     * Note: fields are not closed when the writer is closed
     *
     * @param schema
     * @param out
     * @param allocator
     */
    protected ArrowWriter(Schema schema, OutputStream out, BufferAllocator allocator) {
        this(schema.getFields(), createVectors(schema.getFields(), allocator), Channels.newChannel(out), true);
    }

    protected ArrowWriter(Schema schema, WritableByteChannel out, BufferAllocator allocator) {
        this(schema.getFields(), createVectors(schema.getFields(), allocator), out, true);
    }

    protected ArrowWriter(List<Field> fields, List<FieldVector> vectors, OutputStream out) {
        this(fields, vectors, Channels.newChannel(out), false);
    }

    protected ArrowWriter(List<Field> fields, List<FieldVector> vectors, WritableByteChannel out, boolean allocated) {
        this.vectors = vectors;
        this.out = new WriteChannel(out);
        this.allocated = allocated;

        // translate dictionary fields from in-memory format to message format
        // add dictionary ids, change field types to dictionary type instead of index type
        List<Field> updatedFields = new ArrayList<>(fields);
        List<ArrowDictionaryBatch> dictionaryBatches = new ArrayList<>();
        Set<Long> dictionaryIds = new HashSet<>();

        // go through to add dictionary id to the schema fields and to unload the dictionary batches
        for (FieldVector vector: vectors) {
            if (vector instanceof DictionaryVector) {
                Dictionary dictionary = ((DictionaryVector) vector).getDictionary();
                long dictionaryId = dictionary.getId();
                Field field = vector.getField();
                // find the dictionary field in the schema
                Field schemaField = null;
                int fieldIndex = 0;
                while (fieldIndex < fields.size()) {
                    Field toCheck = fields.get(fieldIndex);
                    if (field.getName().equals(toCheck.getName())) { // TODO more robust comparison?
                        schemaField = toCheck;
                        break;
                    }
                    fieldIndex++;
                }
                if (schemaField == null) {
                    throw new IllegalArgumentException("Dictionary field " + field + " not found in schema " + fields);
                }

                // update the schema field with the dictionary type and the dictionary id for the message format
                ArrowType dictionaryType = dictionary.getVector().getField().getType();
                Field replacement = new Field(field.getName(), field.isNullable(), dictionaryType, dictionary.getEncoding(), field.getChildren());

                updatedFields.remove(fieldIndex);
                updatedFields.add(fieldIndex, replacement);

                // unload the dictionary if we haven't already
                if (dictionaryIds.add(dictionaryId)) {
                    FieldVector dictionaryVector = dictionary.getVector();
                    int valueCount = dictionaryVector.getAccessor().getValueCount();
                    List<ArrowFieldNode> nodes = new ArrayList<>();
                    List<ArrowBuf> buffers = new ArrayList<>();
                    appendNodes(dictionaryVector, nodes, buffers);
                    ArrowRecordBatch batch = new ArrowRecordBatch(valueCount, nodes, buffers);
                    dictionaryBatches.add(new ArrowDictionaryBatch(dictionaryId, batch));
                }
            }
        }

        this.schema = new Schema(updatedFields);
        this.dictionaries = Collections.unmodifiableList(dictionaryBatches);
    }

    public void start() throws IOException {
        ensureStarted();
    }

    public void writeBatch(int count) throws IOException {
        ensureStarted();
        try (ArrowRecordBatch batch = getRecordBatch(count)) {
            writeRecordBatch(batch);
        }
    }

    protected void writeRecordBatch(ArrowRecordBatch batch) throws IOException {
        ArrowBlock block = MessageSerializer.serialize(out, batch);
        LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d",
            block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
        recordBlocks.add(block);
    }

    public void end() throws IOException {
        ensureStarted();
        ensureEnded();
    }

    public long bytesWritten() { return out.getCurrentPosition(); }

    private void ensureStarted() throws IOException {
        if (!started) {
            started = true;
            startInternal(out);
            // write the schema - for file formats this is duplicated in the footer, but matches
            // the streaming format
            MessageSerializer.serialize(out, schema);
            for (ArrowDictionaryBatch batch: dictionaries) {
                try {
                    ArrowBlock block = MessageSerializer.serialize(out, batch);
                    LOGGER.debug(String.format("DictionaryRecordBatch at %d, metadata: %d, body: %d",
                        block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
                    dictionaryBlocks.add(block);
                } finally {
                    batch.close();
                }
            }
        }
    }

    private void ensureEnded() throws IOException {
        if (!ended) {
            ended = true;
            endInternal(out, dictionaryBlocks, recordBlocks);
        }
    }

    protected abstract void startInternal(WriteChannel out) throws IOException;

    protected abstract void endInternal(WriteChannel out,
                                        List<ArrowBlock> dictionaries,
                                        List<ArrowBlock> records) throws IOException;

    private ArrowRecordBatch getRecordBatch(int count) {
        List<ArrowFieldNode> nodes = new ArrayList<>();
        List<ArrowBuf> buffers = new ArrayList<>();
        for (FieldVector vector: vectors) {
            appendNodes(vector, nodes, buffers);
        }
        return new ArrowRecordBatch(count, nodes, buffers);
    }

    private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        Accessor accessor = vector.getAccessor();
        nodes.add(new ArrowFieldNode(accessor.getValueCount(), accessor.getNullCount()));
        List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
        List<ArrowVectorType> expectedBuffers = vector.getField().getTypeLayout().getVectorTypes();
        if (fieldBuffers.size() != expectedBuffers.size()) {
            throw new IllegalArgumentException(String.format(
                    "wrong number of buffers for field %s in vector %s. found: %s",
                    vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
        }
        buffers.addAll(fieldBuffers);

        for (FieldVector child : vector.getChildrenFromFields()) {
            appendNodes(child, nodes, buffers);
        }
    }

    @Override
    public void close() {
        try {
            end();
            out.close();
            if (allocated) {
                for (FieldVector vector: vectors) {
                    vector.close();
                }
            }
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Schema getSchema() {
        return schema;
    }

    public List<FieldVector> getVectors() {
        return vectors;
    }

    public static List<FieldVector> createVectors(List<Field> fields, BufferAllocator allocator) {
        List<FieldVector> vectors = new ArrayList<>();
        for (Field field : fields) {
            MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
            FieldVector vector = minorType.getNewVector(field.getName(), allocator, null);
            vector.initializeChildrenFromFields(field.getChildren());
            vectors.add(vector);
        }
        return vectors;
    }
}
