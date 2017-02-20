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

import com.google.common.collect.Iterators;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowMessage.ArrowMessageVisitor;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.VectorLayout;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class ArrowReader<T extends ReadChannel> implements AutoCloseable {

    private final T in;
    private final BufferAllocator allocator;
    private Schema schema;

    private List<FieldVector> vectors;
    private Map<String, FieldVector> vectorsByName;
    private Map<Long, FieldVector> dictionaries;

    private int batchCount = 0;
    private boolean initialized = false;

    protected ArrowReader(T in, BufferAllocator allocator) {
        this.in = in;
        this.allocator = allocator;
    }

    public Schema getSchema() throws IOException {
        ensureInitialized();
        return schema;
    }

    public List<FieldVector> getVectors() throws IOException {
        ensureInitialized();
        return vectors;
    }

    public int loadNextBatch() throws IOException {
        ensureInitialized();
        batchCount = 0;
        // read in all dictionary batches, then stop after our first record batch
        ArrowMessageVisitor<Boolean> visitor = new ArrowMessageVisitor<Boolean>() {
            @Override
            public Boolean visit(ArrowDictionaryBatch message) {
                try {
                    load(message);
                } finally {
                    message.close();
                }
                return true;
            }
            @Override
            public Boolean visit(ArrowRecordBatch message) {
                try {
                    load(message);
                } finally {
                    message.close();
                }
                return false;
            }
        };
        ArrowMessage message = readMessage(in, allocator);
        while (message != null && message.accepts(visitor)) {
            message = readMessage(in, allocator);
        }
        return batchCount;
    }

    public long bytesRead() { return in.bytesRead(); }

    @Override
    public void close() throws IOException {
        if (initialized) {
            for (FieldVector vector: vectors) {
                vector.close();
            }
            for (FieldVector vector: dictionaries.values()) {
                vector.close();
            }
        }
        in.close();
    }

    protected abstract Schema readSchema(T in) throws IOException;

    protected abstract ArrowMessage readMessage(T in, BufferAllocator allocator) throws IOException;

    protected void ensureInitialized() throws IOException {
        if (!initialized) {
            initialize();
            initialized = true;
        }
    }

    /**
     * Reads the schema and initializes the vectors
     */
    private void initialize() throws IOException {
        Schema schema = readSchema(in);
        List<Field> fields = new ArrayList<>();
        List<FieldVector> vectors = new ArrayList<>();
        Map<String, FieldVector> vectorsByName = new HashMap<>();
        Map<Long, FieldVector> dictionaries = new HashMap<>();
        // in the message format, fields have dictionary ids and the dictionary type
        // in the memory format, they have no dictionary id and the index type
        for (Field field: schema.getFields()) {
            DictionaryEncoding dictionaryEncoding = field.getDictionary();
            if (dictionaryEncoding == null) {
                MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
                FieldVector vector = minorType.getNewVector(field.getName(), allocator, null);
                vector.initializeChildrenFromFields(field.getChildren());
                fields.add(field);
                vectors.add(vector);
                vectorsByName.put(field.getName(), vector);
            } else {
                // get existing or create dictionary vector
                FieldVector dictionaryVector = dictionaries.get(dictionaryEncoding.getId());
                if (dictionaryVector == null) {
                    MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
                    dictionaryVector = minorType.getNewVector(field.getName(), allocator, null);
                    dictionaryVector.initializeChildrenFromFields(field.getChildren());
                    dictionaries.put(dictionaryEncoding.getId(), dictionaryVector);
                }
                // create index vector
                ArrowType dictionaryType = new ArrowType.Int(32, true); // TODO check actual index type
                Field updated = new Field(field.getName(), field.isNullable(), dictionaryType, null);
                MinorType minorType = Types.getMinorTypeForArrowType(dictionaryType);
                FieldVector vector = minorType.getNewVector(field.getName(), allocator, null);
                // note: we don't need to initialize children as the index vector won't have any
                Dictionary metadata = new Dictionary(dictionaryVector, dictionaryEncoding.getId(), dictionaryEncoding.isOrdered());
                DictionaryVector dictionary = new DictionaryVector(vector, metadata);
                fields.add(updated);
                vectors.add(dictionary);
                vectorsByName.put(updated.getName(), dictionary);
            }
        }
        this.schema = new Schema(fields);
        this.vectors = Collections.unmodifiableList(vectors);
        this.vectorsByName = Collections.unmodifiableMap(vectorsByName);
        this.dictionaries = Collections.unmodifiableMap(dictionaries);
    }

    private void load(ArrowDictionaryBatch dictionaryBatch) {
        long id = dictionaryBatch.getDictionaryId();
        FieldVector vector = dictionaries.get(id);
        if (vector == null) {
            throw new IllegalArgumentException("Dictionary ID " + id + " not defined in schema");
        }
        ArrowRecordBatch recordBatch = dictionaryBatch.getDictionary();
        Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
        Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
        loadBuffers(vector, vector.getField(), buffers, nodes);
    }

    /**
     * Loads the record batch in the vectors
     * will not close the record batch
     * @param recordBatch
     */
    private void load(ArrowRecordBatch recordBatch) {
        Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
        Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
        List<Field> fields = schema.getFields();
        for (Field field : fields) {
            FieldVector fieldVector = vectorsByName.get(field.getName());
            loadBuffers(fieldVector, field, buffers, nodes);
        }
        this.batchCount = recordBatch.getLength();
        if (nodes.hasNext() || buffers.hasNext()) {
            throw new IllegalArgumentException("not all nodes and buffers where consumed. nodes: " +
                Iterators.toString(nodes) + " buffers: " + Iterators.toString(buffers));
        }
    }

    private static void loadBuffers(FieldVector vector,
                                    Field field,
                                    Iterator<ArrowBuf> buffers,
                                    Iterator<ArrowFieldNode> nodes) {
        checkArgument(nodes.hasNext(),
                      "no more field nodes for for field " + field + " and vector " + vector);
        ArrowFieldNode fieldNode = nodes.next();
        List<VectorLayout> typeLayout = field.getTypeLayout().getVectors();
        List<ArrowBuf> ownBuffers = new ArrayList<>(typeLayout.size());
        for (int j = 0; j < typeLayout.size(); j++) {
            ownBuffers.add(buffers.next());
        }
        try {
            vector.loadFieldBuffers(fieldNode, ownBuffers);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Could not load buffers for field " +
                field + ". error message: " + e.getMessage(), e);
        }
        List<Field> children = field.getChildren();
        if (children.size() > 0) {
            List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
            checkArgument(children.size() == childrenFromFields.size(), "should have as many children as in the schema: found " + childrenFromFields.size() + " expected " + children.size());
            for (int i = 0; i < childrenFromFields.size(); i++) {
                Field child = children.get(i);
                FieldVector fieldVector = childrenFromFields.get(i);
                loadBuffers(fieldVector, child, buffers, nodes);
            }
        }
    }
}
