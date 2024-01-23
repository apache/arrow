package org.apache.arrow.adapter.avro;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static org.apache.arrow.adapter.avro.AvroToArrow.avroToArrowIterator;

public class ToastAvroToArrowTest {

    protected AvroToArrowConfig config;

    @ClassRule
    public static final TemporaryFolder TMP = new TemporaryFolder();

    @Before
    public void init() {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        config = new AvroToArrowConfigBuilder(allocator).build();
    }

    static final String schemaName = "toast/test_deep_nesting.avsc";

    public static Schema getSchema(String schemaName) throws Exception {
        try (InputStream is = ToastAvroToArrowTest.class.getResourceAsStream("/schema/" + schemaName)) {
            return new Schema.Parser().parse(is);
        }
    }

    public List<GenericRecord> generateData() throws Exception {
        Schema nestedRecordExampleSchema = getSchema(schemaName);
        Schema nestedRecord1Schema = nestedRecordExampleSchema.getField("NestedRecord1").schema();
        Schema nestedRecord2Schema = nestedRecordExampleSchema.getField("NestedRecord2").schema();
        Schema nestedRecord3Schema = nestedRecord2Schema.getField("NestedRecord3").schema();
        Schema nestedRecord4Schema = nestedRecordExampleSchema.getField("NestedRecord4").schema();
        Schema nestedRecord5Schema = nestedRecord4Schema.getField("NestedRecords").schema().getElementType();
        ArrayList<GenericRecord> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // Create sample data
            GenericRecord nestedRecordExample = new GenericData.Record(nestedRecordExampleSchema);
            nestedRecordExample.put("id", 1);

            GenericRecord nestedRecord1 = new GenericData.Record(nestedRecord1Schema);
            nestedRecord1.put("id", 2);
            nestedRecordExample.put("NestedRecord1", nestedRecord1);

            GenericRecord nestedRecord2 = new GenericData.Record(nestedRecord2Schema);
            nestedRecord2.put("id", 3);
            GenericRecord nestedRecord3 = new GenericData.Record(nestedRecord3Schema);
            nestedRecord3.put("id", 4);
            nestedRecord2.put("NestedRecord3", nestedRecord3);
            nestedRecordExample.put("NestedRecord2", nestedRecord2);

            GenericRecord nestedRecord4 = new GenericData.Record(nestedRecord4Schema);
            nestedRecord4.put("id", 5);
            GenericRecord nestedRecord5 = new GenericData.Record(nestedRecord5Schema);
            nestedRecord5.put("id", 6);
            List<GenericRecord> nestedRecords = new ArrayList<>();
            nestedRecords.add(nestedRecord5);
            nestedRecord4.put("NestedRecords", nestedRecords);
            nestedRecordExample.put("NestedRecord4", nestedRecord4);
            data.add(nestedRecordExample);
        }
        return data;
    }

    @Test
    public void nestedSchemaAvroToArrowTest() throws Exception {

        File dataFile = TMP.newFile();
        List<GenericRecord> data = generateData();
        Schema schema = getSchema(schemaName);

        BinaryEncoder encoder
                = new EncoderFactory().directBinaryEncoder(newOutputStream(dataFile.toPath()), null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(newInputStream(dataFile.toPath()), null);
        for (GenericRecord datum : data) {
            writer.write(datum, encoder);
        }

        try (BufferAllocator allocator = new RootAllocator()) {
            AvroToArrowConfig config = new AvroToArrowConfigBuilder(allocator).build();
            try (AvroToArrowVectorIterator avroToArrowVectorIterator = avroToArrowIterator(schema, decoder, config)) {
                while(avroToArrowVectorIterator.hasNext()) {
                    try (VectorSchemaRoot root = avroToArrowVectorIterator.next()) {
                        System.out.print(root.contentToTSVString());
                    }
                }
            }
        }
    }
}
