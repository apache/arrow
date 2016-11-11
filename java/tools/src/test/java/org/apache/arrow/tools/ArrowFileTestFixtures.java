package org.apache.arrow.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowFooter;
import org.apache.arrow.vector.file.ArrowReader;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;

public class ArrowFileTestFixtures {
  static final int COUNT = 10;

  static void writeData(int count, MapVector parent) {
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    MapWriter rootWriter = writer.rootAsMap();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    for (int i = 0; i < count; i++) {
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
    }
    writer.setValueCount(count);
  }

  static void validateOutput(File testOutFile, BufferAllocator allocator) throws Exception {
    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(testOutFile);
        ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        ) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();

      // initialize vectors
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, readerAllocator)) {
        VectorLoader vectorLoader = new VectorLoader(root);

        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            vectorLoader.load(recordBatch);
          }
          validateContent(COUNT, root);
        }
      }
    }
  }

  static void validateContent(int count, VectorSchemaRoot root) {
    Assert.assertEquals(count, root.getRowCount());
    for (int i = 0; i < count; i++) {
      Assert.assertEquals(i, root.getVector("int").getAccessor().getObject(i));
      Assert.assertEquals(Long.valueOf(i), root.getVector("bigInt").getAccessor().getObject(i));
    }
  }

  static void write(FieldVector parent, File file) throws FileNotFoundException, IOException {
    Schema schema = new Schema(parent.getField().getChildren());
    int valueCount = parent.getAccessor().getValueCount();
    List<FieldVector> fields = parent.getChildrenFromFields();
    VectorUnloader vectorUnloader = new VectorUnloader(schema, valueCount, fields);
    try (
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);
        ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
            ) {
      arrowWriter.writeRecordBatch(recordBatch);
    }
  }


  static void writeInput(File testInFile, BufferAllocator allocator) throws FileNotFoundException, IOException {
    int count = ArrowFileTestFixtures.COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)) {
      writeData(count, parent);
      write(parent.getChild("root"), testInFile);
    }
  }
}
