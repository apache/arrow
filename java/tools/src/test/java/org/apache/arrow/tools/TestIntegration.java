/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.tools;

import static org.apache.arrow.tools.ArrowFileTestFixtures.validateOutput;
import static org.apache.arrow.tools.ArrowFileTestFixtures.validateVariadicOutput;
import static org.apache.arrow.tools.ArrowFileTestFixtures.write;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeData;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeInput;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeVariableWidthViewInput;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.NopIndenter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.tools.Integration.Command;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestIntegration {

  @TempDir public File testFolder;

  private BufferAllocator allocator;
  private ObjectMapper om = new ObjectMapper();

  {
    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
    prettyPrinter.indentArraysWith(NopIndenter.instance);
    om.setDefaultPrettyPrinter(prettyPrinter);
    om.enable(SerializationFeature.INDENT_OUTPUT);
    om.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  static void writeInputFloat(File testInFile, BufferAllocator allocator, double... f)
      throws IOException {
    try (BufferAllocator vectorAllocator =
            allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", vectorAllocator)) {
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      Float8Writer floatWriter = rootWriter.float8("float");
      for (int i = 0; i < f.length; i++) {
        floatWriter.setPosition(i);
        floatWriter.writeFloat8(f[i]);
      }
      writer.setValueCount(f.length);
      write(parent.getChild("root"), testInFile);
    }
  }

  static void writeInput2(File testInFile, BufferAllocator allocator) throws IOException {
    int count = ArrowFileTestFixtures.COUNT;
    try (BufferAllocator vectorAllocator =
            allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", vectorAllocator)) {
      writeData(count, parent);
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      StructWriter rootWriter = writer.rootAsStruct();
      IntWriter intWriter = rootWriter.integer("int");
      BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
      intWriter.setPosition(5);
      intWriter.writeInt(999);
      bigIntWriter.setPosition(4);
      bigIntWriter.writeBigInt(777L);
      writer.setValueCount(count);
      write(parent.getChild("root"), testInFile);
    }
  }

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testValid() throws Exception {
    File testInFile = new File(testFolder, "testIn.arrow");
    File testJSONFile = new File(testFolder, "testOut.json");
    testJSONFile.delete();
    File testOutFile = new File(testFolder, "testOut.arrow");
    testOutFile.delete();

    // generate an arrow file
    writeInput(testInFile, allocator);

    Integration integration = new Integration();

    // convert it to json
    String[] args1 = {
      "-arrow",
      testInFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.ARROW_TO_JSON.name()
    };
    integration.run(args1);

    // convert back to arrow
    String[] args2 = {
      "-arrow",
      testOutFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.JSON_TO_ARROW.name()
    };
    integration.run(args2);

    // check it is the same
    validateOutput(testOutFile, allocator);

    // validate arrow against json
    String[] args3 = {
      "-arrow",
      testInFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.VALIDATE.name()
    };
    integration.run(args3);
  }

  @Test
  public void testJSONRoundTripWithVariableWidth() throws Exception {
    File testJSONFile =
        new File("../../docs/source/format/integration_json_examples/simple.json")
            .getCanonicalFile();
    if (!testJSONFile.exists()) {
      testJSONFile = new File("../docs/source/format/integration_json_examples/simple.json");
    }
    File testOutFile = new File(testFolder, "testOut.arrow");
    File testRoundTripJSONFile = new File(testFolder, "testOut.json");
    testOutFile.delete();
    testRoundTripJSONFile.delete();

    Integration integration = new Integration();

    // convert to arrow
    String[] args1 = {
      "-arrow",
      testOutFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.JSON_TO_ARROW.name()
    };
    integration.run(args1);

    // convert back to json
    String[] args2 = {
      "-arrow",
      testOutFile.getAbsolutePath(),
      "-json",
      testRoundTripJSONFile.getAbsolutePath(),
      "-command",
      Command.ARROW_TO_JSON.name()
    };
    integration.run(args2);

    BufferedReader orig = readNormalized(testJSONFile);
    BufferedReader rt = readNormalized(testRoundTripJSONFile);
    String i;
    String o;
    int j = 0;
    while ((i = orig.readLine()) != null && (o = rt.readLine()) != null) {
      assertEquals(i, o, "line: " + j);
      ++j;
    }
  }

  @Test
  public void testJSONRoundTripWithStruct() throws Exception {
    File testJSONFile =
        new File("../../docs/source/format/integration_json_examples/struct.json")
            .getCanonicalFile();
    if (!testJSONFile.exists()) {
      testJSONFile = new File("../docs/source/format/integration_json_examples/struct.json");
    }
    File testOutFile = new File(testFolder, "testOutStruct.arrow");
    File testRoundTripJSONFile = new File(testFolder, "testOutStruct.json");
    testOutFile.delete();
    testRoundTripJSONFile.delete();

    Integration integration = new Integration();

    // convert to arrow
    String[] args1 = {
      "-arrow",
      testOutFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.JSON_TO_ARROW.name()
    };
    integration.run(args1);

    // convert back to json
    String[] args2 = {
      "-arrow",
      testOutFile.getAbsolutePath(),
      "-json",
      testRoundTripJSONFile.getAbsolutePath(),
      "-command",
      Command.ARROW_TO_JSON.name()
    };
    integration.run(args2);

    BufferedReader orig = readNormalized(testJSONFile);
    BufferedReader rt = readNormalized(testRoundTripJSONFile);
    String i;
    String o;
    int j = 0;
    while ((i = orig.readLine()) != null && (o = rt.readLine()) != null) {
      assertEquals(i, o, "line: " + j);
      ++j;
    }
  }

  private BufferedReader readNormalized(File f) throws IOException {
    Map<?, ?> tree = om.readValue(f.getCanonicalFile(), Map.class);
    String normalized = om.writeValueAsString(tree);
    return new BufferedReader(new StringReader(normalized));
  }

  /** The test should not be sensitive to small variations in float representation. */
  @Test
  public void testFloat() throws Exception {
    File testValidInFile = new File(testFolder, "testValidFloatIn.arrow");
    File testInvalidInFile = new File(testFolder, "testAlsoValidFloatIn.arrow");
    File testJSONFile = new File(testFolder, "testValidOut.json");
    testJSONFile.delete();

    // generate an arrow file
    writeInputFloat(testValidInFile, allocator, 912.4140000000002, 912.414);
    // generate a different arrow file
    writeInputFloat(testInvalidInFile, allocator, 912.414, 912.4140000000002);

    Integration integration = new Integration();

    // convert the "valid" file to json
    String[] args1 = {
      "-arrow",
      testValidInFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.ARROW_TO_JSON.name()
    };
    integration.run(args1);

    // compare the "invalid" file to the "valid" json
    String[] args3 = {
      "-arrow",
      testInvalidInFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.VALIDATE.name()
    };
    // this should fail
    integration.run(args3);
  }

  @Test
  public void testInvalid() throws Exception {
    File testValidInFile = new File(testFolder, "testValidIn.arrow");
    File testInvalidInFile = new File(testFolder, "testInvalidIn.arrow");
    File testJSONFile = new File(testFolder, "testInvalidOut.json");
    testJSONFile.delete();

    // generate an arrow file
    writeInput(testValidInFile, allocator);
    // generate a different arrow file
    writeInput2(testInvalidInFile, allocator);

    Integration integration = new Integration();

    // convert the "valid" file to json
    String[] args1 = {
      "-arrow",
      testValidInFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.ARROW_TO_JSON.name()
    };
    integration.run(args1);

    // compare the "invalid" file to the "valid" json
    String[] args3 = {
      "-arrow",
      testInvalidInFile.getAbsolutePath(),
      "-json",
      testJSONFile.getAbsolutePath(),
      "-command",
      Command.VALIDATE.name()
    };
    // this should fail
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              integration.run(args3);
            });

    assertTrue(e.getMessage().contains("Different values in column"), e.getMessage());
    assertTrue(e.getMessage().contains("999"), e.getMessage());
  }

  @Test
  public void testValidateVariableWidthView() throws Exception {
    final int valueCount = 256;
    final int multiplier = 6;

    for (int i = 1; i < multiplier; i++) {
      File testInFile = new File(testFolder, "testIn.arrow");
      File testJSONFile = new File(testFolder, "testOut.json");
      testJSONFile.delete();
      File testOutFile = new File(testFolder, "testOut.arrow");
      testOutFile.delete();

      writeVariableWidthViewInput(testInFile, allocator, multiplier * valueCount);

      Integration integration = new Integration();

      // convert it to json
      String[] args1 = {
        "-arrow",
        testInFile.getAbsolutePath(),
        "-json",
        testJSONFile.getAbsolutePath(),
        "-command",
        Command.ARROW_TO_JSON.name()
      };
      integration.run(args1);

      // convert back to arrow
      String[] args2 = {
        "-arrow",
        testOutFile.getAbsolutePath(),
        "-json",
        testJSONFile.getAbsolutePath(),
        "-command",
        Command.JSON_TO_ARROW.name()
      };
      integration.run(args2);

      // check it is the same
      validateVariadicOutput(testOutFile, allocator, multiplier * valueCount);

      // validate arrow against json
      String[] args3 = {
        "-arrow",
        testInFile.getAbsolutePath(),
        "-json",
        testJSONFile.getAbsolutePath(),
        "-command",
        Command.VALIDATE.name()
      };
      integration.run(args3);
    }
  }
}
