/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.arrow.tools;

import static org.apache.arrow.tools.ArrowFileTestFixtures.validateOutput;
import static org.apache.arrow.tools.ArrowFileTestFixtures.write;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeData;
import static org.apache.arrow.tools.ArrowFileTestFixtures.writeInput;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.tools.Integration.Command;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.NopIndenter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TestIntegration {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testValid() throws Exception {
    File testInFile = testFolder.newFile("testIn.arrow");
    File testJSONFile = testFolder.newFile("testOut.json");
    testJSONFile.delete();
    File testOutFile = testFolder.newFile("testOut.arrow");
    testOutFile.delete();

    // generate an arrow file
    writeInput(testInFile, allocator);

    Integration integration = new Integration();

    // convert it to json
    String[] args1 = { "-arrow", testInFile.getAbsolutePath(), "-json",  testJSONFile.getAbsolutePath(), "-command", Command.ARROW_TO_JSON.name()};
    integration.run(args1);

    // convert back to arrow
    String[] args2 = { "-arrow", testOutFile.getAbsolutePath(), "-json",  testJSONFile.getAbsolutePath(), "-command", Command.JSON_TO_ARROW.name()};
    integration.run(args2);

    // check it is the same
    validateOutput(testOutFile, allocator);

    // validate arrow against json
    String[] args3 = { "-arrow", testInFile.getAbsolutePath(), "-json",  testJSONFile.getAbsolutePath(), "-command", Command.VALIDATE.name()};
    integration.run(args3);
  }

  @Test
  public void testJSONRoundTripWithVariableWidth() throws Exception {
    File testJSONFile = new File("../../integration/data/simple.json");
    File testOutFile = testFolder.newFile("testOut.arrow");
    File testRoundTripJSONFile = testFolder.newFile("testOut.json");
    testOutFile.delete();
    testRoundTripJSONFile.delete();

    Integration integration = new Integration();

    // convert to arrow
    String[] args1 = { "-arrow", testOutFile.getAbsolutePath(), "-json",  testJSONFile.getAbsolutePath(), "-command", Command.JSON_TO_ARROW.name()};
    integration.run(args1);

    // convert back to json
    String[] args2 = { "-arrow", testOutFile.getAbsolutePath(), "-json",  testRoundTripJSONFile.getAbsolutePath(), "-command", Command.ARROW_TO_JSON.name()};
    integration.run(args2);

    BufferedReader orig = readNormalized(testJSONFile);
    BufferedReader rt = readNormalized(testRoundTripJSONFile);
    String i, o;
    int j = 0;
    while ((i = orig.readLine()) != null && (o = rt.readLine()) != null) {
      Assert.assertEquals("line: " + j, i, o);
      ++j;
    }
  }

  private ObjectMapper om = new ObjectMapper();
  {
    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
    prettyPrinter.indentArraysWith(NopIndenter.instance);
    om.setDefaultPrettyPrinter(prettyPrinter);
    om.enable(SerializationFeature.INDENT_OUTPUT);
    om.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  private BufferedReader readNormalized(File f) throws IOException {
    Map<?,?> tree = om.readValue(f, Map.class);
    String normalized = om.writeValueAsString(tree);
    return new BufferedReader(new StringReader(normalized));
  }


  @Test
  public void testInvalid() throws Exception {
    File testValidInFile = testFolder.newFile("testValidIn.arrow");
    File testInvalidInFile = testFolder.newFile("testInvalidIn.arrow");
    File testJSONFile = testFolder.newFile("testInvalidOut.json");
    testJSONFile.delete();

    // generate an arrow file
    writeInput(testValidInFile, allocator);
    // generate a different arrow file
    writeInput2(testInvalidInFile, allocator);

    Integration integration = new Integration();

    // convert the "valid" file to json
    String[] args1 = { "-arrow", testValidInFile.getAbsolutePath(), "-json",  testJSONFile.getAbsolutePath(), "-command", Command.ARROW_TO_JSON.name()};
    integration.run(args1);

    // compare the "invalid" file to the "valid" json
    String[] args3 = { "-arrow", testInvalidInFile.getAbsolutePath(), "-json",  testJSONFile.getAbsolutePath(), "-command", Command.VALIDATE.name()};
    // this should fail
    try {
      integration.run(args3);
      fail("should have failed");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Different values in column"));
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("999"));
    }

  }

  static void writeInput2(File testInFile, BufferAllocator allocator) throws FileNotFoundException, IOException {
    int count = ArrowFileTestFixtures.COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)) {
      writeData(count, parent);
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter rootWriter = writer.rootAsMap();
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

}
