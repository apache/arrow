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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowFooter;
import org.apache.arrow.vector.file.ArrowReader;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;

public class FileRoundtrip {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileRoundtrip.class);
  public static final Options OPTIONS;

  static {
    OPTIONS = new Options();
  }

  public static void main(String[] args) {
    try {
      String[] cargs = Arrays.copyOfRange(args, 1, args.length);

      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(OPTIONS, cargs, false);

      String[] parsed_args = cmd.getArgs();

      String inFileName = parsed_args[0];
      String outFileName = parsed_args[1];

      BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

      File inFile = new File(inFileName);
      FileInputStream fileInputStream = new FileInputStream(inFile);
      ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), allocator);

      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("Found schema: " + schema);

      File outFile = new File(outFileName);
      FileOutputStream fileOutputStream = new FileOutputStream(outFile);
      ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);

      // initialize vectors

      List<ArrowBlock> recordBatches = footer.getRecordBatches();
      for (ArrowBlock rbBlock : recordBatches) {
        ArrowRecordBatch inRecordBatch = arrowReader.readRecordBatch(rbBlock);

        NullableMapVector inParent = new NullableMapVector("parent", allocator, null);
        NullableMapVector root = inParent.addOrGet("root", Types.MinorType.MAP, NullableMapVector.class);
        VectorLoader vectorLoader = new VectorLoader(schema, root);
        vectorLoader.load(inRecordBatch);

        NullableMapVector outParent = new NullableMapVector("parent", allocator, null);
        VectorUnloader vectorUnloader = new VectorUnloader(outParent);
        ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
        arrowWriter.writeRecordBatch(recordBatch);
      }
    } catch (Throwable th) {
      System.err.println(th.getMessage());
    }
  }
}
