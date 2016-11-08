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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.file.ArrowBlock;
import org.apache.arrow.vector.file.ArrowFooter;
import org.apache.arrow.vector.file.ArrowReader;
import org.apache.arrow.vector.file.ArrowWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRoundtrip {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileRoundtrip.class);

  public static void main(String[] args) {
    System.exit(new FileRoundtrip(System.out, System.err).run(args));
  }

  private final Options options;
  private final PrintStream out;
  private final PrintStream err;

  FileRoundtrip(PrintStream out, PrintStream err) {
    this.out = out;
    this.err = err;
    this.options = new Options();
    this.options.addOption("i", "in", true, "input file");
    this.options.addOption("o", "out", true, "output file");

  }

  private File validateFile(String type, String fileName) {
    if (fileName == null) {
      throw new IllegalArgumentException("missing " + type + " file parameter");
    }
    File f = new File(fileName);
    if (!f.exists() || f.isDirectory()) {
      throw new IllegalArgumentException(type + " file not found: " + f.getAbsolutePath());
    }
    return f;
  }

  int run(String[] args) {
    try {
      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(options, args, false);

      String inFileName = cmd.getOptionValue("in");
      String outFileName = cmd.getOptionValue("out");

      File inFile = validateFile("input", inFileName);
      File outFile = validateFile("output", outFileName);
      BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE); // TODO: close
      try(
          FileInputStream fileInputStream = new FileInputStream(inFile);
          ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), allocator);) {

        ArrowFooter footer = arrowReader.readFooter();
        Schema schema = footer.getSchema();
        LOGGER.debug("Input file size: " + inFile.length());
        LOGGER.debug("Found schema: " + schema);

        try (
            FileOutputStream fileOutputStream = new FileOutputStream(outFile);
            ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);
            ) {

          // initialize vectors

          List<ArrowBlock> recordBatches = footer.getRecordBatches();
          for (ArrowBlock rbBlock : recordBatches) {
            try (ArrowRecordBatch inRecordBatch = arrowReader.readRecordBatch(rbBlock);
                VectorSchemaRoot root = new VectorSchemaRoot(schema, allocator);) {

              VectorLoader vectorLoader = new VectorLoader(root);
              vectorLoader.load(inRecordBatch);

              VectorUnloader vectorUnloader = new VectorUnloader(root);
              ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
              arrowWriter.writeRecordBatch(recordBatch);
            }
          }
        }
        LOGGER.debug("Output file size: " + outFile.length());
      }
    } catch (ParseException e) {
      return fatalError("Invalid parameters", e);
    } catch (IOException e) {
      return fatalError("Error accessing files", e);
    }
    return 0;
  }

  private int fatalError(String message, Throwable e) {
    err.println(message);
    LOGGER.error(message, e);
    return 1;
  }

}
