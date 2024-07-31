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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/**
 * Converts an Arrow file to an Arrow stream. The file should be specified as the first argument and
 * the output is written to standard out.
 */
public class FileToStream {
  private FileToStream() {}

  /** Reads an Arrow file from in and writes it back to out. */
  public static void convert(FileInputStream in, OutputStream out) throws IOException {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    try (ArrowFileReader reader = new ArrowFileReader(in.getChannel(), allocator)) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      // load the first batch before instantiating the writer so that we have any dictionaries
      // only writeBatches if we loaded one in the first place.
      boolean writeBatches = reader.loadNextBatch();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, reader, out)) {
        writer.start();
        while (writeBatches) {
          writer.writeBatch();
          if (!reader.loadNextBatch()) {
            break;
          }
        }
        writer.end();
      }
    }
  }

  /**
   * Main method. The first arg is the file path. The second, optional argument, is an output file
   * location (defaults to standard out).
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 1 && args.length != 2) {
      System.err.println("Usage: FileToStream <input file> [output file]");
      System.exit(1);
    }

    FileInputStream in = new FileInputStream(new File(args[0]));
    OutputStream out = args.length == 1 ? System.out : new FileOutputStream(new File(args[1]));

    convert(in, out);
  }
}
