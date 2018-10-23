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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.JsonFileReader;
import org.apache.arrow.vector.ipc.JsonFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Integration {
  private static final Logger LOGGER = LoggerFactory.getLogger(Integration.class);
  private final Options options;

  Integration() {
    this.options = new Options();
    this.options.addOption("a", "arrow", true, "arrow file");
    this.options.addOption("j", "json", true, "json file");
    this.options.addOption("c", "command", true, "command to execute: " + Arrays.toString(Command
        .values()));
  }

  public static void main(String[] args) {
    try {
      new Integration().run(args);
    } catch (ParseException e) {
      fatalError("Invalid parameters", e);
    } catch (IOException e) {
      fatalError("Error accessing files", e);
    } catch (RuntimeException e) {
      fatalError("Incompatible files", e);
    }
  }

  private static void fatalError(String message, Throwable e) {
    System.err.println(message);
    System.err.println(e.getMessage());
    LOGGER.error(message, e);
    System.exit(1);
  }

  private File validateFile(String type, String fileName, boolean shouldExist) {
    if (fileName == null) {
      throw new IllegalArgumentException("missing " + type + " file parameter");
    }
    File f = new File(fileName);
    if (shouldExist && (!f.exists() || f.isDirectory())) {
      throw new IllegalArgumentException(type + " file not found: " + f.getAbsolutePath());
    }
    if (!shouldExist && f.exists()) {
      throw new IllegalArgumentException(type + " file already exists: " + f.getAbsolutePath());
    }
    return f;
  }

  static void extractDictionaryEncodings(List<Field> fields, List<DictionaryEncoding> encodings) {
    for (Field field : fields) {
      DictionaryEncoding encoding = field.getDictionary();
      if (encoding != null) {
        encodings.add(encoding);
      }

      extractDictionaryEncodings(field.getChildren(), encodings);
    }
  }

  void run(String[] args) throws ParseException, IOException {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args, false);

    Command command = toCommand(cmd.getOptionValue("command"));
    File arrowFile = validateFile("arrow", cmd.getOptionValue("arrow"), command.arrowExists);
    File jsonFile = validateFile("json", cmd.getOptionValue("json"), command.jsonExists);
    command.execute(arrowFile, jsonFile);
  }

  private Command toCommand(String commandName) {
    try {
      return Command.valueOf(commandName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown command: " + commandName + " expected one of " +
          Arrays.toString(Command.values()));
    }
  }

  enum Command {
    ARROW_TO_JSON(true, false) {
      @Override
      public void execute(File arrowFile, File jsonFile) throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             FileInputStream fileInputStream = new FileInputStream(arrowFile);
             ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(),
                 allocator)) {
          VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
          Schema schema = root.getSchema();
          LOGGER.debug("Input file size: " + arrowFile.length());
          LOGGER.debug("Found schema: " + schema);
          try (JsonFileWriter writer = new JsonFileWriter(jsonFile, JsonFileWriter.config()
              .pretty(true))) {
            writer.start(schema, arrowReader);
            for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
              if (!arrowReader.loadRecordBatch(rbBlock)) {
                throw new IOException("Expected to load record batch");
              }
              writer.write(root);
            }
          }
          LOGGER.debug("Output file size: " + jsonFile.length());
        }
      }
    },
    JSON_TO_ARROW(false, true) {
      @Override
      public void execute(File arrowFile, File jsonFile) throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             JsonFileReader reader = new JsonFileReader(jsonFile, allocator)) {
          Schema schema = reader.start();
          LOGGER.debug("Input file size: " + jsonFile.length());
          LOGGER.debug("Found schema: " + schema);
          try (FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
               VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
               // TODO json dictionaries
               ArrowFileWriter arrowWriter = new ArrowFileWriter(root, reader, fileOutputStream
                   .getChannel())) {
            arrowWriter.start();
            while (reader.read(root)) {
              arrowWriter.writeBatch();
            }
            arrowWriter.end();
          }
          LOGGER.debug("Output file size: " + arrowFile.length());
        }
      }
    },
    VALIDATE(true, true) {
      @Override
      public void execute(File arrowFile, File jsonFile) throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             JsonFileReader jsonReader = new JsonFileReader(jsonFile, allocator);
             FileInputStream fileInputStream = new FileInputStream(arrowFile);
             ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(),
                 allocator)) {
          Schema jsonSchema = jsonReader.start();
          VectorSchemaRoot arrowRoot = arrowReader.getVectorSchemaRoot();
          Schema arrowSchema = arrowRoot.getSchema();
          LOGGER.debug("Arrow Input file size: " + arrowFile.length());
          LOGGER.debug("ARROW schema: " + arrowSchema);
          LOGGER.debug("JSON Input file size: " + jsonFile.length());
          LOGGER.debug("JSON schema: " + jsonSchema);
          Validator.compareSchemas(jsonSchema, arrowSchema);

          List<ArrowBlock> recordBatches = arrowReader.getRecordBlocks();
          Iterator<ArrowBlock> iterator = recordBatches.iterator();
          VectorSchemaRoot jsonRoot;
          int totalBatches = 0;
          while ((jsonRoot = jsonReader.read()) != null && iterator.hasNext()) {
            ArrowBlock rbBlock = iterator.next();
            if (!arrowReader.loadRecordBatch(rbBlock)) {
              throw new IOException("Expected to load record batch");
            }
            Validator.compareVectorSchemaRoot(arrowRoot, jsonRoot);
            jsonRoot.close();
            totalBatches++;
          }

          // Validate Dictionaries after ArrowFileReader has read batches
          List<DictionaryEncoding> encodingsJson = new ArrayList<>();
          extractDictionaryEncodings(jsonSchema.getFields(), encodingsJson);
          List<DictionaryEncoding> encodingsArrow = new ArrayList<>();
          extractDictionaryEncodings(arrowSchema.getFields(), encodingsArrow);
          Validator.compareDictionaries(encodingsJson, encodingsArrow, jsonReader, arrowReader);

          boolean hasMoreJSON = jsonRoot != null;
          boolean hasMoreArrow = iterator.hasNext();
          if (hasMoreJSON || hasMoreArrow) {
            throw new IllegalArgumentException("Unexpected RecordBatches. Total: " + totalBatches +
                " J:" + hasMoreJSON + " " +
                "A:" + hasMoreArrow);
          }
        }
      }
    };

    public final boolean arrowExists;
    public final boolean jsonExists;

    Command(boolean arrowExists, boolean jsonExists) {
      this.arrowExists = arrowExists;
      this.jsonExists = jsonExists;
    }

    abstract public void execute(File arrowFile, File jsonFile) throws IOException;

  }

}
