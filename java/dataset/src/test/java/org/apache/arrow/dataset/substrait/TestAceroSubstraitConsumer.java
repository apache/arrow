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

package org.apache.arrow.dataset.substrait;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAceroSubstraitConsumer extends TestDataset {
  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  @Test
  public void testRunQueryLocalFiles() throws Exception {
    // Query: SELECT * from nation
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("FieldPath(0)", new ArrowType.Int(64, true)),
        Field.nullable("FieldPath(1)", new ArrowType.FixedSizeBinary(25)),
        Field.nullable("FieldPath(2)", new ArrowType.Int(64, true)),
        Field.nullable("FieldPath(3)", new ArrowType.Utf8())
    ));
    try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator())
        .runQuery(
            planReplaceLocalFileURI(
                localTableJsonPlan,
                getNamedTableUri("nation.parquet")
            ),
            Collections.EMPTY_MAP
        )
    ) {
      assertEquals(schema.toString(), arrowReader.getVectorSchemaRoot().getSchema().toString());
      while (arrowReader.loadNextBatch()) {
        assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
      }
    }
  }

  @Test
  public void testRunQueryNamedTableNation() throws Exception {
    // Query: SELECT * from nation
    final Schema schema = new Schema(Arrays.asList(
        Field.nullable("FieldPath(0)", new ArrowType.Int(32, true)),
        Field.nullable("FieldPath(1)", new ArrowType.Utf8()),
        Field.nullable("FieldPath(2)", new ArrowType.Int(32, true)),
        Field.nullable("FieldPath(3)", new ArrowType.Utf8())
    ));
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, getNamedTableUri("nation.parquet"));
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", reader);
      try (ArrowReader arrowReader = new AceroSubstraitConsumer(rootAllocator()).runQuery(
          namedTableJsonPlan,
          mapTableToArrowReader
      )) {
        assertEquals(schema.toString(), arrowReader.getVectorSchemaRoot().getSchema().toString());
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("MOROCCO"));
        }
      }
    }
  }

  private static String planReplaceLocalFileURI(String plan, String uri) {
    StringBuilder builder = new StringBuilder(plan);
    builder.replace(builder.indexOf("FILENAME_PLACEHOLDER"),
        builder.indexOf("FILENAME_PLACEHOLDER") + "FILENAME_PLACEHOLDER".length(), uri);
    return builder.toString();
  }

  final String localTableJsonPlan = "" +
      "{\n" +
      "  \"extensionUris\": [],\n" +
      "  \"extensions\": [],\n" +
      "  \"relations\": [{\n" +
      "    \"root\": {\n" +
      "      \"input\": {\n" +
      "        \"project\": {\n" +
      "          \"common\": {\n" +
      "            \"emit\": {\n" +
      "              \"outputMapping\": [4, 5, 6, 7]\n" +
      "            }\n" +
      "          },\n" +
      "          \"input\": {\n" +
      "            \"read\": {\n" +
      "              \"common\": {\n" +
      "                \"direct\": {\n" +
      "                }\n" +
      "              },\n" +
      "              \"baseSchema\": {\n" +
      "                \"names\": [\"N_NATIONKEY\", \"N_NAME\", \"N_REGIONKEY\", \"N_COMMENT\"],\n" +
      "                \"struct\": {\n" +
      "                  \"types\": [{\n" +
      "                    \"i64\": {\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_REQUIRED\"\n" +
      "                    }\n" +
      "                  }, {\n" +
      "                    \"fixedChar\": {\n" +
      "                      \"length\": 25,\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_NULLABLE\"\n" +
      "                    }\n" +
      "                  }, {\n" +
      "                    \"i64\": {\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_REQUIRED\"\n" +
      "                    }\n" +
      "                  }, {\n" +
      "                    \"varchar\": {\n" +
      "                      \"length\": 152,\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_NULLABLE\"\n" +
      "                    }\n" +
      "                  }],\n" +
      "                  \"typeVariationReference\": 0,\n" +
      "                  \"nullability\": \"NULLABILITY_REQUIRED\"\n" +
      "                }\n" +
      "              },\n" +
      "              \"local_files\": {\n" +
      "                \"items\": [\n" +
      "                  {\n" +
      "                    \"uri_file\": \"FILENAME_PLACEHOLDER\",\n" +
      "                    \"parquet\": {}\n" +
      "                  }\n" +
      "                ]\n" +
      "              }\n" +
      "            }\n" +
      "          },\n" +
      "          \"expressions\": [{\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 0\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 1\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 2\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 3\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }]\n" +
      "        }\n" +
      "      },\n" +
      "      \"names\": [\"N_NATIONKEY\", \"N_NAME\", \"N_REGIONKEY\", \"N_COMMENT\"]\n" +
      "    }\n" +
      "  }],\n" +
      "  \"expectedTypeUrls\": []\n" +
      "}" +
      "";
  
  final String namedTableJsonPlan = "" +
      "{\n" +
      "  \"extensionUris\": [],\n" +
      "  \"extensions\": [],\n" +
      "  \"relations\": [{\n" +
      "    \"root\": {\n" +
      "      \"input\": {\n" +
      "        \"project\": {\n" +
      "          \"common\": {\n" +
      "            \"emit\": {\n" +
      "              \"outputMapping\": [4, 5, 6, 7]\n" +
      "            }\n" +
      "          },\n" +
      "          \"input\": {\n" +
      "            \"read\": {\n" +
      "              \"common\": {\n" +
      "                \"direct\": {\n" +
      "                }\n" +
      "              },\n" +
      "              \"baseSchema\": {\n" +
      "                \"names\": [\"N_NATIONKEY\", \"N_NAME\", \"N_REGIONKEY\", \"N_COMMENT\"],\n" +
      "                \"struct\": {\n" +
      "                  \"types\": [{\n" +
      "                    \"i64\": {\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_REQUIRED\"\n" +
      "                    }\n" +
      "                  }, {\n" +
      "                    \"fixedChar\": {\n" +
      "                      \"length\": 25,\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_NULLABLE\"\n" +
      "                    }\n" +
      "                  }, {\n" +
      "                    \"i64\": {\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_REQUIRED\"\n" +
      "                    }\n" +
      "                  }, {\n" +
      "                    \"varchar\": {\n" +
      "                      \"length\": 152,\n" +
      "                      \"typeVariationReference\": 0,\n" +
      "                      \"nullability\": \"NULLABILITY_NULLABLE\"\n" +
      "                    }\n" +
      "                  }],\n" +
      "                  \"typeVariationReference\": 0,\n" +
      "                  \"nullability\": \"NULLABILITY_REQUIRED\"\n" +
      "                }\n" +
      "              },\n" +
      "              \"namedTable\": {\n" +
      "                \"names\": [\"NATION\"]\n" +
      "              }\n" +
      "            }\n" +
      "          },\n" +
      "          \"expressions\": [{\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 0\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 1\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 2\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"selection\": {\n" +
      "              \"directReference\": {\n" +
      "                \"structField\": {\n" +
      "                  \"field\": 3\n" +
      "                }\n" +
      "              },\n" +
      "              \"rootReference\": {\n" +
      "              }\n" +
      "            }\n" +
      "          }]\n" +
      "        }\n" +
      "      },\n" +
      "      \"names\": [\"N_NATIONKEY\", \"N_NAME\", \"N_REGIONKEY\", \"N_COMMENT\"]\n" +
      "    }\n" +
      "  }],\n" +
      "  \"expectedTypeUrls\": []\n" +
      "}" +
      "";
}
