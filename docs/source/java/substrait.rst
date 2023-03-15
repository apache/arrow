.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

=========
Substrait
=========

Cross-Language Serialization for Relational Algebra.

`Substrait`_ is a well-defined, cross-language specification for data compute operations.

.. contents::

Getting Started
===============

There are two perspectives among substrait implementors:

- Substrait Consumer
- Substrait Producer

Substrait is calling Acero thru JNI wrappers to Consume Substrait plans.

.. seealso:: :doc:`../cpp/streaming_execution` for more information on Acero.


Substrait Consumer
==================

Substrait Plan offer two ways to define URI for Query:

- Local Files: A fixed URI value on the plan
- Named Table: An external configuration to define URI value

Local Files:

.. code-block:: json

    "local_files": {
      "items": [
        {
          "uri_file": "file:///tmp/opt/lineitem.parquet",
          "parquet": {}
        }
      ]
    }

Named Table:

.. code-block:: json

    "namedTable": {
        "names": ["LINEITEM"]
    }

Below shows a simplest example of using Substrait to query a Parquet file in Java:


Substrait Consumer For Binary Plan
----------------------------------

Next examples uses `Isthmus`_ library to create Substrait binary plan.

Substrait Consumer For Named Table
++++++++++++++++++++++++++++++++++

Query one table. Nation TPCH table:

.. code-block:: Java

    // Query: SELECT * from nation
    String uri = "file:///data/tpch_parquet/nation.parquet";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", reader);
      // get binary plan
      String sql = "SELECT * from nation";
      String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
          "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
      Plan plan = getPlan(sql, ImmutableList.of(nation));
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
      substraitPlan.put(plan.toByteArray());
      // run query
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          substraitPlan,
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 25);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("MOROCCO"));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

.. code-block:: text

    // Results example:
    FieldPath(0)	FieldPath(1)	FieldPath(2)	FieldPath(3)
    0	ALGERIA	0	 haggle. carefully final deposits detect slyly agai
    1	ARGENTINA	1	al foxes promise slyly according to the regular accounts. bold requests alon

Query two tables. Nation and Customer TPCH tables:

.. code-block:: Java

    String uriNation = "file:///data/tpch_parquet/nation.parquet";
    String uriCustomer = "file:///data/tpch_parquet/customer.parquet";
    ScanOptions optionsNations = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsCustomer = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(),
            FileFormat.PARQUET, uriNation);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsNations);
        ArrowReader readerNation = scanner.scanBatches();
        DatasetFactory datasetFactoryCustomer = new FileSystemDatasetFactory(rootAllocator(),
            NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriCustomer);
        Dataset datasetCustomer = datasetFactoryCustomer.finish();
        Scanner scannerCustomer = datasetCustomer.newScan(optionsCustomer);
        ArrowReader readerCustomer = scannerCustomer.scanBatches()
    ) {
      // map table to reader
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("CUSTOMER", readerCustomer);
      // get binary plan
      String sql = "SELECT n.n_name, c.c_name, c.c_phone, c.c_address FROM nation n JOIN customer c " +
          "ON n.n_nationkey = c.c_nationkey";
      String nation = "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), " +
          "N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))";
      String customer = "CREATE TABLE CUSTOMER (C_CUSTKEY BIGINT NOT NULL, C_NAME VARCHAR(25), " +
          "C_ADDRESS VARCHAR(40), C_NATIONKEY BIGINT NOT NULL, C_PHONE CHAR(15), C_ACCTBAL DECIMAL, " +
          "C_MKTSEGMENT CHAR(10), C_COMMENT VARCHAR(117) )";
      Plan plan = getPlan(sql, ImmutableList.of(nation, customer));
      ByteBuffer substraitPlan = ByteBuffer.allocateDirect(plan.toByteArray().length);
      substraitPlan.put(plan.toByteArray());
      // run query
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          substraitPlan,
          mapTableToArrowReader
      )) {
        while (arrowReader.loadNextBatch()) {
          assertEquals(arrowReader.getVectorSchemaRoot().getRowCount(), 15000);
          assertTrue(arrowReader.getVectorSchemaRoot().contentToTSVString().contains("Customer#000014924"));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

Substrait Consumer For JSON Plan
--------------------------------

In the following example, we use plain text Substrait plans to illustrate the process.
For production environments, it is recommended to use binary plans.

Substrait Consumer for Local Files
++++++++++++++++++++++++++++++++++

.. code-block:: Java

    String substraitPlan = "{...json Substrait plan...}";
    try (ArrowReader reader = new SubstraitConsumer(rootAllocator()).
                                  runQueryLocalFiles(
                                    substraitPlan
                                  )
    ) {
      while (reader.loadNextBatch()) {
        System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
      }
    }


Substrait Consumer For Named Table
++++++++++++++++++++++++++++++++++

Query one table. Nation TPCH table:

.. code-block:: Java

    // Query: SELECT * from nation
    String uri = "file:///data/tpch_parquet/nation.parquet";
    String substraitPlan = "{...json Substrait plan...}";
    ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches()
    ) {
      Map<String, ArrowReader> mapReaderToTable = new HashMap<>();
      mapReaderToTable.put("NATION", reader);
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          substraitPlan,
          mapReaderToTable
      )){
        while(arrowReader.loadNextBatch()){
          System.out.println(arrowReader.getVectorSchemaRoot().contentToTSVString());
          System.out.println(arrowReader.getVectorSchemaRoot().getRowCount());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

.. code-block:: text

    // Results example:
    FieldPath(0)	FieldPath(1)	FieldPath(2)	FieldPath(3)
    0	ALGERIA	0	 haggle. carefully final deposits detect slyly agai
    1	ARGENTINA	1	al foxes promise slyly according to the regular accounts. bold requests alon

Query two tables. Nation and Customer TPCH tables:

.. code-block:: Java

    // Query: SELECT n.n_name, c.c_name, c.c_phone, c.c_address FROM nation n JOIN customer c ON n.n_nationkey = c.c_nationkey (defined below)
    String uriNation = "file:///data/tpch_parquet/nation.parquet";
    String uriCustomer = "file:///data/tpch_parquet/customer.parquet";
    String substraitPlan = "{...json Substrait plan...}";
    ScanOptions optionsNations = new ScanOptions(/*batchSize*/ 32768);
    ScanOptions optionsCustomer = new ScanOptions(/*batchSize*/ 32768);
    try (
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriNation);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(optionsNations);
        ArrowReader readerNation = scanner.scanBatches();
        DatasetFactory datasetFactoryCustomer = new FileSystemDatasetFactory(rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, uriCustomer);
        Dataset datasetCustomer = datasetFactoryCustomer.finish();
        Scanner scannerCustomer = datasetCustomer.newScan(optionsCustomer);
        ArrowReader readerCustomer = scannerCustomer.scanBatches();
    ) {
      Map<String, ArrowReader> mapTableToArrowReader = new HashMap<>();
      mapTableToArrowReader.put("NATION", readerNation);
      mapTableToArrowReader.put("CUSTOMER", readerCustomer);
      try (ArrowReader arrowReader = new SubstraitConsumer(rootAllocator()).runQueryNamedTables(
          substraitPlan,
          mapTableToArrowReader
      )){
        while(arrowReader.loadNextBatch()){
          System.out.println(arrowReader.getVectorSchemaRoot().contentToTSVString());
          System.out.println(arrowReader.getVectorSchemaRoot().getRowCount());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

.. code-block:: text

    // Results example:
    FieldPath(1)	FieldPath(5)	FieldPath(8)	FieldPath(6)
    ALGERIA	Customer#000014977	10-901-414-3869	ARaV3SU4TwhxUf
    ALGERIA	Customer#000014975	10-318-218-3381	BzV ELDsdtukkrRf5fQ

Substrait Producer
==================

Let start producing Substrait plan by `Java Substrait`_ thru `Isthmus`_ by
CLI command:

.. code-block:: sql

    # schema.sql
    CREATE TABLE CUSTOMER (
      C_CUSTKEY BIGINT NOT NULL,
      C_NAME VARCHAR(25),
      C_ADDRESS VARCHAR(40),
      C_NATIONKEY BIGINT NOT NULL,
      C_PHONE CHAR(15),
      C_ACCTBAL DECIMAL,
      C_MKTSEGMENT CHAR(10),
      C_COMMENT VARCHAR(117)
    );
    CREATE TABLE NATION (
      N_NATIONKEY BIGINT NOT NULL,
      N_NAME CHAR(25),
      N_REGIONKEY BIGINT NOT NULL,
      N_COMMENT VARCHAR(152)
    );

.. code-block:: bash

    # define Schema DDL, Query, download Isthmus and run CLI command.
    DDL=`cat schema.sql`
    QUERY="SELECT n.n_name, c.c_name, c.c_phone, c.c_address FROM nation n JOIN customer c ON n.n_nationkey = c.c_nationkey"
    ./isthmus-macOS-0.6.0 "${QUERY}" --create "${DDL}"

Then, the following Susbtrait Plan will be generated:

.. code-block:: json

    {
      "extensionUris": [{
        "extensionUriAnchor": 1,
        "uri": "/functions_comparison.yaml"
      }],
      "extensions": [{
        "extensionFunction": {
          "extensionUriReference": 1,
          "functionAnchor": 0,
          "name": "equal:any_any"
        }
      }],
      "relations": [{
        "root": {
          "input": {
            "project": {
              "common": {
                "emit": {
                  "outputMapping": [12, 13, 14, 15]
                }
              },
              "input": {
                "join": {
                  "common": {
                    "direct": {
                    }
                  },
                  "left": {
                    "read": {
                      "common": {
                        "direct": {
                        }
                      },
                      "baseSchema": {
                        "names": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"],
                        "struct": {
                          "types": [{
                            "i64": {
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_REQUIRED"
                            }
                          }, {
                            "fixedChar": {
                              "length": 25,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }, {
                            "i64": {
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_REQUIRED"
                            }
                          }, {
                            "varchar": {
                              "length": 152,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }],
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_REQUIRED"
                        }
                      },
                      "namedTable": {
                        "names": ["NATION"]
                      }
                    }
                  },
                  "right": {
                    "read": {
                      "common": {
                        "direct": {
                        }
                      },
                      "baseSchema": {
                        "names": ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"],
                        "struct": {
                          "types": [{
                            "i64": {
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_REQUIRED"
                            }
                          }, {
                            "varchar": {
                              "length": 25,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }, {
                            "varchar": {
                              "length": 40,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }, {
                            "i64": {
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_REQUIRED"
                            }
                          }, {
                            "fixedChar": {
                              "length": 15,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }, {
                            "decimal": {
                              "scale": 0,
                              "precision": 19,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }, {
                            "fixedChar": {
                              "length": 10,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }, {
                            "varchar": {
                              "length": 117,
                              "typeVariationReference": 0,
                              "nullability": "NULLABILITY_NULLABLE"
                            }
                          }],
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_REQUIRED"
                        }
                      },
                      "namedTable": {
                        "names": ["CUSTOMER"]
                      }
                    }
                  },
                  "expression": {
                    "scalarFunction": {
                      "functionReference": 0,
                      "args": [],
                      "outputType": {
                        "bool": {
                          "typeVariationReference": 0,
                          "nullability": "NULLABILITY_REQUIRED"
                        }
                      },
                      "arguments": [{
                        "value": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 0
                              }
                            },
                            "rootReference": {
                            }
                          }
                        }
                      }, {
                        "value": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 7
                              }
                            },
                            "rootReference": {
                            }
                          }
                        }
                      }]
                    }
                  },
                  "type": "JOIN_TYPE_INNER"
                }
              },
              "expressions": [{
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 1
                    }
                  },
                  "rootReference": {
                  }
                }
              }, {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 5
                    }
                  },
                  "rootReference": {
                  }
                }
              }, {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 8
                    }
                  },
                  "rootReference": {
                  }
                }
              }, {
                "selection": {
                  "directReference": {
                    "structField": {
                      "field": 6
                    }
                  },
                  "rootReference": {
                  }
                }
              }]
            }
          },
          "names": ["N_NAME", "C_NAME", "C_PHONE", "C_ADDRESS"]
        }
      }],
      "expectedTypeUrls": []
    }

.. _`Substrait`: https://substrait.io/
.. _`Java Substrait`: https://github.com/substrait-io/substrait-java
.. _`Isthmus`: https://github.com/substrait-io/substrait-java/releases