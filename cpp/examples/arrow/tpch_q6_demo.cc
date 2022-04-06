// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/engine/api.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"

namespace eng = arrow::engine;
namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

arrow::Future<std::shared_ptr<arrow::Buffer>> GetSubstraitPlanStage0() {
  std::string substrait_json = R"({
    "extensions": [
      {
        "extensionFunction": {
          "functionAnchor": 6,
          "name": "lte:fp64_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 9,
          "name": "sum:opt_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 4,
          "name": "lt:date_date"
        }
      },
      {
        "extensionFunction": {
          "name": "is_not_null:date"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 3,
          "name": "gte:date_date"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 7,
          "name": "lt:fp64_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 1,
          "name": "is_not_null:fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 2,
          "name": "and:bool_bool"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 5,
          "name": "gte:fp64_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 8,
          "name": "multiply:opt_fp64_fp64"
        }
      }
    ],
    "relations": [
      {
        "root": {
          "input": {
            "aggregate": {
              "common": {
                "direct": {}
              },
              "input": {
                "project": {
                  "common": {
                    "direct": {}
                  },
                  "input": {
                    "project": {
                      "common": {
                        "direct": {}
                      },
                      "input": {
                        "filter": {
                          "common": {
                            "direct": {}
                          },
                          "input": {
                            "read": {
                              "common": {
                                "direct": {}
                              },
                              "baseSchema": {
                                "names": [
                                  "l_quantity",
                                  "l_extendedprice",
                                  "l_discount",
                                  "l_shipdate"
                                ],
                                "struct": {
                                  "types": [
                                    {
                                      "fp64": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    },
                                    {
                                      "fp64": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    },
                                    {
                                      "fp64": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    },
                                    {
                                      "date": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    }
                                  ]
                                }
                              },
                              "filter": {
                                "scalarFunction": {
                                  "functionReference": 2,
                                  "args": [
                                    {
                                      "scalarFunction": {
                                        "functionReference": 2,
                                        "args": [
                                          {
                                            "scalarFunction": {
                                              "functionReference": 2,
                                              "args": [
                                                {
                                                  "scalarFunction": {
                                                    "functionReference": 2,
                                                    "args": [
                                                      {
                                                        "scalarFunction": {
                                                          "functionReference": 2,
                                                          "args": [
                                                            {
                                                              "scalarFunction": {
                                                                "functionReference": 2,
                                                                "args": [
                                                                  {
                                                                    "scalarFunction": {
                                                                      "functionReference": 2,
                                                                      "args": [
                                                                        {
                                                                          "scalarFunction": {
                                                                            "args": [
                                                                              {
                                                                                "selection": {
                                                                                  "directReference": {
                                                                                    "structField": {
                                                                                      "field": 3
                                                                                    }
                                                                                  }
                                                                                }
                                                                              }
                                                                            ],
                                                                            "outputType": {
                                                                              "bool": {
                                                                                "nullability": "NULLABILITY_NULLABLE"
                                                                              }
                                                                            }
                                                                          }
                                                                        },
                                                                        {
                                                                          "scalarFunction": {
                                                                            "functionReference": 1,
                                                                            "args": [
                                                                              {
                                                                                "selection": {
                                                                                  "directReference": {
                                                                                    "structField": {
                                                                                      "field": 2
                                                                                    }
                                                                                  }
                                                                                }
                                                                              }
                                                                            ],
                                                                            "outputType": {
                                                                              "bool": {
                                                                                "nullability": "NULLABILITY_NULLABLE"
                                                                              }
                                                                            }
                                                                          }
                                                                        }
                                                                      ],
                                                                      "outputType": {
                                                                        "bool": {
                                                                          "nullability": "NULLABILITY_NULLABLE"
                                                                        }
                                                                      }
                                                                    }
                                                                  },
                                                                  {
                                                                    "scalarFunction": {
                                                                      "functionReference": 1,
                                                                      "args": [
                                                                        {
                                                                          "selection": {
                                                                            "directReference": {
                                                                              "structField": {}
                                                                            }
                                                                          }
                                                                        }
                                                                      ],
                                                                      "outputType": {
                                                                        "bool": {
                                                                          "nullability": "NULLABILITY_NULLABLE"
                                                                        }
                                                                      }
                                                                    }
                                                                  }
                                                                ],
                                                                "outputType": {
                                                                  "bool": {
                                                                    "nullability": "NULLABILITY_NULLABLE"
                                                                  }
                                                                }
                                                              }
                                                            },
                                                            {
                                                              "scalarFunction": {
                                                                "functionReference": 3,
                                                                "args": [
                                                                  {
                                                                    "selection": {
                                                                      "directReference": {
                                                                        "structField": {
                                                                          "field": 3
                                                                        }
                                                                      }
                                                                    }
                                                                  },
                                                                  {
                                                                    "literal": {
                                                                      "date": 8766
                                                                    }
                                                                  }
                                                                ],
                                                                "outputType": {
                                                                  "bool": {
                                                                    "nullability": "NULLABILITY_NULLABLE"
                                                                  }
                                                                }
                                                              }
                                                            }
                                                          ],
                                                          "outputType": {
                                                            "bool": {
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }
                                                        }
                                                      },
                                                      {
                                                        "scalarFunction": {
                                                          "functionReference": 4,
                                                          "args": [
                                                            {
                                                              "selection": {
                                                                "directReference": {
                                                                  "structField": {
                                                                    "field": 3
                                                                  }
                                                                }
                                                              }
                                                            },
                                                            {
                                                              "literal": {
                                                                "date": 9131
                                                              }
                                                            }
                                                          ],
                                                          "outputType": {
                                                            "bool": {
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }
                                                        }
                                                      }
                                                    ],
                                                    "outputType": {
                                                      "bool": {
                                                        "nullability": "NULLABILITY_NULLABLE"
                                                      }
                                                    }
                                                  }
                                                },
                                                {
                                                  "scalarFunction": {
                                                    "functionReference": 5,
                                                    "args": [
                                                      {
                                                        "selection": {
                                                          "directReference": {
                                                            "structField": {
                                                              "field": 2
                                                            }
                                                          }
                                                        }
                                                      },
                                                      {
                                                        "literal": {
                                                          "fp64": 0.05
                                                        }
                                                      }
                                                    ],
                                                    "outputType": {
                                                      "bool": {
                                                        "nullability": "NULLABILITY_NULLABLE"
                                                      }
                                                    }
                                                  }
                                                }
                                              ],
                                              "outputType": {
                                                "bool": {
                                                  "nullability": "NULLABILITY_NULLABLE"
                                                }
                                              }
                                            }
                                          },
                                          {
                                            "scalarFunction": {
                                              "functionReference": 6,
                                              "args": [
                                                {
                                                  "selection": {
                                                    "directReference": {
                                                      "structField": {
                                                        "field": 2
                                                      }
                                                    }
                                                  }
                                                },
                                                {
                                                  "literal": {
                                                    "fp64": 0.07
                                                  }
                                                }
                                              ],
                                              "outputType": {
                                                "bool": {
                                                  "nullability": "NULLABILITY_NULLABLE"
                                                }
                                              }
                                            }
                                          }
                                        ],
                                        "outputType": {
                                          "bool": {
                                            "nullability": "NULLABILITY_NULLABLE"
                                          }
                                        }
                                      }
                                    },
                                    {
                                      "scalarFunction": {
                                        "functionReference": 7,
                                        "args": [
                                          {
                                            "selection": {
                                              "directReference": {
                                                "structField": {}
                                              }
                                            }
                                          },
                                          {
                                            "literal": {
                                              "fp64": 24
                                            }
                                          }
                                        ],
                                        "outputType": {
                                          "bool": {
                                            "nullability": "NULLABILITY_NULLABLE"
                                          }
                                        }
                                      }
                                    }
                                  ],
                                  "outputType": {
                                    "bool": {
                                      "nullability": "NULLABILITY_NULLABLE"
                                    }
                                  }
                                }
                              },
                              "localFiles": {
                                "items": [
                                  {
                                    "uriFile": "file:///home/rong/benchmark/data/tpch1_d4d/lineitem/part-00000-678b839d-2286-4750-923f-653adc66ed74-c000.snappy.parquet",
                                    "format": "FILE_FORMAT_PARQUET",
                                    "length": "206349871"
                                  }
                                ]
                              }
                            }
                          },
                          "condition": {
                            "scalarFunction": {
                              "functionReference": 2,
                              "args": [
                                {
                                  "scalarFunction": {
                                    "functionReference": 2,
                                    "args": [
                                      {
                                        "scalarFunction": {
                                          "functionReference": 2,
                                          "args": [
                                            {
                                              "scalarFunction": {
                                                "functionReference": 2,
                                                "args": [
                                                  {
                                                    "scalarFunction": {
                                                      "functionReference": 2,
                                                      "args": [
                                                        {
                                                          "scalarFunction": {
                                                            "functionReference": 2,
                                                            "args": [
                                                              {
                                                                "scalarFunction": {
                                                                  "functionReference": 2,
                                                                  "args": [
                                                                    {
                                                                      "scalarFunction": {
                                                                        "args": [
                                                                          {
                                                                            "selection": {
                                                                              "directReference": {
                                                                                "structField": {
                                                                                  "field": 3
                                                                                }
                                                                              }
                                                                            }
                                                                          }
                                                                        ],
                                                                        "outputType": {
                                                                          "bool": {
                                                                            "nullability": "NULLABILITY_NULLABLE"
                                                                          }
                                                                        }
                                                                      }
                                                                    },
                                                                    {
                                                                      "scalarFunction": {
                                                                        "functionReference": 1,
                                                                        "args": [
                                                                          {
                                                                            "selection": {
                                                                              "directReference": {
                                                                                "structField": {
                                                                                  "field": 2
                                                                                }
                                                                              }
                                                                            }
                                                                          }
                                                                        ],
                                                                        "outputType": {
                                                                          "bool": {
                                                                            "nullability": "NULLABILITY_NULLABLE"
                                                                          }
                                                                        }
                                                                      }
                                                                    }
                                                                  ],
                                                                  "outputType": {
                                                                    "bool": {
                                                                      "nullability": "NULLABILITY_NULLABLE"
                                                                    }
                                                                  }
                                                                }
                                                              },
                                                              {
                                                                "scalarFunction": {
                                                                  "functionReference": 1,
                                                                  "args": [
                                                                    {
                                                                      "selection": {
                                                                        "directReference": {
                                                                          "structField": {}
                                                                        }
                                                                      }
                                                                    }
                                                                  ],
                                                                  "outputType": {
                                                                    "bool": {
                                                                      "nullability": "NULLABILITY_NULLABLE"
                                                                    }
                                                                  }
                                                                }
                                                              }
                                                            ],
                                                            "outputType": {
                                                              "bool": {
                                                                "nullability": "NULLABILITY_NULLABLE"
                                                              }
                                                            }
                                                          }
                                                        },
                                                        {
                                                          "scalarFunction": {
                                                            "functionReference": 3,
                                                            "args": [
                                                              {
                                                                "selection": {
                                                                  "directReference": {
                                                                    "structField": {
                                                                      "field": 3
                                                                    }
                                                                  }
                                                                }
                                                              },
                                                              {
                                                                "literal": {
                                                                  "date": 8766
                                                                }
                                                              }
                                                            ],
                                                            "outputType": {
                                                              "bool": {
                                                                "nullability": "NULLABILITY_NULLABLE"
                                                              }
                                                            }
                                                          }
                                                        }
                                                      ],
                                                      "outputType": {
                                                        "bool": {
                                                          "nullability": "NULLABILITY_NULLABLE"
                                                        }
                                                      }
                                                    }
                                                  },
                                                  {
                                                    "scalarFunction": {
                                                      "functionReference": 4,
                                                      "args": [
                                                        {
                                                          "selection": {
                                                            "directReference": {
                                                              "structField": {
                                                                "field": 3
                                                              }
                                                            }
                                                          }
                                                        },
                                                        {
                                                          "literal": {
                                                            "date": 9131
                                                          }
                                                        }
                                                      ],
                                                      "outputType": {
                                                        "bool": {
                                                          "nullability": "NULLABILITY_NULLABLE"
                                                        }
                                                      }
                                                    }
                                                  }
                                                ],
                                                "outputType": {
                                                  "bool": {
                                                    "nullability": "NULLABILITY_NULLABLE"
                                                  }
                                                }
                                              }
                                            },
                                            {
                                              "scalarFunction": {
                                                "functionReference": 5,
                                                "args": [
                                                  {
                                                    "selection": {
                                                      "directReference": {
                                                        "structField": {
                                                          "field": 2
                                                        }
                                                      }
                                                    }
                                                  },
                                                  {
                                                    "literal": {
                                                      "fp64": 0.05
                                                    }
                                                  }
                                                ],
                                                "outputType": {
                                                  "bool": {
                                                    "nullability": "NULLABILITY_NULLABLE"
                                                  }
                                                }
                                              }
                                            }
                                          ],
                                          "outputType": {
                                            "bool": {
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }
                                        }
                                      },
                                      {
                                        "scalarFunction": {
                                          "functionReference": 6,
                                          "args": [
                                            {
                                              "selection": {
                                                "directReference": {
                                                  "structField": {
                                                    "field": 2
                                                  }
                                                }
                                              }
                                            },
                                            {
                                              "literal": {
                                                "fp64": 0.07
                                              }
                                            }
                                          ],
                                          "outputType": {
                                            "bool": {
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }
                                        }
                                      }
                                    ],
                                    "outputType": {
                                      "bool": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    }
                                  }
                                },
                                {
                                  "scalarFunction": {
                                    "functionReference": 7,
                                    "args": [
                                      {
                                        "selection": {
                                          "directReference": {
                                            "structField": {}
                                          }
                                        }
                                      },
                                      {
                                        "literal": {
                                          "fp64": 24
                                        }
                                      }
                                    ],
                                    "outputType": {
                                      "bool": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    }
                                  }
                                }
                              ],
                              "outputType": {
                                "bool": {
                                  "nullability": "NULLABILITY_NULLABLE"
                                }
                              }
                            }
                          }
                        }
                      },
                      "expressions": [
                        {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 1
                              }
                            }
                          }
                        },
                        {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 2
                              }
                            }
                          }
                        }
                      ]
                    }
                  },
                  "expressions": [
                    {
                      "scalarFunction": {
                        "functionReference": 8,
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {}
                              }
                            }
                          },
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 1
                                }
                              }
                            }
                          }
                        ],
                        "outputType": {
                          "fp64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      }
                    }
                  ]
                }
              },
              "groupings": [
                {}
              ],
              "measures": [
                {
                  "measure": {
                    "functionReference": 9,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {}
                          }
                        }
                      }
                    ],
                    "phase": "AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE",
                    "outputType": {
                      "fp64": {
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }
                  }
                }
              ]
            }
          },
          "names": [
            "sum"
          ]
        }
      }
    ]
  })";
  return eng::internal::SubstraitFromJSON("Plan", substrait_json);
}

arrow::Future<std::shared_ptr<arrow::Buffer>> GetSubstraitPlanStage1() {
  std::string substrait_json = R"({
    "extensions": [
      {
        "extensionFunction": {
          "name": "sum:opt_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 1,
          "name": "alias:fp64"
        }
      }
    ],
    "relations": [
      {
        "root": {
          "input": {
            "project": {
              "common": {
                "direct": {}
              },
              "input": {
                "aggregate": {
                  "common": {
                    "direct": {}
                  },
                  "input": {
                    "read": {
                      "common": {
                        "direct": {}
                      },
                      "baseSchema": {
                        "names": [
                          "sum"
                        ],
                        "struct": {
                          "types": [
                            {
                              "fp64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            }
                          ]
                        }
                      },
                      "localFiles": {
                        "items": [
                          {
                            "uriFile": "iterator:0"
                          }
                        ]
                      }
                    }
                  },
                  "groupings": [
                    {}
                  ],
                  "measures": [
                    {
                      "measure": {
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {}
                              }
                            }
                          }
                        ],
                        "phase": "AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT",
                        "outputType": {
                          "fp64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      }
                    }
                  ]
                }
              },
              "expressions": [
                {
                  "scalarFunction": {
                    "functionReference": 1,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {}
                          }
                        }
                      }
                    ],
                    "outputType": {
                      "fp64": {
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }
                  }
                }
              ]
            }
          },
          "names": [
            "revenue"
          ]
        }
      }
    ]
  })";
  return eng::internal::SubstraitFromJSON("Plan", substrait_json);
}

void ReplaceSourceDecls(std::vector<arrow::compute::Declaration> source_decls,
                        arrow::compute::Declaration* decl) {
  std::vector<arrow::compute::Declaration*> visited;
  std::vector<arrow::compute::Declaration*> source_indexes;
  visited.push_back(decl);

  while (!visited.empty()) {
    auto top = visited.back();
    visited.pop_back();
    for (auto& input : top->inputs) {
      auto& input_decl = arrow::util::get<arrow::compute::Declaration>(input);
      if (input_decl.factory_name == "source_index") {
        source_indexes.push_back(&input_decl);
      } else {
        visited.push_back(&input_decl);
      }
    }
  }

  assert(source_indexes.size() == source_decls.size());
  for (auto& source_index : source_indexes) {
    auto index =
        arrow::internal::checked_pointer_cast<arrow::compute::SourceIndexOptions>(
            source_index->options)
            ->index;
    *source_index = std::move(source_decls[index]);
  }
}

int main(int argc, char** argv) {
  std::shared_ptr<arrow::Schema> input_schema;
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen;

  // ------------------------ Stage 0 -------------------------
  {
    auto maybe_serialized_plan = GetSubstraitPlanStage0().result();
    ABORT_ON_FAILURE(maybe_serialized_plan.status());
    std::shared_ptr<arrow::Buffer> serialized_plan =
        std::move(maybe_serialized_plan).ValueOrDie();

    // Print the received plan to stdout as JSON
    arrow::Result<std::string> maybe_plan_json =
        eng::internal::SubstraitToJSON("Plan", *serialized_plan);
    ABORT_ON_FAILURE(maybe_plan_json.status());
    std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
    std::cout << maybe_plan_json.ValueOrDie() << std::endl;

    // Deserialize each relation tree in the substrait plan to an Arrow compute
    // Declaration
    arrow::Result<std::vector<cp::Declaration>> maybe_decls =
        eng::DeserializePlan(*serialized_plan);
    ABORT_ON_FAILURE(maybe_decls.status());
    std::vector<cp::Declaration> decls = std::move(maybe_decls).ValueOrDie();

    // It's safe to drop the serialized plan; we don't leave references to its memory
    serialized_plan.reset();

    auto& decl = decls[0];

    // Construct an empty plan (note: configure Function registry and ThreadPool here)
    arrow::Result<std::shared_ptr<cp::ExecPlan>> maybe_plan = cp::ExecPlan::Make();
    ABORT_ON_FAILURE(maybe_plan.status());
    std::shared_ptr<cp::ExecPlan> plan = std::move(maybe_plan).ValueOrDie();

    auto* node = decl.AddToPlan(plan.get()).ValueOrDie();
    std::cout << "schema: " << node->output_schema()->ToString() << std::endl;

    input_schema = node->output_schema();

    // add sink node
    arrow::compute::MakeExecNode("sink", plan.get(), {node},
                                 arrow::compute::SinkNodeOptions{&gen});

    // Validate the plan and print it to stdout
    ABORT_ON_FAILURE(plan->Validate());
    std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
    std::cout << plan->ToString() << std::endl;

    // Start the plan...
    std::cout << std::string(50, '#') << " consuming batches:" << std::endl;
    ABORT_ON_FAILURE(plan->StartProducing());
    ABORT_ON_FAILURE(plan->finished().status());
  }

  // ------------------------ Stage 1 -------------------------

  {
    auto maybe_serialized_plan = GetSubstraitPlanStage1().result();
    ABORT_ON_FAILURE(maybe_serialized_plan.status());
    std::shared_ptr<arrow::Buffer> serialized_plan =
        std::move(maybe_serialized_plan).ValueOrDie();

    // Print the received plan to stdout as JSON
    arrow::Result<std::string> maybe_plan_json =
        eng::internal::SubstraitToJSON("Plan", *serialized_plan);
    ABORT_ON_FAILURE(maybe_plan_json.status());
    std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
    std::cout << maybe_plan_json.ValueOrDie() << std::endl;

    // Deserialize each relation tree in the substrait plan to an Arrow compute
    // Declaration
    arrow::Result<std::vector<cp::Declaration>> maybe_decls =
        eng::DeserializePlan(*serialized_plan);
    ABORT_ON_FAILURE(maybe_decls.status());
    std::vector<cp::Declaration> decls = std::move(maybe_decls).ValueOrDie();

    // It's safe to drop the serialized plan; we don't leave references to its memory
    serialized_plan.reset();

    auto& decl = decls[0];
    // Add decls to plan (note: configure ExecNode registry before this point)
    // Add source node
    std::vector<cp::Declaration> source = {
        arrow::compute::Declaration{"source", cp::SourceNodeOptions{input_schema, gen}}};
    ReplaceSourceDecls(std::move(source), &decl);

    // Construct an empty plan (note: configure Function registry and ThreadPool here)
    arrow::Result<std::shared_ptr<cp::ExecPlan>> maybe_plan = cp::ExecPlan::Make();
    ABORT_ON_FAILURE(maybe_plan.status());
    std::shared_ptr<cp::ExecPlan> plan = std::move(maybe_plan).ValueOrDie();

    auto* node = decl.AddToPlan(plan.get()).ValueOrDie();
    std::cout << "schema: " << node->output_schema()->ToString() << std::endl;

    auto output_schema = node->output_schema();

    // add sink node
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen;
    arrow::compute::MakeExecNode("sink", plan.get(), {node},
                                 arrow::compute::SinkNodeOptions{&sink_gen});

    // Validate the plan and print it to stdout
    ABORT_ON_FAILURE(plan->Validate());
    std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
    std::cout << plan->ToString() << std::endl;

    // Start the plan...
    std::cout << std::string(50, '#') << " consuming batches:" << std::endl;
    ABORT_ON_FAILURE(plan->StartProducing());

    std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
        std::move(output_schema), std::move(sink_gen), arrow::default_memory_pool());

    // Call sink_reader Iterator interface
    int batch_cnt = 0;
    while (true) {
      auto maybe_batch = sink_reader->Next();
      ABORT_ON_FAILURE(maybe_batch.status());
      auto batch = std::move(maybe_batch).ValueOrDie();
      if (!batch) break;
      std::cout << "Batch #" << batch_cnt++ << " Num output rows: " << batch->num_rows()
                << std::endl;
      ARROW_CHECK_EQ(batch->num_rows(), 1);
      std::cout << "Result: " << batch->ToString() << std::endl;
    }
    // ... and wait for it to finish
    ABORT_ON_FAILURE(plan->finished().status());
  }

  return EXIT_SUCCESS;
}
