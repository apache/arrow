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
          "name": "sum:opt_fp64"
        }
      },
      {
        "extensionFunction": {
          "name": "is_not_null:date"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 1,
          "name": "lte:date_date"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 3,
          "name": "subtract:opt_fp64_fp64"
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
          "name": "add:opt_fp64_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 7,
          "name": "avg:opt_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 4,
          "name": "multiply:opt_fp64_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 8,
          "name": "count:opt_i32"
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
                                  "l_tax",
                                  "l_returnflag",
                                  "l_linestatus",
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
                                      "fp64": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    },
                                    {
                                      "string": {
                                        "nullability": "NULLABILITY_NULLABLE"
                                      }
                                    },
                                    {
                                      "string": {
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
                                        "args": [
                                          {
                                            "selection": {
                                              "directReference": {
                                                "structField": {
                                                  "field": 6
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
                                                  "field": 6
                                                }
                                              }
                                            }
                                          },
                                          {
                                            "literal": {
                                              "date": 10470
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
                                    "args": [
                                      {
                                        "selection": {
                                          "directReference": {
                                            "structField": {
                                              "field": 6
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
                                              "field": 6
                                            }
                                          }
                                        }
                                      },
                                      {
                                        "literal": {
                                          "date": 10470
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
                        },
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
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 3
                              }
                            }
                          }
                        },
                        {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 4
                              }
                            }
                          }
                        },
                        {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 5
                              }
                            }
                          }
                        }
                      ]
                    }
                  },
                  "expressions": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 4
                          }
                        }
                      }
                    },
                    {
                      "selection": {
                        "directReference": {
                          "structField": {
                            "field": 5
                          }
                        }
                      }
                    },
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
                    },
                    {
                      "scalarFunction": {
                        "functionReference": 4,
                        "args": [
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
                            "scalarFunction": {
                              "functionReference": 3,
                              "args": [
                                {
                                  "literal": {
                                    "fp64": 1
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
                              ],
                              "outputType": {
                                "fp64": {
                                  "nullability": "NULLABILITY_NULLABLE"
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
                    },
                    {
                      "scalarFunction": {
                        "functionReference": 4,
                        "args": [
                          {
                            "scalarFunction": {
                              "functionReference": 4,
                              "args": [
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
                                  "scalarFunction": {
                                    "functionReference": 3,
                                    "args": [
                                      {
                                        "literal": {
                                          "fp64": 1
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
                                    ],
                                    "outputType": {
                                      "fp64": {
                                        "nullability": "NULLABILITY_NULLABLE"
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
                          },
                          {
                            "scalarFunction": {
                              "functionReference": 5,
                              "args": [
                                {
                                  "literal": {
                                    "fp64": 1
                                  }
                                },
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
                                "fp64": {
                                  "nullability": "NULLABILITY_NULLABLE"
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
                    },
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
                    },
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
                        "i32": 1
                      }
                    }
                  ]
                }
              },
              "groupings": [
                {
                  "groupingExpressions": [
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
                  ]
                }
              ],
              "measures": [
                {
                  "measure": {
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
                      }
                    ],
                    "phase": "AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE",
                    "outputType": {
                      "fp64": {
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }
                  }
                },
                {
                  "measure": {
                    "functionReference": 6,
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
                    "phase": "AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE",
                    "outputType": {
                      "fp64": {
                        "nullability": "NULLABILITY_NULLABLE"
                      }
                    }
                  }
                },
                {
                  "measure": {
                    "functionReference": 6,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 4
                            }
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
                },
                {
                  "measure": {
                    "functionReference": 6,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 5
                            }
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
                },
                {
                  "measure": {
                    "functionReference": 7,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 6
                            }
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
                },
                {
                  "measure": {
                    "functionReference": 7,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 7
                            }
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
                },
                {
                  "measure": {
                    "functionReference": 7,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 8
                            }
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
                },
                {
                  "measure": {
                    "functionReference": 8,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 9
                            }
                          }
                        }
                      }
                    ],
                    "phase": "AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE",
                    "outputType": {
                      "i64": {
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }
                  }
                }
              ]
            }
          },
          "names": [
            "l_returnflag",
            "l_linestatus",
            "sum",
            "sum",
            "sum",
            "sum",
            "sum",
            "count",
            "sum",
            "count",
            "sum",
            "count",
            "count"
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
          "functionAnchor": 3,
          "name": "alias:fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 4,
          "name": "alias:i64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 1,
          "name": "avg:opt_fp64"
        }
      },
      {
        "extensionFunction": {
          "functionAnchor": 2,
          "name": "count:opt_i32"
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
                          "l_returnflag",
                          "l_linestatus",
                          "sum",
                          "sum",
                          "sum",
                          "sum",
                          "sum",
                          "count",
                          "sum",
                          "count",
                          "sum",
                          "count",
                          "count"
                        ],
                        "struct": {
                          "types": [
                            {
                              "string": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "string": {
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
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "fp64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "fp64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            {
                              "i64": {
                                "nullability": "NULLABILITY_REQUIRED"
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
                    {
                      "groupingExpressions": [
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
                      ]
                    }
                  ],
                  "measures": [
                    {
                      "measure": {
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
                        "phase": "AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT",
                        "outputType": {
                          "fp64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      }
                    },
                    {
                      "measure": {
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
                        "phase": "AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT",
                        "outputType": {
                          "fp64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      }
                    },
                    {
                      "measure": {
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 4
                                }
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
                    },
                    {
                      "measure": {
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 5
                                }
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
                    },
                    {
                      "measure": {
                        "functionReference": 1,
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 6
                                }
                              }
                            }
                          },
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 7
                                }
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
                    },
                    {
                      "measure": {
                        "functionReference": 1,
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 8
                                }
                              }
                            }
                          },
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 9
                                }
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
                    },
                    {
                      "measure": {
                        "functionReference": 1,
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 10
                                }
                              }
                            }
                          },
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 11
                                }
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
                    },
                    {
                      "measure": {
                        "functionReference": 2,
                        "args": [
                          {
                            "selection": {
                              "directReference": {
                                "structField": {
                                  "field": 12
                                }
                              }
                            }
                          }
                        ],
                        "phase": "AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT",
                        "outputType": {
                          "i64": {
                            "nullability": "NULLABILITY_REQUIRED"
                          }
                        }
                      }
                    }
                  ]
                }
              },
              "expressions": [
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
                },
                {
                  "scalarFunction": {
                    "functionReference": 3,
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
                      "fp64": {
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
                      }
                    ],
                    "outputType": {
                      "fp64": {
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
                              "field": 4
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
                },
                {
                  "scalarFunction": {
                    "functionReference": 3,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 5
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
                },
                {
                  "scalarFunction": {
                    "functionReference": 3,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 6
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
                },
                {
                  "scalarFunction": {
                    "functionReference": 3,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 7
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
                },
                {
                  "scalarFunction": {
                    "functionReference": 3,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 8
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
                },
                {
                  "scalarFunction": {
                    "functionReference": 4,
                    "args": [
                      {
                        "selection": {
                          "directReference": {
                            "structField": {
                              "field": 9
                            }
                          }
                        }
                      }
                    ],
                    "outputType": {
                      "i64": {
                        "nullability": "NULLABILITY_REQUIRED"
                      }
                    }
                  }
                }
              ]
            }
          }
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

void ReadSink(
    std::shared_ptr<arrow::Schema> output_schema,
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen) {
  // Call sink_reader Iterator interface
  std::shared_ptr<arrow::RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
      std::move(output_schema), std::move(sink_gen), arrow::default_memory_pool());
  std::cout << std::string(50, '#') << " consuming batches:" << std::endl;
  int batch_cnt = 0;
  while (true) {
    auto maybe_batch = sink_reader->Next();
    ABORT_ON_FAILURE(maybe_batch.status());
    auto batch = std::move(maybe_batch).ValueOrDie();
    if (!batch) break;
    std::cout << "Batch #" << batch_cnt++ << " Num output rows: " << batch->num_rows()
              << std::endl;
    std::cout << "Result: " << batch->ToString() << std::endl;
  }
}

int main(int argc, char** argv) {
  std::shared_ptr<arrow::Schema> input_schema;
  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> gen;

  std::vector<cp::Declaration> decls0;
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
    //    std::vector<cp::Declaration> decls = std::move(maybe_decls).ValueOrDie();
    decls0 = std::move(maybe_decls).ValueOrDie();

    // It's safe to drop the serialized plan; we don't leave references to its memory
    serialized_plan.reset();

    auto& decl = decls0[0];

    // Construct an empty plan (note: configure Function registry and ThreadPool here)
    arrow::Result<std::shared_ptr<cp::ExecPlan>> maybe_plan = cp::ExecPlan::Make();
    ABORT_ON_FAILURE(maybe_plan.status());
    std::shared_ptr<cp::ExecPlan> plan = std::move(maybe_plan).ValueOrDie();

    auto maybe_node = decl.AddToPlan(plan.get());
    ABORT_ON_FAILURE(maybe_node.status());
    auto node = std::move(maybe_node).ValueOrDie();
    std::cout << "schema: " << node->output_schema()->ToString() << std::endl;

    input_schema = node->output_schema();

    // add sink node
    auto sink_node = arrow::compute::MakeExecNode("sink", plan.get(), {node},
                                                  arrow::compute::SinkNodeOptions{&gen});
    ABORT_ON_FAILURE(sink_node.status());

    // Validate the plan and print it to stdout
    ABORT_ON_FAILURE(plan->Validate());
    std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
    std::cout << plan->ToString() << std::endl;

    // Start the plan...
    ABORT_ON_FAILURE(plan->StartProducing());

    //    ReadSink(node->output_schema(), std::move(gen));

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

    // Add source node
    std::vector<cp::Declaration> source = {
        arrow::compute::Declaration{"source", cp::SourceNodeOptions{input_schema, gen}}};
    ReplaceSourceDecls(std::move(source), &decls[0]);
    auto& decl = decls[0];

    // Construct an empty plan (note: configure Function registry and ThreadPool here)
    arrow::Result<std::shared_ptr<cp::ExecPlan>> maybe_plan = cp::ExecPlan::Make();
    ABORT_ON_FAILURE(maybe_plan.status());
    std::shared_ptr<cp::ExecPlan> plan = std::move(maybe_plan).ValueOrDie();

    // Add decls to plan (note: configure ExecNode registry before this point)
    auto* node = decl.AddToPlan(plan.get()).ValueOrDie();
    std::cout << "schema: " << node->output_schema()->ToString() << std::endl;

    auto output_schema = node->output_schema();

    // add sink node
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen;
    auto sink_node = arrow::compute::MakeExecNode(
        "sink", plan.get(), {node}, arrow::compute::SinkNodeOptions{&sink_gen});
    ABORT_ON_FAILURE(sink_node.status());

    // Validate the plan and print it to stdout
    ABORT_ON_FAILURE(plan->Validate());
    std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
    std::cout << plan->ToString() << std::endl;

    // Start the plan...
    std::cout << std::string(50, '#') << " consuming batches:" << std::endl;
    ABORT_ON_FAILURE(plan->StartProducing());

    ReadSink(std::move(output_schema), std::move(sink_gen));

    // ... and wait for it to finish
    ABORT_ON_FAILURE(plan->finished().status());
  }

  return EXIT_SUCCESS;
}
