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
#include <vector>

namespace eng = arrow::engine;
namespace cp = arrow::compute;
namespace ac = arrow::acero;

class IgnoringConsumer : public ac::SinkNodeConsumer {
 public:
  explicit IgnoringConsumer(size_t tag) : tag_{tag} {}

  arrow::Status Init(const std::shared_ptr<arrow::Schema>& schema,
                     ac::BackpressureControl* backpressure_control,
                     ac::ExecPlan* plan) override {
    return arrow::Status::OK();
  }

  arrow::Status Consume(cp::ExecBatch batch) override {
    // Consume a batch of data
    // (just print its row count to stdout)
    std::cout << "-" << tag_ << " consumed " << batch.length << " rows" << std::endl;
    return arrow::Status::OK();
  }

  arrow::Future<> Finish() override {
    // Signal to the consumer that the last batch has been delivered
    // (we don't do any real work in this consumer so mark it finished immediately)
    //
    // The returned future should only finish when all outstanding tasks have completed
    // (after this method is called Consume is guaranteed not to be called again)
    std::cout << "-" << tag_ << " finished" << std::endl;
    return arrow::Future<>::MakeFinished();
  }

 private:
  // A unique label for instances to help distinguish logging output if a plan has
  // multiple sinks
  //
  // In this example, this is set to the zero-based index of the relation tree in the plan
  size_t tag_;
};

arrow::Future<std::shared_ptr<arrow::Buffer>> GetSubstraitFromServer(
    const std::string& filename) {
  // Emulate server interaction by parsing hard coded JSON
  std::string substrait_json = R"({
    "relations": [
      {"rel": {
        "read": {
          "base_schema": {
            "struct": {
              "types": [ {"i64": {}}, {"bool": {}} ]
            },
            "names": ["i", "b"]
          },
          "local_files": {
            "items": [
              {
                "uri_file": "file://FILENAME_PLACEHOLDER",
                "parquet": {}
              }
            ]
          }
        }
      }}
    ],
    "extension_uris": [
      {
        "extension_uri_anchor": 7,
        "uri": "https://github.com/apache/arrow/blob/main/format/substrait/extension_types.yaml"
      }
    ],
    "extensions": [
      {"extension_type": {
        "extension_uri_reference": 7,
        "type_anchor": 42,
        "name": "null"
      }},
      {"extension_function": {
        "extension_uri_reference": 7,
        "function_anchor": 42,
        "name": "add"
      }}
    ]
  })";
  std::string filename_placeholder = "FILENAME_PLACEHOLDER";
  substrait_json.replace(substrait_json.find(filename_placeholder),
                         filename_placeholder.size(), filename);
  return eng::internal::SubstraitFromJSON("Plan", substrait_json);
}

arrow::Status RunSubstraitConsumer(int argc, char** argv) {
  // Plans arrive at the consumer serialized in a Buffer, using the binary protobuf
  // serialization of a substrait Plan
  auto maybe_serialized_plan = GetSubstraitFromServer(argv[1]).result();
  ARROW_RETURN_NOT_OK(maybe_serialized_plan.status());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> serialized_plan,
                        std::move(maybe_serialized_plan));

  // Print the received plan to stdout as JSON
  ARROW_ASSIGN_OR_RAISE(auto plan_json,
                        eng::internal::SubstraitToJSON("Plan", *serialized_plan));

  std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
  std::cout << plan_json << std::endl;

  // The data sink(s) for plans is/are implicit in substrait plans, but explicit in
  // Arrow. Therefore, deserializing a plan requires a factory for consumers: each
  // time the root of a substrait relation tree is deserialized, an Arrow consumer is
  // constructed into which its batches will be piped.
  std::vector<std::shared_ptr<ac::SinkNodeConsumer>> consumers;
  std::function<std::shared_ptr<ac::SinkNodeConsumer>()> consumer_factory = [&] {
    // All batches produced by the plan will be fed into IgnoringConsumers:
    auto tag = consumers.size();
    consumers.emplace_back(new IgnoringConsumer{tag});
    return consumers.back();
  };

  // Deserialize each relation tree in the substrait plan to an Arrow compute Declaration
  arrow::Result<std::vector<ac::Declaration>> maybe_decls =
      eng::DeserializePlans(*serialized_plan, consumer_factory);
  ARROW_RETURN_NOT_OK(maybe_decls.status());
  ARROW_ASSIGN_OR_RAISE(std::vector<ac::Declaration> decls, std::move(maybe_decls));

  // It's safe to drop the serialized plan; we don't leave references to its memory
  serialized_plan.reset();

  // Construct an empty plan (note: configure Function registry and ThreadPool here)
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ac::ExecPlan> plan, ac::ExecPlan::Make());

  // Add decls to plan (note: configure ExecNode registry before this point)
  for (const ac::Declaration& decl : decls) {
    ARROW_RETURN_NOT_OK(decl.AddToPlan(plan.get()).status());
  }

  // Validate the plan and print it to stdout
  ARROW_RETURN_NOT_OK(plan->Validate());
  std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
  std::cout << plan->ToString() << std::endl;

  // Start the plan...
  std::cout << std::string(50, '#') << " consuming batches:" << std::endl;
  plan->StartProducing();

  // ... and wait for it to finish
  ARROW_RETURN_NOT_OK(plan->finished().status());
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Please specify a parquet file to scan" << std::endl;
    // Fake pass for CI
    return EXIT_SUCCESS;
  }

  auto status = RunSubstraitConsumer(argc, argv);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
