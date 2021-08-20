# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module Arrow
  class Group
    def initialize(table, keys)
      @table = table
      @keys = keys
    end

    def count
      aggregate(["hash_count"])
    end

    def sum
      aggregate(["hash_sum"])
    end

    def product
      aggregate(["hash_product"])
    end

    def mean
      aggregate(["hash_mean"])
    end

    def min_max
      aggregate(["hash_min_max"])
    end

    def stddev
      aggregate(["hash_stddev"])
    end

    def variance
      aggregate(["hash_variance"])
    end

    def aggregate(aggregations)
      normalized_aggregations = normalize_aggregations(aggregations)
      plan = ExecutePlan.new
      source_node = plan.build_source_node(@table)
      aggregate_node =
        plan.build_aggregate_node(source_node,
                                  {
                                    aggregations: normalized_aggregations,
                                    keys: @keys
                                  })
      sink_node_options = SinkNodeOptions.new
      plan.build_sink_node(aggregate_node, sink_node_options)
      plan.validate
      plan.start
      plan.wait
      reader = sink_node_options.get_reader(aggregate_node.output_schema)
      reader.read_all
    end

    private
    def normalize_aggregations(aggregations)
      normalized_aggregations = []
      aggregations.each do |aggregation|
        case aggregation
        when :all
          all_functions = [
            "hash_count",
            "hash_sum",
            "hash_product",
            "hash_mean",
            "hash_stddev",
            "hash_variance",
            "hash_tdigest",
            "hash_min_max",
            "hash_any",
            "hash_all",
          ]
          normalized_aggregations.concat(normalize_aggregations(all_functions))
        when String
          function = aggregation
          case function
          when "count", "hash_count", "any", "hash_any", "all", "hash_all"
            target_columns.each do |column|
              normalized_aggregations << {function: function, input: column.name}
            end
          else
            numeric_target_columns.each do |column|
              normalized_aggregations << {function: function, input: column.name}
            end
          end
        else
          normalized_aggregations << aggregation
        end
      end
      normalized_aggregations
    end

    def target_columns
      @target_columns ||= find_target_columns
    end

    def find_target_columns
      key_names = @keys.collect(&:to_s)
      @table.columns.find_all do |column|
        not key_names.include?(column.name)
      end
    end

    def numeric_target_columns
      @numeric_target_columns ||= find_numeric_target_columns
    end

    def find_numeric_target_columns
      target_columns.find_all do |column|
        column.data_type.is_a?(NumericDataType)
      end
    end
  end
end
