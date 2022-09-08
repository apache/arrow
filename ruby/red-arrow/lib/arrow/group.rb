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

    def count(*target_names)
      aggregate(*build_aggregations("hash_count", target_names))
    end

    def sum(*target_names)
      aggregate(*build_aggregations("hash_sum", target_names))
    end

    def product(*target_names)
      aggregate(*build_aggregations("hash_product", target_names))
    end

    def mean(*target_names)
      aggregate(*build_aggregations("hash_mean", target_names))
    end

    def min(*target_names)
      aggregate(*build_aggregations("hash_min", target_names))
    end

    def max(*target_names)
      aggregate(*build_aggregations("hash_max", target_names))
    end

    def stddev(*target_names)
      aggregate(*build_aggregations("hash_stddev", target_names))
    end

    def variance(*target_names)
      aggregate(*build_aggregations("hash_variance", target_names))
    end

    def aggregate(aggregation, *more_aggregations)
      aggregations = [aggregation] + more_aggregations
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
    def build_aggregations(function_name, target_names)
      if target_names.empty?
        [function_name]
      else
        target_names.collect do |name|
          "#{function_name}(#{name})"
        end
      end
    end

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
            # "hash_tdigest",
            "hash_min",
            "hash_max",
            "hash_any",
            "hash_all",
          ]
          normalized_aggregations.concat(normalize_aggregations(all_functions))
        when /\A([a-zA-Z0-9_].+?)\((.+?)\)\z/
          function = $1
          input = $2.strip
          normalized_aggregations << {function: function, input: input}
        when "count", "hash_count"
          function = aggregation
          target_columns.each do |column|
            normalized_aggregations << {function: function, input: column.name}
          end
        when "any", "hash_any", "all", "hash_all"
          function = aggregation
          boolean_target_columns.each do |column|
            normalized_aggregations << {function: function, input: column.name}
          end
        when String
          function = aggregation
          numeric_target_columns.each do |column|
            normalized_aggregations << {function: function, input: column.name}
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

    def boolean_target_columns
      @boolean_target_columns ||= find_boolean_target_columns
    end

    def find_boolean_target_columns
      target_columns.find_all do |column|
        column.data_type.is_a?(BooleanDataType)
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
