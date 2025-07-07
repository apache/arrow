% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.

classdef Table < arrow.tabular.Tabular

    methods

        function obj = Table(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.tabular.proxy.Table")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.tabular.Tabular(proxy);
        end

    end

    methods (Access=protected)

        function column = constructColumnFromProxy(~, proxyInfo)
            proxy = libmexclass.proxy.Proxy(Name="arrow.array.proxy.ChunkedArray", ID=proxyInfo);
            column = arrow.array.ChunkedArray(proxy);
        end
    end

    methods (Static, Access=public)

        function arrowTable = fromArrays(arrowArrays, opts)
            arguments(Repeating)
                arrowArrays(1, 1) arrow.array.Array
            end
            arguments
                opts.ColumnNames(1, :) string {mustBeNonmissing} = compose("Column%d", 1:numel(arrowArrays))
            end

            import arrow.tabular.internal.validateArrayLengths
            import arrow.tabular.internal.validateColumnNames
            import arrow.array.internal.getArrayProxyIDs

            numColumns = numel(arrowArrays);
            validateArrayLengths(arrowArrays);
            validateColumnNames(opts.ColumnNames, numColumns);

            arrayProxyIDs = getArrayProxyIDs(arrowArrays);
            args = struct(Method="from_arrays", ArrayProxyIDs=arrayProxyIDs, ColumnNames=opts.ColumnNames);
            proxyName = "arrow.tabular.proxy.Table";
            proxy = arrow.internal.proxy.create(proxyName, args);
            arrowTable = arrow.tabular.Table(proxy);
        end

        function arrowTable = fromRecordBatches(batches)
            arguments(Repeating)
                batches(1, 1) arrow.tabular.RecordBatch
            end
            if numel(batches) == 0
                msg = "The fromRecordBatches method requires at least one RecordBatch to be supplied.";
                error("arrow:Table:FromRecordBatches:ZeroBatches", msg);
            elseif numel(batches) > 1
                % Verify that all supplied RecordBatches have a consistent Schema.
                firstSchema = batches{1}.Schema;
                otherSchemas = cellfun(@(rb) rb.Schema, batches(2:end), UniformOutput=false);
                idx = cellfun(@(other) ~isequal(firstSchema, other), otherSchemas, UniformOutput=true);
                inconsistentSchemaIndex = find(idx, 1,"first");
                if ~isempty(inconsistentSchemaIndex)
                    inconsistentSchemaIndex = inconsistentSchemaIndex + 1;
                    expectedSchema = arrow.tabular.internal.display.getSchemaString(firstSchema);
                    inconsistentSchema = arrow.tabular.internal.display.getSchemaString(batches{inconsistentSchemaIndex}.Schema);
                    msg = "All RecordBatches must have the same Schema.\n\nSchema of RecordBatch %d is\n\n\t%s\n\nExpected RecordBatch Schema to be\n\n\t%s";
                    msg = compose(msg, inconsistentSchemaIndex, inconsistentSchema, expectedSchema);
                    error("arrow:Table:FromRecordBatches:InconsistentSchema", msg);
                end
            end

            % TODO: Rename getArrayProxyIDs to getProxyIDs
            proxyIDs = arrow.array.internal.getArrayProxyIDs(batches);
            args = struct(Method="from_record_batches", RecordBatchProxyIDs=proxyIDs);
            proxyName = "arrow.tabular.proxy.Table";
            proxy = arrow.internal.proxy.create(proxyName, args);
            arrowTable = arrow.tabular.Table(proxy);
        end

    end

end
