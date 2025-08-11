%RECORDBATCH A tabular data structure representing a set of 
%arrow.array.Array objects with a fixed schema.

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

classdef RecordBatch < arrow.tabular.Tabular

    methods

        function obj = RecordBatch(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.tabular.proxy.RecordBatch")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.tabular.Tabular(proxy);
        end

        function export(obj, cArrowArrayAddress, cArrowSchemaAddress)
            arguments
                obj(1, 1) arrow.tabular.RecordBatch
                cArrowArrayAddress(1, 1) uint64
                cArrowSchemaAddress(1, 1) uint64
            end
            args = struct(...
                ArrowArrayAddress=cArrowArrayAddress,...
                ArrowSchemaAddress=cArrowSchemaAddress...
            );
            obj.Proxy.exportToC(args);
        end

    end

    methods (Access=protected)

        function column = constructColumnFromProxy(~, proxyInfo)
            traits = arrow.type.traits.traits(arrow.type.ID(proxyInfo.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=proxyInfo.ProxyID);
            column = traits.ArrayConstructor(proxy);
        end

    end

    methods (Static, Access=public)

        function recordBatch = fromArrays(arrowArrays, opts)
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
            args = struct(ArrayProxyIDs=arrayProxyIDs, ColumnNames=opts.ColumnNames);
            proxyName = "arrow.tabular.proxy.RecordBatch";
            proxy = arrow.internal.proxy.create(proxyName, args);
            recordBatch = arrow.tabular.RecordBatch(proxy);
        end

        function recordBatch = import(cArray, cSchema)
            arguments
                cArray(1, 1) arrow.c.Array
                cSchema(1, 1) arrow.c.Schema
            end
            importer = arrow.c.internal.RecordBatchImporter();
            recordBatch = importer.import(cArray, cSchema);
        end

    end

end
