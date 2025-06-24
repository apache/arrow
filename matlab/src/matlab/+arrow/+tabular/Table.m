%TABLE A tabular data structure representing a set of
% arrow.array.ChunkedArray objects with a fixed schema.

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

classdef Table < matlab.mixin.CustomDisplay & matlab.mixin.Scalar

    properties (Dependent, SetAccess=private, GetAccess=public)
        NumRows
        NumColumns
        ColumnNames
        Schema
    end

    properties (Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    methods

        function obj = Table(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.tabular.proxy.Table")}
            end
            import arrow.internal.proxy.validate
            obj.Proxy = proxy;
        end

        function numColumns = get.NumColumns(obj)
            numColumns = obj.Proxy.getNumColumns();
        end

        function numRows = get.NumRows(obj)
            numRows = obj.Proxy.getNumRows();
        end

        function columnNames = get.ColumnNames(obj)
            columnNames = obj.Proxy.getColumnNames();
        end

        function schema = get.Schema(obj)
            proxyID = obj.Proxy.getSchema();
            proxy = libmexclass.proxy.Proxy(Name="arrow.tabular.proxy.Schema", ID=proxyID);
            schema = arrow.tabular.Schema(proxy);
        end

        function chunkedArray = column(obj, idx)
            import arrow.internal.validate.*

            idx = index.numericOrString(idx, "int32", AllowNonScalar=false);

            if isnumeric(idx)
                args = struct(Index=idx);
                proxyID = obj.Proxy.getColumnByIndex(args);
            else
                args = struct(Name=idx);
                proxyID = obj.Proxy.getColumnByName(args);
            end

            proxy = libmexclass.proxy.Proxy(Name="arrow.array.proxy.ChunkedArray", ID=proxyID);
            chunkedArray = arrow.array.ChunkedArray(proxy);
        end

        function T = table(obj)
            import arrow.tabular.internal.*

            numColumns = obj.NumColumns;
            matlabArrays = cell(1, numColumns);

            for ii = 1:numColumns
                chunkedArray = obj.column(ii);
                matlabArrays{ii} = toMATLAB(chunkedArray);
            end

            validVariableNames = makeValidVariableNames(obj.ColumnNames);
            validDimensionNames = makeValidDimensionNames(validVariableNames);

            T = table(matlabArrays{:}, ...
                VariableNames=validVariableNames, ...
                DimensionNames=validDimensionNames);
        end

        function T = toMATLAB(obj)
            T = obj.table();
        end

        function tf = isequal(obj, varargin)
            tf = arrow.tabular.internal.isequal(obj, varargin{:});
        end

    end

    methods (Access = private)

        function str = toString(obj)
            str = obj.Proxy.toString();
        end

    end

    methods (Access=protected)
        function displayScalarObject(obj)
            className = matlab.mixin.CustomDisplay.getClassNameForHeader(obj);
            tabularDisplay = arrow.tabular.internal.display.getTabularDisplay(obj, className);
            disp(tabularDisplay + newline);
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
