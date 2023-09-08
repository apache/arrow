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

classdef RecordBatch < matlab.mixin.CustomDisplay & ...
                       matlab.mixin.Scalar

    properties (Dependent, SetAccess=private, GetAccess=public)
        NumColumns
        ColumnNames
        Schema
    end

    properties (Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    methods
        function obj = RecordBatch(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.tabular.proxy.RecordBatch")}
            end
            import arrow.internal.proxy.validate
            obj.Proxy = proxy;
        end

        function numColumns = get.NumColumns(obj)
            numColumns = obj.Proxy.getNumColumns();
        end

        function columnNames = get.ColumnNames(obj)
            columnNames = obj.Proxy.getColumnNames();
        end

        function schema = get.Schema(obj)
            proxyID = obj.Proxy.getSchema();
            proxy = libmexclass.proxy.Proxy(Name="arrow.tabular.proxy.Schema", ID=proxyID);
            schema = arrow.tabular.Schema(proxy);
        end

        function arrowArray = column(obj, idx)
            import arrow.internal.validate.*

            idx = index.numericOrString(idx, "int32", AllowNonScalar=false);

            if isnumeric(idx)
                args = struct(Index=idx);
                [proxyID, typeID] = obj.Proxy.getColumnByIndex(args);
            else
                args = struct(Name=idx);
                [proxyID, typeID] = obj.Proxy.getColumnByName(args);
            end
            
            traits = arrow.type.traits.traits(arrow.type.ID(typeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=proxyID);
            arrowArray = traits.ArrayConstructor(proxy);
        end

        function T = table(obj)
            import arrow.tabular.internal.*

            numColumns = obj.NumColumns;
            matlabArrays = cell(1, numColumns);
            
            for ii = 1:numColumns
                arrowArray = obj.column(ii);
                matlabArrays{ii} = toMATLAB(arrowArray);
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
            narginchk(2, inf);
            tf = false;

            schemasToCompare = cell([1 numel(varargin)]);
            for ii = 1:numel(varargin)
                rb = varargin{ii};
                if ~isa(rb, "arrow.tabular.RecordBatch")
                    % If rb is not a RecordBatch, then it cannot be equal
                    % to obj. Return false early.
                    return;
                end
                schemasToCompare{ii} = rb.Schema;
            end

            if ~isequal(obj.Schema, schemasToCompare{:})
                % If the schemas are not equal, the record batches are not
                % equal. Return false early.
                return;
            end

            % Function that extracts the column stored at colIndex from the
            % record batch stored at rbIndex in varargin.
            getColumnFcn = @(rbIndex, colIndex) varargin{rbIndex}.column(colIndex);

            rbIndices = 1:numel(varargin);
            for ii = 1:obj.NumColumns
                colIndices = repmat(ii, [1 numel(rbIndices)]);
                % Gather all columns at index ii across the record
                % batches stored in varargin. Compare these columns with
                % the corresponding column in obj. If they are not equal,
                % then the record batches are not equal. Return false.
                columnsToCompare = arrayfun(getColumnFcn, rbIndices, colIndices, UniformOutput=false);
                if ~isequal(obj.column(ii), columnsToCompare{:})
                    return;
                end
            end
            tf = true;
        end
    end

    methods (Access = private)
        function str = toString(obj)
            str = obj.Proxy.toString();
        end
    end

    methods (Access=protected)
        function displayScalarObject(obj)
            disp(obj.toString());
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
    end
end
