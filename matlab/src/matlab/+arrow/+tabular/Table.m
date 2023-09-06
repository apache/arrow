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
            args = struct(ArrayProxyIDs=arrayProxyIDs, ColumnNames=opts.ColumnNames);
            proxyName = "arrow.tabular.proxy.Table";
            proxy = arrow.internal.proxy.create(proxyName, args);
            arrowTable = arrow.tabular.Table(proxy);
        end

    end

end
