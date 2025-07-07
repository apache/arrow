%TABULAR Interface that represents a tabular data structure.

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

classdef Tabular < matlab.mixin.CustomDisplay & matlab.mixin.Scalar

    properties (Dependent, SetAccess=private, GetAccess=public)
        NumRows
        NumColumns
        ColumnNames
        Schema
    end

    properties (Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end
    
    methods(Access=protected, Abstract)
        % constructColumnFromProxy must construct an instance of the
        % appropriate MATLAB class from the proxyInfo argument. The
        % template method arrow.tabular.Tabular/column() invokes this
        % method.
        column = constructColumnFromProxy(obj, proxyInfo)
    end

    methods

        function obj = Tabular(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy
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

        function array = column(obj, idx)
            import arrow.internal.validate.*

            idx = index.numericOrString(idx, "int32", AllowNonScalar=false);

            if isnumeric(idx)
                args = struct(Index=idx);
                proxyInfo = obj.Proxy.getColumnByIndex(args);
            else
                args = struct(Name=idx);
                proxyInfo = obj.Proxy.getColumnByName(args);
            end

            array = obj.constructColumnFromProxy(proxyInfo);
        end

        function T = table(obj)
            import arrow.tabular.internal.*

            numColumns = obj.NumColumns;
            matlabArrays = cell(1, numColumns);

            for ii = 1:numColumns
                matlabArrays{ii} = toMATLAB(obj.column(ii));
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

end