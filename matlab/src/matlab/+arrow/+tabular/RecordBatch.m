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
%arrow.tabular.RecordBatch A tabular data structure representing
% a set of arrow.array.Array objects with a fixed schema.

    properties (Access=private)
        ArrowArrays = {};
    end

    properties (Dependent, SetAccess=private, GetAccess=public)
        NumColumns
        ColumnNames
    end

    properties (Access=protected)
        Proxy
    end

    methods

        function numColumns = get.NumColumns(obj)
            numColumns = obj.Proxy.numColumns();
        end

        function columnNames = get.ColumnNames(obj)
            columnNames = obj.Proxy.columnNames();
        end

        function arrowArray = column(obj, idx)
            arrowArray = obj.ArrowArrays{idx};
        end

        function obj = RecordBatch(T)
            obj.ArrowArrays = arrow.tabular.RecordBatch.decompose(T);
            columnNames = string(T.Properties.VariableNames);
            arrayProxyIDs = arrow.tabular.RecordBatch.getArrowProxyIDs(obj.ArrowArrays);
            opts = struct("ArrayProxyIDs", arrayProxyIDs, ...
                          "ColumnNames", columnNames);
            obj.Proxy = libmexclass.proxy.Proxy("Name", "arrow.tabular.proxy.RecordBatch", "ConstructorArguments", {opts});
        end

        function T = table(obj)
            matlabArrays = cell(1, numel(obj.ArrowArrays));
            
            for ii = 1:numel(obj.ArrowArrays)
                matlabArrays{ii} = toMATLAB(obj.ArrowArrays{ii});
            end

            variableNames = matlab.lang.makeUniqueStrings(obj.ColumnNames);
            % NOTE: Does not currently handle edge cases like ColumnNames
            %       matching the table DimensionNames.
            T = table(matlabArrays{:}, VariableNames=variableNames);
        end

        function T = toMATLAB(obj)
            T = obj.table();
        end
        
    end

    methods (Static)

        function arrowArrays = decompose(T)
            % Decompose the input MATLAB table
            % input a cell array of equivalent arrow.array.Array
            % instances.
            arguments
                T table
            end

            numColumns = width(T);
            arrowArrays = cell(1, numColumns);

            % Convert each MATLAB array into a corresponding
            % arrow.array.Array.
            for ii = 1:numColumns
                arrowArrays{ii} = arrow.array(T{:, ii});
            end
        end

        function proxyIDs = getArrowProxyIDs(arrowArrays)
            % Extract the Proxy IDs underlying a cell array of 
            % arrow.array.Array instances.
            proxyIDs = zeros(1, numel(arrowArrays), "uint64");

            % Convert each MATLAB array into a corresponding
            % arrow.array.Array.
            for ii = 1:numel(arrowArrays)
                proxyIDs(ii) = arrowArrays{ii}.Proxy.ID;
            end
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

end

