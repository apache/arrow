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

classdef StructType < arrow.type.Type

    methods
        function obj = StructType(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.type.proxy.StructType")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.type.Type(proxy);
        end
    end

    methods(Access = protected)
        function groups = getDisplayPropertyGroups(obj)
            targets = ["ID", "Fields"];
            groups = matlab.mixin.util.PropertyGroup(targets);
        end
    end

    methods (Hidden)
        function data = preallocateMATLABArray(obj, numElements)
            import arrow.tabular.internal.*

            fields = obj.Fields;
            
            % Construct the VariableNames and VariableDimensionNames
            fieldNames = [fields.Name];
            validVariableNames = makeValidVariableNames(fieldNames);
            validDimensionNames = makeValidDimensionNames(validVariableNames);

            % Recursively call preallocateMATLABArray to handle
            % preallocation of nested types
            variableData = cell(1, numel(fields));
            for ii = 1:numel(fields)
                type = fields(ii).Type;
                variableData{ii} = preallocateMATLABArray(type,  numElements);
            end

            % Return a table with the appropriate schema and dimensions 
            data = table(variableData{:}, ...
                VariableNames=validVariableNames, ...
                DimensionNames=validDimensionNames);
         end
    end
end