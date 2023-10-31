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

classdef TableValidator < arrow.array.internal.list.ClassTypeValidator

    properties (GetAccess=public, SetAccess=private)
        VariableNames string = string.empty(1, 0)
        VariableValidators arrow.array.internal.list.Validator = arrow.array.internal.list.Validator.empty(1, 0) 
    end

    methods
        function obj = TableValidator(T)
            arguments
                T table {}
            end
            
            numVars = width(T);

            if (numVars == 0)
                error("arrow:array:list:TableWithZeroVariables", ...
                    "Require tables to have at least one variable.");
            end

            obj@arrow.array.internal.list.ClassTypeValidator(table);
            obj.VariableNames = string(T.Properties.VariableNames);
            validators = cell([1 numVars]);
            for ii = 1:numVars
                validators{ii} = arrow.array.internal.list.createValidator(T.(ii));
            end

            obj.VariableValidators = [validators{:}];
        end

        function validateElement(obj, element)
            % Verify element is a table
            validateElement@arrow.array.internal.list.ClassTypeValidator(obj, element);
            
            % Validate element has the expected number of variables
            numVars = numel(obj.VariableNames);
            if width(element) ~= numVars
                id = "arrow:array:list:NumVariablesMismatch";
                fmt = "Expected all tables in the cell array to have %d variables";
                msg = compose(fmt, numVars);
                error(id, msg);
            end

            % Validate element has the expected variable names
            if ~all(obj.VariableNames == string(element.Properties.VariableNames))
                id = "arrow:array:list:VariableNamesMismatch";
                msg = "Expected table names to match";
                error(id, msg);
            end

            for ii=1:numVars
                var = element.(ii);

                if ~istable(var) || ~iscolumn(var)
                    id = "arrow:array:list:NonTabularVariablesMustBeColumnar";
                    msg = "Table variables must be columnar";
                    error(id, msg);
                end

                obj.VariableValidators(ii).validateElement(var);
            end
        end

        function length = getElementLength(~, element)
            length = height(element);
        end

        function C = reshapeCellElements(~, C)
            % NO-OP for cell array of tables
        end
    end
end