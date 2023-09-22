%SCHEMA A tabular schema which semantically describes
% the names and types of the columns of an associated tabular
% Arrow data type.

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

classdef Schema < matlab.mixin.CustomDisplay & ...
                  matlab.mixin.Scalar

    properties (GetAccess=public, SetAccess=private, Hidden)
        Proxy
    end

    properties (Dependent, SetAccess=private, GetAccess=public)
        % Underlying array of Fields that the Schema wraps.
        Fields
        % Names of the columns in the associated tabular type.
        FieldNames
        % Number of fields in the schema
        NumFields
    end

    methods

        function obj = Schema(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.tabular.proxy.Schema")}
            end
            import arrow.internal.proxy.validate

            obj.Proxy = proxy;
        end
        
        function F = field(obj, idx)
            import arrow.internal.validate.*
            
            idx = index.numericOrString(idx, "int32", AllowNonScalar=false);

            if isnumeric(idx)
                args = struct(Index=idx);
                proxyID = obj.Proxy.getFieldByIndex(args);
            else
                args = struct(Name=idx);
                proxyID = obj.Proxy.getFieldByName(args);
            end

            proxy = libmexclass.proxy.Proxy(Name="arrow.type.proxy.Field", ID=proxyID);
            F = arrow.type.Field(proxy);
        end

        function fields = get.Fields(obj)
            fields = arrow.type.Field.empty(0, obj.NumFields);
            for ii = 1:obj.NumFields
                fields(ii) = obj.field(ii);
            end
        end

        function fieldNames = get.FieldNames(obj)
            fieldNames = obj.Proxy.getFieldNames();
        end

        function numFields = get.NumFields(obj)
            numFields = obj.Proxy.getNumFields();
        end

        function tf = isequal(obj, varargin)
            narginchk(2, inf);
            tf = false;
            
            fieldsToCompare = cell([1 numel(varargin)]);
            for ii = 1:numel(varargin)
                schema = varargin{ii};
                if ~isa(schema, "arrow.tabular.Schema")
                    % Return false early if schema is not actually an
                    % arrow.tabular.Schema instance.
                    return;
                end

                fieldsToCompare{ii} = schema.Fields;
            end

            % Return if the Schema Fields properties are equal
            tf = isequal(obj.Fields, fieldsToCompare{:});
        end
    end

    methods (Access=protected)

        function header = getHeader(obj)
            name = matlab.mixin.CustomDisplay.getClassNameForHeader(obj);
            numFields = obj.NumFields;
            if numFields == 0
                header = compose("  %s with 0 fields" + newline, name);
            elseif numFields == 1
                header = compose("  %s with %d field:" + newline, name, numFields);
            else
                header = compose("  %s with %d fields:" + newline, name, numFields);
            end
        end

        function displayScalarObject(obj)
            disp(getHeader(obj));
            numFields = obj.NumFields;

            if numFields > 0
                strs = strings([3, numFields]);
                strs(1, :) = [obj.FieldNames];
                strs(3, :) = arrayfun(@(type) string(class(type)), [obj.Fields.Type]);
                
                % Calculate the maximum string length per column
                maxLength = max(strlength(strs), [], 1);
    
                % Create the dividers ("_") between each field name and type
                strs(2, :) = arrayfun(@(width) string(repmat('_', [1 width])), maxLength);
    
                % Pad each string with whitespace before center-aligning.
                for ii = 1:numFields
                    strs(:, ii) = pad(strs(:, ii), maxLength(ii));
                end
                strs = strjust(strs, "center");
    
                % Concatenate the text values in each row to create a 3x1
                % string vector. 
                if (size(strs, 2) > 1)
                    strs = strcat(join(strs, "    "));
                end
                
                % Indent the rows
                strs = "    " + strs;
    
                % Create the header row by joining the first two rows with a
                % newline character.
                header = strjoin(strs(1:2), newline);
    
                % Concatenate the header with the last row using two newline
                % characters as the delimiter. This is the final display.
                display = header + newline + newline + strs(3);
                
                disp(display + newline);
            end

        end

    end

end
