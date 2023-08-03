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

classdef Schema < matlab.mixin.CustomDisplay
%SCHEMA A tabular schema which semantically describes
% the names and types of the columns of an associated tabular
% Arrow data type.

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
            idx = convertCharsToStrings(idx);
            if ~isempty(idx) && isscalar(idx) && isnumeric(idx) && idx >= 1
                args = struct(Index=int32(idx));
                proxyID = obj.Proxy.getFieldByIndex(args);
            elseif isscalar(idx) && isstring(idx)
                name = idx;
                args = struct(Name=name);
                proxyID = obj.Proxy.getFieldByName(args);
            else
                error("arrow:tabular:schema:UnsupportedFieldIndexType", ...
                      "Index must be a positive scalar integer or a valid field name.");
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
