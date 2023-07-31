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
        % Names
        % Types of the columns in the associated tabular type.
        % Types
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
            idx = converCharsToStrings(idx);
            
            % idx must be a positive whole number or field name.
            if isstring(idx)
                name = convertCharsToStrings(idx);
                proxyID = obj.Proxy.getFieldByName(name);
            elseif isnumeric()
                proxyID = obj.Proxy.getFieldByIndex(idx);
            end

            fieldProxy = libmexclass.proxy.Proxy(Name="arrow.type.proxy.Field", ID=proxyID);
            F = arrow.Field(fieldProxy);
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