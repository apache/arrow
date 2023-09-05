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

classdef Field < matlab.mixin.CustomDisplay
%FIELD A class representing a name and a type.
% Fields are often used in tabular schemas for describing a column's
% name and type.

    properties (GetAccess=public, SetAccess=private, Hidden)
        Proxy
    end

    properties (Dependent)
        % Name of the field
        Name
        % Arrow type of the field
        Type
    end

    methods
        function obj = Field(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.type.proxy.Field")}
            end
            import arrow.internal.proxy.validate

            obj.Proxy = proxy;
        end

        function type = get.Type(obj)
            typeStruct = obj.Proxy.getType();
            traits = arrow.type.traits.traits(arrow.type.ID(typeStruct.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.TypeProxyClassName, ID=typeStruct.ProxyID);
            type = traits.TypeConstructor(proxy);
        end

        function name = get.Name(obj)
            name = obj.Proxy.getName();
        end

        function tf = isequal(obj, varargin)
            narginchk(2, inf);
            tf = false;
            
            names = [obj(:).Name];
            types = 
            namesToCompare = strings(numel(obj), numel(varargin));
            typesToCompare = cell([1 numel(varargin)]);

            for ii = 1:numel(varargin)
                field = varargin{ii};
                if ~isa(field, "arrow.type.Field") || ~isequal(size(obj), size(field))
                    % Return early if field is not an arrow.type.Field
                    % or if the dimensions of obj and field do not match.
                    return;
                end

                % field(:) flattens N-dimensional arrays into column vectors.
                namesToCompare(:, ii) = [field(:).Name];
                typesToCompare(1, ii) = {field(:).Type};
            end

            names = [obj(:).Name];
            if any(names ~= namesToCompare, "all")
                % Return early if the field names don't match
                return;
            end

            types = [obj(:).Type];
            tf = isequal(types, typesToCompare{:});
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
