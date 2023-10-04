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

classdef ListType < arrow.type.Type

    properties (Dependent, GetAccess=public, SetAccess=private)
        % The inner element type of the ListType.
        % For example, for List64<UInt64Type>, Type = UInt64Type.
        Type
    end

    methods
        function obj = ListType(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.type.proxy.ListType")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.type.Type(proxy);
        end
    end

    methods(Access = protected)
        function groups = getDisplayPropertyGroups(~)
            targets = ["ID", "Type"];
            groups = matlab.mixin.util.PropertyGroup(targets);
        end
    end


    methods
        function type = get.Type(obj)
            typeStruct = obj.Proxy.getType();
            traits = arrow.type.traits.traits(arrow.type.ID(typeStruct.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.TypeProxyClassName, ID=typeStruct.ProxyID);
            type = traits.TypeConstructor(proxy);
        end
    end

    methods (Hidden)
        function data = preallocateMATLABArray(~, numElements)
            % Preallocate an empty cell array.
            data = cell(numElements, 1);
        end
    end
end