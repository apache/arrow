%ARRAYIMPORTER Imports Arrow Array using the C Data Interface Format.

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
classdef ArrayImporter < matlab.mixin.Scalar

    properties (Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    methods

        function obj = ArrayImporter()
            proxyName = "arrow.c.proxy.ArrayImporter";
            proxy = arrow.internal.proxy.create(proxyName, struct());
            obj.Proxy = proxy;
        end

        function array = import(obj, cArray, cSchema)
            arguments
                obj(1, 1) arrow.c.internal.ArrayImporter
                cArray(1, 1) arrow.c.Array
                cSchema(1, 1) arrow.c.Schema
            end
            args = struct(...
                ArrowArrayAddress=cArray.Address,...
                ArrowSchemaAddress=cSchema.Address...
            );
            proxyInfo = obj.Proxy.import(args);
            traits = arrow.type.traits.traits(arrow.type.ID(proxyInfo.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=proxyInfo.ProxyID);
            array = traits.ArrayConstructor(proxy);
        end

    end

end

