%RECORDBATCHIMPORTER Imports Arrow RecordBatch using the C Data Interface
% Format.

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

classdef RecordBatchImporter

    properties (Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    methods

        function obj = RecordBatchImporter()
            proxyName = "arrow.c.proxy.RecordBatchImporter";
            proxy = arrow.internal.proxy.create(proxyName, struct());
            obj.Proxy = proxy;
        end

        function recordBatch = import(obj, cArray, cSchema)
            arguments
                obj(1, 1) arrow.c.internal.RecordBatchImporter
                cArray(1, 1) arrow.c.Array
                cSchema(1, 1) arrow.c.Schema
            end
            args = struct(...
                ArrowArrayAddress=cArray.Address,...
                ArrowSchemaAddress=cSchema.Address...
            );
            proxyID = obj.Proxy.import(args);
            proxyName = "arrow.tabular.proxy.RecordBatch";
            proxy = libmexclass.proxy.Proxy(Name=proxyName, ID=proxyID);
            recordBatch = arrow.tabular.RecordBatch(proxy);
        end

    end

end

