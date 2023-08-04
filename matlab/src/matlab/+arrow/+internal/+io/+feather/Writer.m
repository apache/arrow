%WRITER Class for writing feather V1 files.

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
classdef Writer < matlab.mixin.Scalar
    %UNTITLED2 Summary of this class goes here
    %   Detailed explanation goes here

    properties(Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    properties(Dependent)
        Filename
    end

    methods
        function obj = Writer(filename)
            arguments
                filename(1, 1) {mustBeNonmissing, mustBeNonzeroLengthText}
            end

            args = struct(Filename=filename);
            proxyName = "arrow.io.feather.proxy.FeatherWriter";
            obj.Proxy = libmexclass.proxy.Proxy(Name=proxyName, ...
                ConstructorArguments={args});
        end

        function write(obj, T)
            rb = arrow.recordbatch(T);
            args = struct(RecordBatchProxyID=rb.Proxy.ID);
            obj.Proxy.writeRecordBatch(args);
        end

        function filename = get.Filename(obj)
            filename = obj.Proxy.getFilename();
        end
    end
end