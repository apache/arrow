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

classdef Reader
%READER An internal Reader object for reading Feather files.

    properties (GetAccess=public, SetAccess=private, Hidden)
        Proxy
    end

    properties (Dependent, SetAccess=private, GetAccess=public)
        % Name of the file to read.
        Filename
    end

    methods

        function obj = Reader(filename)
            arguments
                filename(1, 1) {mustBeNonmissing, mustBeNonzeroLengthText}
            end

            args = struct(Filename=filename);
            obj.Proxy = arrow.internal.proxy.create("arrow.io.feather.proxy.Reader", args);
        end

        function recordBatch = read(obj)
            recordBatchProxyID = obj.Proxy.read();
            proxy = libmexclass.proxy.Proxy(Name="arrow.tabular.proxy.RecordBatch", ID=recordBatchProxyID);
            recordBatch = arrow.tabular.RecordBatch(proxy);
        end

        function filename = get.Filename(obj)
            filename = obj.Proxy.getFilename();
        end

    end

end
