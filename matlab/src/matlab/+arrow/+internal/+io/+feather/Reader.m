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
            filename = convertCharsToStrings(filename);
            if ~(isstring(filename) && isscalar(filename))
                error("arrow:io:feather:FilenameUnsupportedType", "Filename must be a scalar string or char row vector.");
            end
            obj.Filename = filename;
        end

        function T = read(obj)
            args = struct(Filename=obj.Filename);
            recordBatchProxyID = obj.Proxy.read(args);
            proxy = libmexclass.proxy.Proxy(Name="arrow.tabular.proxy.RecordBatch", ID=recordBatchProxyID);
            recordBatch = arrow.tabular.RecordBatch(proxy);
            T = recordBatch.toMATLAB();
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