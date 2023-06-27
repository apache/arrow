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

classdef StringArray < arrow.array.Array
% arrow.array.StringArray

    properties (Hidden, SetAccess=private)
        NullSubstitionValue = string(missing);
    end

    properties(SetAccess=private, GetAccess=public)
        Type = arrow.type.StringType
    end

    methods
        function obj = StringArray(data, opts)
            arguments
                data
                opts.InferNulls(1,1) logical = true
                opts.Valid
            end
            % Support constructing a StringArray from a cell array of strings (i.e. cellstr),
            % or a string array, but not a char array.
            if ~ischar(data)
                data = convertCharsToStrings(data);
            end
            arrow.args.validateTypeAndShape(data, "string");
            validElements = arrow.args.parseValidElements(data, opts);
            opts = struct(MatlabArray=data, Valid=validElements);
            obj@arrow.array.Array("Name", "arrow.array.proxy.StringArray", "ConstructorArguments", {opts});
        end

        function data = string(obj)
            data = obj.toMATLAB();
        end

        function matlabArray = toMATLAB(obj)
            matlabArray = obj.Proxy.toMATLAB();
            matlabArray(~obj.Valid) = obj.NullSubstitionValue;
        end
    end
end
