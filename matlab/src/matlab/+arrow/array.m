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

function arrowArray = array(data, opts)
    arguments
        data
        opts.InferNulls(1, 1) logical = true
        opts.Valid
    end

    data = convertCharsToStrings(data);
    classname = string(class(data));
    args = namedargs2cell(opts);

    switch (classname)
        case "logical"
            arrowArray = arrow.array.BooleanArray.fromMATLAB(data, args{:});
        case "int8"
            arrowArray = arrow.array.Int8Array.fromMATLAB(data, args{:});
        case "uint8"
        case "int16"
        case "uint16"
        case "int32"
        case "uint32"
        case "int64"
        case "uint64"
        case "single"
        case "double"
        case "string"
        case "datetime"
        otherwise
            errid = "arrow:array:UnsupportedMATLABType";
            msg = join(["Unable to convert MATLAB type" classname "to arrow array."]);
            error(errid, msg);
    end
end