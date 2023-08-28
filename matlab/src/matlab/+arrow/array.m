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

    data = convertCellstrToString(data);
    classname = string(class(data));
    args = namedargs2cell(opts);

    switch (classname)
        case "logical"
            arrowArray = arrow.array.BooleanArray.fromMATLAB(data, args{:});
        case "uint8"
            arrowArray = arrow.array.UInt8Array.fromMATLAB(data, args{:});
        case "uint16"
            arrowArray = arrow.array.UInt16Array.fromMATLAB(data, args{:});
        case "uint32"
            arrowArray = arrow.array.UInt32Array.fromMATLAB(data, args{:});
        case "uint64"
            arrowArray = arrow.array.UInt64Array.fromMATLAB(data, args{:});
        case "int8"
            arrowArray = arrow.array.Int8Array.fromMATLAB(data, args{:});
        case "int16"
            arrowArray = arrow.array.Int16Array.fromMATLAB(data, args{:});
        case "int32"
            arrowArray = arrow.array.Int32Array.fromMATLAB(data, args{:});
        case "int64"
            arrowArray = arrow.array.Int64Array.fromMATLAB(data, args{:});
        case "single"
            arrowArray = arrow.array.Float32Array.fromMATLAB(data, args{:});
        case "double"
            arrowArray = arrow.array.Float64Array.fromMATLAB(data, args{:});
        case "string"
            arrowArray = arrow.array.StringArray.fromMATLAB(data, args{:});
        case "datetime"
            arrowArray = arrow.array.TimestampArray.fromMATLAB(data, args{:});
        case "duration"
            arrowArray = arrow.array.Time64Array.fromMATLAB(data, args{:});
        otherwise
            errid = "arrow:array:UnsupportedMATLABType";
            msg = join(["Unable to convert MATLAB type" classname "to arrow array."]);
            error(errid, msg);
    end
end

function data = convertCellstrToString(data)
    % Support constructing a StringArray from a cell array of strings
    % (i.e. cellstr), or a string array, but not a char array.
    if ~ischar(data)
        data = convertCharsToStrings(data);
    end
end