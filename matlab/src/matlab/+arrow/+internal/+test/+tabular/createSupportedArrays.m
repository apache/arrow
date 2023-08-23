%CREATESUPPORTEDARRAYS Creates a MATLAB cell array containing all the
%concrete subclasses of arrow.array.Array.

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

function [arrowArrays, matlabData] = createSupportedArrays(opts)
    arguments
        opts.NumRows(1, 1) {mustBeFinite, mustBeNonnegative} = 3;  
    end

    import arrow.type.ID
    import arrow.array.*

    arrowArrays = cell(14, 1);
    matlabData  = cell(14, 1);

    matlabData{1}   = randomLogicals(opts.NumRows); 
    arrowArrays{1}  = BooleanArray.fromMATLAB(matlabData{1});

    matlabData{2}   = randomNumbers("uint8", opts.NumRows);
    arrowArrays{2}  = UInt8Array.fromMATLAB(matlabData{2});

    matlabData{3}   = randomNumbers("uint16", opts.NumRows);
    arrowArrays{3}  = UInt16Array.fromMATLAB(matlabData{3});

    matlabData{4}   = randomNumbers("uint32", opts.NumRows);
    arrowArrays{4}  = UInt32Array.fromMATLAB(matlabData{4});

    matlabData{5}   = randomNumbers("uint64", opts.NumRows);
    arrowArrays{5}  = UInt64Array.fromMATLAB(matlabData{5});

    matlabData{6}   = randomNumbers("int8", opts.NumRows);
    arrowArrays{6}  = Int8Array.fromMATLAB(matlabData{6});

    matlabData{7}   = randomNumbers("int16", opts.NumRows);
    arrowArrays{7}  = Int16Array.fromMATLAB(matlabData{7});

    matlabData{8}   = randomNumbers("int32", opts.NumRows);
    arrowArrays{8}  = Int32Array.fromMATLAB(matlabData{8});

    matlabData{9}   = randomNumbers("int64", opts.NumRows);
    arrowArrays{9}  = Int64Array.fromMATLAB(matlabData{9});

    matlabData{10}   = randomNumbers("single", opts.NumRows);
    arrowArrays{10} = Float32Array.fromMATLAB(matlabData{10});

    matlabData{11}  = randomNumbers("double", opts.NumRows);
    arrowArrays{11} = Float64Array.fromMATLAB(matlabData{11});

    matlabData{12}  = randomStrings(opts.NumRows);
    arrowArrays{12} = StringArray.fromMATLAB(matlabData{12});

    matlabData{13}  = randomDatetimes(opts.NumRows);
    arrowArrays{13} = TimestampArray.fromMATLAB(matlabData{13});

    matlabData{14}  = randomDurations(opts.NumRows);
    arrowArrays{14} = Time32Array.fromMATLAB(matlabData{14});
end


function number = randomNumbers(numberType, numElements)
    number = cast(randi(255, [numElements 1]), numberType);
end

function text = randomStrings(numElements)
    text = string(randi(255, [numElements 1]));
end

function tf = randomLogicals(numElements)
    number = randi(2, [numElements 1]) - 1;
    tf = logical(number);
end

function times = randomDurations(numElements)
    number = randi(255, [numElements 1]);
    times = seconds(number);
end

function dates = randomDatetimes(numElements)
    day = days(randi(255, [numElements 1]));
    dates = datetime(2023, 8, 23) + day;
end