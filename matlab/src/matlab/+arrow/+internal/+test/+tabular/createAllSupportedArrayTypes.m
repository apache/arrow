%CREATEALLSUPPORTEDARRAYTYPES Creates a MATLAB cell array containing all 
%the concrete subclasses of arrow.array.Array. Returns a cell array
%containing the MATLAB data from which the arrow arrays were generated
%as second output argument.

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

function [arrowArrays, matlabData] = createAllSupportedArrayTypes(opts)
    arguments
        opts.NumRows(1, 1) {mustBeFinite, mustBeNonnegative} = 3;  
    end

    import arrow.type.ID
    import arrow.array.*

    classes = getArrayClassNames();
    numClasses = numel(classes);
    arrowArrays = cell(numClasses, 1);
    matlabData  = cell(numClasses, 1);
    
    numericArrayToMatlabTypeDict = getArrowArrayToMatlabTypeDictionary();

    for ii = 1:numel(classes)
        name = classes(ii);
        if name == "arrow.array.BooleanArray"
            matlabData{ii} = randomLogicals(opts.NumRows);
            arrowArrays{ii} = BooleanArray.fromMATLAB(matlabData{ii});
        elseif isKey(numericArrayToMatlabTypeDict, name)
            matlabType = numericArrayToMatlabTypeDict(name);
            matlabData{ii} = randomNumbers(matlabType, opts.NumRows);
            cmd = compose("%s.fromMATLAB(matlabData{ii})", name);
            arrowArrays{ii} = eval(cmd);
        elseif name == "arrow.array.StringArray"
            matlabData{ii} = randomStrings(opts.NumRows);
            arrowArrays{ii} = StringArray.fromMATLAB(matlabData{ii});
        elseif name == "arrow.array.TimestampArray"
            matlabData{ii} = randomDatetimes(opts.NumRows);
            arrowArrays{ii} = TimestampArray.fromMATLAB(matlabData{ii});
        elseif name == "arrow.array.Time32Array"
            matlabData{ii} = randomDurations(opts.NumRows);
            arrowArrays{ii} = Time32Array.fromMATLAB(matlabData{ii});
        else
            error("arrow:test:SupportedArrayCase", ...
                "Missing if-branch for array class " + name); 
        end
    end
end

function classes = getArrayClassNames()
    metaClass = meta.package.fromName("arrow.array").ClassList;

    % Removes all Abstract classes from the list of all subclasses
    abstract = [metaClass.Abstract];
    metaClass(abstract) = [];
    classes = string({metaClass.Name});
end

function dict = getArrowArrayToMatlabTypeDictionary()
    pkg = "arrow.array";
    unsignedTypes = compose("UInt%d", power(2, 3:6));
    signedTypes = compose("Int%d", power(2, 3:6));
    floatTypes = compose("Float%d", power(2, 5:6));
    numericTypes = [unsignedTypes, signedTypes, floatTypes];
    keys = compose("%s.%sArray", pkg, numericTypes);
    
    values = [lower([unsignedTypes, signedTypes]) "single" "double"];
    dict = dictionary(keys, values);
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