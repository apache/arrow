%GETHEADER Generates the display header for arrow.array.Array classes

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

function header = getHeader(className, numElements, numNulls)
    import arrow.internal.display.pluralizeStringIfNeeded
    elementString = pluralizeStringIfNeeded(numElements, "element");

    nullString = pluralizeStringIfNeeded(numNulls, "null value");
    
    numString = "%d";
    if usejava("desktop")
        % Bold the number of elements and nulls if the desktop is enabled
        numString = compose("<strong>%s</strong>", numString);
    end

    formatSpec = "  %s with " + numString + " %s and " + numString + " %s";
    if numElements > 0
        formatSpec = formatSpec + ":";
    end
    formatSpec = formatSpec + newline;
    
    header = compose(formatSpec, className, numElements, elementString, numNulls, nullString);
    header = char(header);
end