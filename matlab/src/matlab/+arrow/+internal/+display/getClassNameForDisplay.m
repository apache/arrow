%GETCLASSNAMEFORDISPLAY Returns a display-ready class name string, optionally
%with a hyperlink when the MATLAB desktop is available.

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

function displayName = getClassNameForDisplay(fullClassName, opts)
    arguments
        fullClassName(1, 1) string
        opts.BoldFont(1, 1) logical = true
    end
    % Extract the short class name (e.g., "BooleanArray" from "arrow.array.BooleanArray")
    parts = split(fullClassName, ".");
    shortName = parts(end);
    displayName = arrow.internal.display.makeLinkString( ...
        HelpTarget=fullClassName, ...
        Text=shortName, ...
        BoldFont=opts.BoldFont);
end
