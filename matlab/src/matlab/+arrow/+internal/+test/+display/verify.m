%VERIFY Utility function used to verify object display.

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

function verify(testCase, actualDisplay, expectedDisplay)
    % When the MATLAB GUI is running, '×' (char(215)) is used as
    % the delimiter between dimension values. However, when the 
    % GUI is not running, 'x' (char(120)) is used as the delimiter.
    % To account for this discrepancy, check if actualDisplay 
    % contains char(215). If not, replace all instances of
    % char(215) in expectedDisplay with char(120).

    tf = contains(actualDisplay, char(215));
    if ~tf
        idx = strfind(expectedDisplay, char(215));
        expectedDisplay(idx) = char(120);
    end
    testCase.verifyEqual(actualDisplay, expectedDisplay);
end
