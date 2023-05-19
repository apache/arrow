function validElements = parseValidElements(data, detectNulls, nullDetectionFcn)
    % Creates the validElements logical vector based on data if
    % detectNulls is true. If so, data is based to the NullDetectionFcn.
    % Otherwise, all elements in data are valid.

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

    if detectNulls
        % TODO: consider making validElements empty if everything is valid.
        validElements = ~nullDetectionFcn(data);
    
        % If data was empty, it's ok for validElements to be a 0x0 
        % logical array. 
        expectedShape = 'vector';
        if isempty(validElements)
            expectedShape = '2d';
        end
        validateattributes(validElements, "logical", {expectedShape, 'numel', numel(data)});
    else
        % TODO: consider making this an empty array if everything is valid
        validElements = true([numel(data) 1]);
    end
end
