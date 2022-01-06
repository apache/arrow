function addInstallDirToSearchPath(installDirPath)
    % addInstallDirToSearchPath Add the input path, installDirPath, to the 
    %                           MATLAB Search Path and save.
    %
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

    addpath(installDirPath);
    status = savepath(fullfile(matlabroot, "toolbox", "local", "pathdef.m"));

    % Return exit code 1 to indicate failure and 0 to indicate the path has
    % been saved successfully.
    if status == 0
        disp("Sucessfully added directory to the MATLAB Search Path: " + installDirPath);
        quit(0);
    else
        quit(1);
    end
end
