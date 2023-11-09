toolboxFolder = string(getenv("ARROW_MATLAB_TOOLBOX_FOLDER"));
outputFolder = string(getenv("ARROW_MATLAB_TOOLBOX_OUTPUT_FOLDER"));
toolboxVersion = string(getenv("ARROW_MATLAB_TOOLBOX_VERSION"));

identifier = "ARROW-MATLAB-TOOLBOX";

opts = matlab.addons.toolbox.ToolboxOptions(toolboxFolder, identifier);

opts.ToolboxName = "MATLAB Interface to Arrow";
opts.ToolboxVersion = toolboxVersion;
opts.ToolboxMatlabPath = toolboxFolder;

% Set the SupportedPlatforms
opts.SupportedPlatforms.Win64 = true;
opts.SupportedPlatforms.Maci64 = true;
opts.SupportedPlatforms.Glnxa64 = true;
opts.SupportedPlatforms.MatlabOnline = true;

% TODO: Determine what to set the min/max release to
opts.MinimumMatlabRelease = "R2023a";
opts.MaximumMatlabRelease = "";

opts.OutputFile = fullfile(outputFolder, compose("matlab-arrow-%s.mltbx", toolboxVersion));
matlab.addons.toolbox.packageToolbox(opts);
