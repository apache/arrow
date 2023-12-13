/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.maven.plugins;

import java.util.ArrayList;
import java.util.List;

import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * A maven plugin for compiler module-info files in main code with JDK8.
 */
@Mojo(name = "compile", defaultPhase = LifecyclePhase.COMPILE)
public class ModuleInfoCompilerPlugin extends BaseModuleInfoCompilerPlugin {

  @Parameter(defaultValue = "${project.compileSourceRoots}", property = "compileSourceRoots",
      required = true)
  private final List<String> compileSourceRoots = new ArrayList<>();

  @Parameter(defaultValue = "false", property = "skip", required = false)
  private boolean skip = false;

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Override
  protected List<String> getSourceRoots() {
    return compileSourceRoots;
  }

  @Override
  protected boolean skip() {
    return skip;
  }

  @Override
  protected String getOutputDirectory() {
    return project.getBuild().getOutputDirectory();
  }
}
