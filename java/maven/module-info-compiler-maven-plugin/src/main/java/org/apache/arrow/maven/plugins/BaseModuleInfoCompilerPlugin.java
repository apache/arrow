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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.glavo.mic.ModuleInfoCompiler;

/**
 * Compiles the first module-info.java file in the project purely syntactically.
 */
public abstract class BaseModuleInfoCompilerPlugin extends AbstractMojo {
  protected abstract List<String> getSourceRoots();

  protected abstract boolean skip();

  protected abstract String getOutputDirectory();

  @Override
  public void execute() throws MojoExecutionException {
    if (skip()) {
      getLog().info("Skipping module-info-compiler-maven-plugin");
      return;
    }

    Optional<File> moduleInfoFile = findFirstModuleInfo(getSourceRoots());
    if (moduleInfoFile.isPresent()) {
      // The compiled module-info.class file goes into target/classes/module-info/main
      Path outputDir = Paths.get(getOutputDirectory());

      outputDir.toFile().mkdirs();
      Path targetPath = outputDir.resolve("module-info.class");

      // Invoke the compiler,
      ModuleInfoCompiler compiler = new ModuleInfoCompiler();
      try (Reader reader = new InputStreamReader(Files.newInputStream(moduleInfoFile.get().toPath()),
          StandardCharsets.UTF_8);
           OutputStream output = Files.newOutputStream(targetPath)) {
        compiler.compile(reader, output);
        getLog().info("Successfully wrote module-info.class file.");
      } catch (IOException ex) {
        throw new MojoExecutionException("Error compiling module-info.java", ex);
      }
    } else {
      getLog().info("No module-info.java file found. module-info.class file was not generated.");
    }
  }

  /**
   * Finds the first module-info.java file in the set of source directories.
   */
  private Optional<File> findFirstModuleInfo(List<String> sourceDirectories) {
    if (sourceDirectories == null) {
      return Optional.empty();
    }

    return sourceDirectories.stream().map(Paths::get)
        .map(sourcePath ->
            sourcePath.toFile().listFiles(file ->
                file.getName().equals("module-info.java")))
        .filter(matchingFiles -> matchingFiles != null && matchingFiles.length != 0)
        .map(matchingFiles -> matchingFiles[0])
        .findAny();
  }
}
