// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Utils for getting "external" test data dirs for arrow and parquet, to make testing,
//! benchmark and example easier.

use std::{env, error::Error, fs, path::PathBuf};

/// Gets arrow test data dir.
/// The env `ARROW_TEST_DATA` has higher prior to the default git submodule dir.
#[inline]
#[allow(non_snake_case)]
pub fn ARROW_TEST_DATA() -> Result<String, Box<dyn Error>> {
    find_dir("ARROW_TEST_DATA", PathBuf::from("."), "testing/data")
}

/// Gets parquet test data dir.
/// The env `PARQUET_TEST_DATA` has higher prior to the default git submodule dir.
#[inline]
#[allow(non_snake_case)]
pub fn PARQUET_TEST_DATA() -> Result<String, Box<dyn Error>> {
    find_dir(
        "PARQUET_TEST_DATA",
        PathBuf::from("."),
        "cpp/submodules/parquet-testing/data",
    )
}

/// Gets full path dir defined by `env_key` or `relative_dir` relative to git workspace.
/// The `from_dir` is start point to search ".git" from it's ancestors, MUST be set as
/// current dir unless called by tests.
fn find_dir(
    env_key: &'static str,
    from_dir: PathBuf,
    relative_dir: &'static str,
) -> Result<String, Box<dyn std::error::Error>> {
    const TIP: &str = "run: `git submodule update --init`";

    if let Ok(mut v) = env::var(env_key) {
        v = v.trim().to_string();
        if !v.is_empty() {
            let pb = PathBuf::from(&v);
            if pb.exists() {
                return Ok(pb.display().to_string());
            }
            return Err(format!(
                "dir {} defined by env {} not exists, {}",
                v, env_key, TIP
            )
            .into());
        }
    }

    let dir = fs::canonicalize(from_dir)?;

    // ancestors() includes self.
    for ancestor in dir.ancestors() {
        if ancestor.join(".git").exists() {
            let sub_dir = ancestor.join(relative_dir);
            if sub_dir.exists() {
                return Ok(sub_dir.display().to_string());
            } else {
                return Err(format!(
                    "data dir {:?} => {:?} not exists, {}",
                    relative_dir, sub_dir, TIP
                )
                .into());
            }
        }
    }

    Err(format!("dir {:?} is not in git workspace", dir.to_str()).into())
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_all() {
        const KEY: &str = "ARROW-MOCK-KEY-1";
        const NON_EXISTING: &str = "arrow:this-dir-should-not-exist";

        // Remove env if exists.
        if env::var_os(KEY).is_some() {
            env::remove_var(KEY);
        }

        let tmp_dir = TempDir::new().unwrap();
        let tmp_dir_path = tmp_dir.path();
        let sub_dir = tmp_dir_path.join("sub");
        fs::create_dir(&sub_dir).unwrap();

        let res = super::find_dir(KEY, sub_dir.to_owned(), NON_EXISTING);
        debug_assert!(res.is_err());

        // Set the env value as a non existing dir.
        env::set_var(KEY, NON_EXISTING);
        let res = super::find_dir(KEY, sub_dir.to_owned(), NON_EXISTING);
        debug_assert!(res.is_err());

        // Set the env value as a white spaces.
        env::set_var(KEY, " ");
        let res = super::find_dir(KEY, sub_dir.to_owned(), NON_EXISTING);
        debug_assert!(res.is_err());

        // Create .git file in sub dir.
        let dot_git = sub_dir.join(".git");
        fs::File::create(&dot_git).unwrap();

        // Set env var as sub dir.
        env::set_var(KEY, sub_dir.clone());

        let res = super::find_dir(KEY, sub_dir.to_owned(), "");
        debug_assert!(res.is_ok());

        // Empty env value: KEY=""
        env::set_var(KEY, "");

        let res = super::find_dir(KEY, sub_dir.to_owned(), NON_EXISTING);
        debug_assert!(res.is_err());

        // Remove env.
        env::remove_var(KEY);

        let res = super::find_dir(KEY, sub_dir.to_owned(), NON_EXISTING);
        debug_assert!(res.is_err());

        // Remove file ".git" from sub dir.
        fs::remove_file(&dot_git).unwrap();

        // Create ".git" dir in tmp dir.
        let dot_git = tmp_dir_path.join(".git");
        fs::create_dir(&dot_git).unwrap();

        let res = super::find_dir(KEY, sub_dir, "");
        debug_assert!(res.is_ok());

        let res = super::ARROW_TEST_DATA();
        debug_assert!(res.is_ok());

        let res = super::PARQUET_TEST_DATA();
        debug_assert!(res.is_ok());

        // Finally, remove the tmp dir.
        fs::remove_dir_all(tmp_dir).unwrap();
    }
}
