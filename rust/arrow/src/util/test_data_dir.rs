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

use std::{env, error::Error, path::PathBuf, process::Command};

const ARROW_ENV: &str = "ARROW_TEST_DATA";
const ARROW_SUBMODULE: &str = "testing";
const ARROW_SUBMODULE_DATA: &str = "data";

const PARQUET_ENV: &str = "PARQUET_TEST_DATA";
const PARQUET_SUBMODULE: &str = "cpp/submodules/parquet-testing";
const PARQUET_SUBMODULE_DATA: &str = "data";

/// Gets arrow test data dir. Panic on error.
#[allow(non_snake_case)]
pub fn ARROW_TEST_DATA() -> String {
    let getter = TestDataGetter {
        env_key: ARROW_ENV,
        submodule: ARROW_SUBMODULE,
        submodule_data: ARROW_SUBMODULE_DATA,
    };
    let res = getter.by_env_or_submodule();
    match res {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!(format!("{}(): {}", getter.env_key, err)),
    }
}

/// Gets parquet test data dir. Panic on error.
#[allow(non_snake_case)]
pub fn PARQUET_TEST_DATA() -> String {
    let getter = TestDataGetter {
        env_key: PARQUET_ENV,
        submodule: PARQUET_SUBMODULE,
        submodule_data: PARQUET_SUBMODULE_DATA,
    };
    let res = getter.by_env_or_submodule();
    match res {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!(format!("{}(): {}", getter.env_key, err)),
    }
}

/// TestDataGetter is used to get data dir for arrow or parquet.
struct TestDataGetter<'a> {
    env_key: &'a str,
    submodule: &'a str,
    submodule_data: &'a str,
}

impl TestDataGetter<'_> {
    // NOTE: `std::fs::canonicalize` returns UNC paths on Windows, so don't use `canonicalize`.
    // Ref: https://github.com/rust-lang/rust/issues/42869

    /// Gets data dir defined by `env_key`.
    /// Return `None` if `env_key` is undefined or it's value is trimmed to empty string.
    fn by_env(&self) -> Option<Result<PathBuf, Box<dyn Error>>> {
        let env_val = env::var(self.env_key);
        if env_val.is_err() {
            return None;
        }

        let env_val = env_val.unwrap().trim().to_string();
        if env_val.is_empty() {
            return None;
        }

        let data_dir = PathBuf::from(&env_val);
        if data_dir.is_dir() {
            Some(Ok(data_dir))
        } else {
            Some(Err(format!(
                "`{}` defined by env `{}` not exists or is not directory",
                data_dir.display().to_string(),
                self.env_key
            )
            .into()))
        }
    }

    /// Gets data dir from `git top-level dir`, `submodule` and `data_dir`.
    /// The git top-level dir will be get by `git rev-parse --show-toplevel`.
    fn by_submodule(&self) -> Result<PathBuf, Box<dyn Error>> {
        let args = vec!["rev-parse", "--show-toplevel"];

        if let Ok(out) = Command::new("git").args(&args).output() {
            if out.status.success() {
                let top = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
                let sub = top.join(self.submodule);

                if sub.exists() {
                    let mut data = sub.join(self.submodule_data);
                    if data.is_relative() {
                        data = env::current_dir().unwrap().join(data)
                    }
                    if data.exists() {
                        return Ok(data);
                    } else {
                        return Err(format!(
                            "submodule data dir `{}` not exists, did you run `git submodule update --init`?",
                            data.to_string_lossy().to_string()
                        )
                        .into());
                    }
                } else {
                    let s = sub.to_string_lossy();
                    return Err(format!("submodule dir `{}` not exists, did you run `git submodule update --init`?", s).into());
                }
            }
        }

        Err(format!(
            "failed to get git top-level dir with `git {}`",
            args.join(" ")
        )
        .into())
    }

    /// Gets data dir by env or submodule.
    /// If `by_env()` returns `None`, then call `by_submodule`.
    fn by_env_or_submodule(&self) -> Result<PathBuf, Box<dyn Error>> {
        match self.by_env() {
            Some(dir_or_error) => dir_or_error,
            None => self.by_submodule(),
        }
    }
}

impl Default for TestDataGetter<'_> {
    fn default() -> Self {
        TestDataGetter {
            env_key: "",
            submodule: "",
            submodule_data: "",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    // NOTE: DON'T update env vars `ARROW_TEST_DATA` and `PARQUET_TEST_DATA`, because that
    // MAY break other tests.

    #[test]
    fn test_by_env() {
        let env_key = "test_by_env";
        let mut getter = TestDataGetter::default();
        getter.env_key = env_key;

        env::remove_var(env_key);
        let res = getter.by_env();
        debug_assert!(res.is_none());

        env::set_var(env_key, "");
        let res = getter.by_env();
        debug_assert!(res.is_none());

        env::set_var(env_key, " ");
        let res = getter.by_env();
        debug_assert!(res.is_none());

        env::set_var(env_key, "non-existing-env-dir");
        let res = getter.by_env();
        debug_assert!(res.is_some());
        debug_assert!(res.unwrap().is_err());

        env::set_var(env_key, ".");
        let res = getter.by_env();
        debug_assert!(res.is_some());
        debug_assert!(res.unwrap().is_ok());

        env::set_var(env_key, "\t. ");
        let res = getter.by_env();
        debug_assert!(res.is_some());
        debug_assert!(res.unwrap().is_ok());
    }

    #[test]
    fn test_by_submodule() {
        use super::*;

        let mut getter = TestDataGetter::default();
        getter.submodule = "non-existing-submodule";
        getter.submodule_data = "any";

        let res = getter.by_submodule();
        debug_assert!(res.is_err());

        getter.submodule = ARROW_SUBMODULE;
        getter.submodule_data = "non-existing-data-dir";
        let res = getter.by_submodule();
        debug_assert!(res.is_err());

        getter.submodule_data = ARROW_SUBMODULE_DATA;
        let res = getter.by_submodule();
        debug_assert!(res.is_ok());
    }

    #[test]
    fn test_by_env_or_submodule() {
        let env_key = "test_by_env_or_submodule";
        let mut getter = TestDataGetter::default();
        getter.env_key = env_key;

        let args = vec!["rev-parse", "--show-toplevel"];
        let out = Command::new("git").args(args).output().unwrap();

        let expected = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
            .join(ARROW_SUBMODULE)
            .join(ARROW_SUBMODULE_DATA);

        env::set_var(env_key, &expected);
        let res = getter.by_env_or_submodule();
        debug_assert!(res.is_ok());
        assert_eq!(res.unwrap(), expected);

        env::set_var(env_key, "non-existing-env-dir");
        let res = getter.by_env_or_submodule();
        debug_assert!(res.is_err());

        env::remove_var(env_key);

        getter.submodule = "non-existing-submodule";
        let res = getter.by_env_or_submodule();
        debug_assert!(res.is_err());

        getter.submodule = ARROW_SUBMODULE;

        getter.submodule_data = "non-existing-data-dir";
        let res = getter.by_env_or_submodule();
        debug_assert!(res.is_err());

        getter.submodule_data = ARROW_SUBMODULE_DATA;
        let res = getter.by_env_or_submodule();
        debug_assert!(res.is_ok());
        assert_eq!(res.unwrap(), expected);
    }
}
