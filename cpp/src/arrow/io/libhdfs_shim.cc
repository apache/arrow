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

// This shim interface to libhdfs (for runtime shared library loading) has been
// adapted from the SFrame project, released under the ASF-compatible 3-clause
// BSD license
//
// Using this required having the $JAVA_HOME and $HADOOP_HOME environment
// variables set, so that libjvm and libhdfs can be located easily

// Copyright (C) 2015 Dato, Inc.
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

#ifdef HAS_HADOOP

#ifndef _WIN32
#include <dlfcn.h>
#else
#include <windows.h>
#include <winsock2.h>

// TODO(wesm): address when/if we add windows support
// #include <util/syserr_reporting.hpp>
#endif

extern "C" {
#include <hdfs.h>
}

#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include <boost/filesystem.hpp>  // NOLINT

#include "arrow/util/status.h"
#include "arrow/util/visibility.h"

namespace fs = boost::filesystem;

extern "C" {

#ifndef _WIN32
static void* libhdfs_handle = NULL;
static void* libjvm_handle = NULL;
#else
static HINSTANCE libhdfs_handle = NULL;
static HINSTANCE libjvm_handle = NULL;
#endif
/*
 * All the shim pointers
 */

// NOTE(wesm): cpplint does not like use of short and other imprecise C types

static hdfsFS (*ptr_hdfsConnectAsUser)(
    const char* host, tPort port, const char* user) = NULL;
static hdfsFS (*ptr_hdfsConnect)(const char* host, tPort port) = NULL;
static int (*ptr_hdfsDisconnect)(hdfsFS fs) = NULL;

static hdfsFile (*ptr_hdfsOpenFile)(hdfsFS fs, const char* path, int flags,
    int bufferSize, short replication, tSize blocksize) = NULL;  // NOLINT

static int (*ptr_hdfsCloseFile)(hdfsFS fs, hdfsFile file) = NULL;
static int (*ptr_hdfsExists)(hdfsFS fs, const char* path) = NULL;
static int (*ptr_hdfsSeek)(hdfsFS fs, hdfsFile file, tOffset desiredPos) = NULL;
static tOffset (*ptr_hdfsTell)(hdfsFS fs, hdfsFile file) = NULL;
static tSize (*ptr_hdfsRead)(hdfsFS fs, hdfsFile file, void* buffer, tSize length) = NULL;
static tSize (*ptr_hdfsPread)(
    hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length) = NULL;
static tSize (*ptr_hdfsWrite)(
    hdfsFS fs, hdfsFile file, const void* buffer, tSize length) = NULL;
static int (*ptr_hdfsFlush)(hdfsFS fs, hdfsFile file) = NULL;
static int (*ptr_hdfsAvailable)(hdfsFS fs, hdfsFile file) = NULL;
static int (*ptr_hdfsCopy)(
    hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) = NULL;
static int (*ptr_hdfsMove)(
    hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) = NULL;
static int (*ptr_hdfsDelete)(hdfsFS fs, const char* path, int recursive) = NULL;
static int (*ptr_hdfsRename)(hdfsFS fs, const char* oldPath, const char* newPath) = NULL;
static char* (*ptr_hdfsGetWorkingDirectory)(
    hdfsFS fs, char* buffer, size_t bufferSize) = NULL;
static int (*ptr_hdfsSetWorkingDirectory)(hdfsFS fs, const char* path) = NULL;
static int (*ptr_hdfsCreateDirectory)(hdfsFS fs, const char* path) = NULL;
static int (*ptr_hdfsSetReplication)(
    hdfsFS fs, const char* path, int16_t replication) = NULL;
static hdfsFileInfo* (*ptr_hdfsListDirectory)(
    hdfsFS fs, const char* path, int* numEntries) = NULL;
static hdfsFileInfo* (*ptr_hdfsGetPathInfo)(hdfsFS fs, const char* path) = NULL;
static void (*ptr_hdfsFreeFileInfo)(hdfsFileInfo* hdfsFileInfo, int numEntries) = NULL;
static char*** (*ptr_hdfsGetHosts)(
    hdfsFS fs, const char* path, tOffset start, tOffset length) = NULL;
static void (*ptr_hdfsFreeHosts)(char*** blockHosts) = NULL;
static tOffset (*ptr_hdfsGetDefaultBlockSize)(hdfsFS fs) = NULL;
static tOffset (*ptr_hdfsGetCapacity)(hdfsFS fs) = NULL;
static tOffset (*ptr_hdfsGetUsed)(hdfsFS fs) = NULL;
static int (*ptr_hdfsChown)(
    hdfsFS fs, const char* path, const char* owner, const char* group) = NULL;
static int (*ptr_hdfsChmod)(hdfsFS fs, const char* path, short mode) = NULL;  // NOLINT
static int (*ptr_hdfsUtime)(hdfsFS fs, const char* path, tTime mtime, tTime atime) = NULL;

// Helper functions for dlopens
static std::vector<fs::path> get_potential_libjvm_paths();
static std::vector<fs::path> get_potential_libhdfs_paths();
static arrow::Status try_dlopen(std::vector<fs::path> potential_paths, const char* name,
#ifndef _WIN32
    void*& out_handle);
#else
    HINSTANCE& out_handle);
#endif

#define GET_SYMBOL(SYMBOL_NAME)                                                  \
  if (!ptr_##SYMBOL_NAME) {                                                      \
    *reinterpret_cast<void**>(&ptr_##SYMBOL_NAME) = get_symbol("" #SYMBOL_NAME); \
  }

static void* get_symbol(const char* symbol) {
  if (libhdfs_handle == NULL) return NULL;
#ifndef _WIN32
  return dlsym(libhdfs_handle, symbol);
#else

  void* ret = reinterpret_cast<void*>(GetProcAddress(libhdfs_handle, symbol));
  if (ret == NULL) {
    // logstream(LOG_INFO) << "GetProcAddress error: "
    //                     << get_last_err_str(GetLastError()) << std::endl;
  }
  return ret;
#endif
}

hdfsFS hdfsConnectAsUser(const char* host, tPort port, const char* user) {
  return ptr_hdfsConnectAsUser(host, port, user);
}

// Returns NULL on failure
hdfsFS hdfsConnect(const char* host, tPort port) {
  if (ptr_hdfsConnect) {
    return ptr_hdfsConnect(host, port);
  } else {
    // TODO: error reporting when shim setup fails
    return NULL;
  }
}

int hdfsDisconnect(hdfsFS fs) {
  return ptr_hdfsDisconnect(fs);
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags, int bufferSize,
    short replication, tSize blocksize) {  // NOLINT
  return ptr_hdfsOpenFile(fs, path, flags, bufferSize, replication, blocksize);
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  return ptr_hdfsCloseFile(fs, file);
}

int hdfsExists(hdfsFS fs, const char* path) {
  return ptr_hdfsExists(fs, path);
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  return ptr_hdfsSeek(fs, file, desiredPos);
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
  return ptr_hdfsTell(fs, file);
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
  return ptr_hdfsRead(fs, file, buffer, length);
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void* buffer, tSize length) {
  return ptr_hdfsPread(fs, file, position, buffer, length);
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length) {
  return ptr_hdfsWrite(fs, file, buffer, length);
}

int hdfsFlush(hdfsFS fs, hdfsFile file) {
  return ptr_hdfsFlush(fs, file);
}

int hdfsAvailable(hdfsFS fs, hdfsFile file) {
  GET_SYMBOL(hdfsAvailable);
  if (ptr_hdfsAvailable)
    return ptr_hdfsAvailable(fs, file);
  else
    return 0;
}

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(hdfsCopy);
  if (ptr_hdfsCopy)
    return ptr_hdfsCopy(srcFS, src, dstFS, dst);
  else
    return 0;
}

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst) {
  GET_SYMBOL(hdfsMove);
  if (ptr_hdfsMove)
    return ptr_hdfsMove(srcFS, src, dstFS, dst);
  else
    return 0;
}

int hdfsDelete(hdfsFS fs, const char* path, int recursive) {
  return ptr_hdfsDelete(fs, path, recursive);
}

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath) {
  GET_SYMBOL(hdfsRename);
  if (ptr_hdfsRename)
    return ptr_hdfsRename(fs, oldPath, newPath);
  else
    return 0;
}

char* hdfsGetWorkingDirectory(hdfsFS fs, char* buffer, size_t bufferSize) {
  GET_SYMBOL(hdfsGetWorkingDirectory);
  if (ptr_hdfsGetWorkingDirectory) {
    return ptr_hdfsGetWorkingDirectory(fs, buffer, bufferSize);
  } else {
    return NULL;
  }
}

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path) {
  GET_SYMBOL(hdfsSetWorkingDirectory);
  if (ptr_hdfsSetWorkingDirectory) {
    return ptr_hdfsSetWorkingDirectory(fs, path);
  } else {
    return 0;
  }
}

int hdfsCreateDirectory(hdfsFS fs, const char* path) {
  return ptr_hdfsCreateDirectory(fs, path);
}

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication) {
  GET_SYMBOL(hdfsSetReplication);
  if (ptr_hdfsSetReplication) {
    return ptr_hdfsSetReplication(fs, path, replication);
  } else {
    return 0;
  }
}

hdfsFileInfo* hdfsListDirectory(hdfsFS fs, const char* path, int* numEntries) {
  return ptr_hdfsListDirectory(fs, path, numEntries);
}

hdfsFileInfo* hdfsGetPathInfo(hdfsFS fs, const char* path) {
  return ptr_hdfsGetPathInfo(fs, path);
}

void hdfsFreeFileInfo(hdfsFileInfo* hdfsFileInfo, int numEntries) {
  ptr_hdfsFreeFileInfo(hdfsFileInfo, numEntries);
}

char*** hdfsGetHosts(hdfsFS fs, const char* path, tOffset start, tOffset length) {
  GET_SYMBOL(hdfsGetHosts);
  if (ptr_hdfsGetHosts) {
    return ptr_hdfsGetHosts(fs, path, start, length);
  } else {
    return NULL;
  }
}

void hdfsFreeHosts(char*** blockHosts) {
  GET_SYMBOL(hdfsFreeHosts);
  if (ptr_hdfsFreeHosts) { ptr_hdfsFreeHosts(blockHosts); }
}

tOffset hdfsGetDefaultBlockSize(hdfsFS fs) {
  GET_SYMBOL(hdfsGetDefaultBlockSize);
  if (ptr_hdfsGetDefaultBlockSize) {
    return ptr_hdfsGetDefaultBlockSize(fs);
  } else {
    return 0;
  }
}

tOffset hdfsGetCapacity(hdfsFS fs) {
  return ptr_hdfsGetCapacity(fs);
}

tOffset hdfsGetUsed(hdfsFS fs) {
  return ptr_hdfsGetUsed(fs);
}

int hdfsChown(hdfsFS fs, const char* path, const char* owner, const char* group) {
  GET_SYMBOL(hdfsChown);
  if (ptr_hdfsChown) {
    return ptr_hdfsChown(fs, path, owner, group);
  } else {
    return 0;
  }
}

int hdfsChmod(hdfsFS fs, const char* path, short mode) {  // NOLINT
  GET_SYMBOL(hdfsChmod);
  if (ptr_hdfsChmod) {
    return ptr_hdfsChmod(fs, path, mode);
  } else {
    return 0;
  }
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
  GET_SYMBOL(hdfsUtime);
  if (ptr_hdfsUtime) {
    return ptr_hdfsUtime(fs, path, mtime, atime);
  } else {
    return 0;
  }
}

static std::vector<fs::path> get_potential_libhdfs_paths() {
  std::vector<fs::path> libhdfs_potential_paths = {
      // find one in the local directory
      fs::path("./libhdfs.so"), fs::path("./hdfs.dll"),
      // find a global libhdfs.so
      fs::path("libhdfs.so"), fs::path("hdfs.dll"),
  };

  const char* hadoop_home = std::getenv("HADOOP_HOME");
  if (hadoop_home != nullptr) {
    auto path = fs::path(hadoop_home) / "lib/native/libhdfs.so";
    libhdfs_potential_paths.push_back(path);
  }
  return libhdfs_potential_paths;
}

static std::vector<fs::path> get_potential_libjvm_paths() {
  std::vector<fs::path> libjvm_potential_paths;

  std::vector<fs::path> search_prefixes;
  std::vector<fs::path> search_suffixes;
  std::string file_name;

// From heuristics
#ifdef __WIN32
  search_prefixes = {""};
  search_suffixes = {"/jre/bin/server", "/bin/server"};
  file_name = "jvm.dll";
#elif __APPLE__
  search_prefixes = {""};
  search_suffixes = {""};
  file_name = "libjvm.dylib";

// SFrame uses /usr/libexec/java_home to find JAVA_HOME; for now we are
// expecting users to set an environment variable
#else
  search_prefixes = {
      "/usr/lib/jvm/default-java",                // ubuntu / debian distros
      "/usr/lib/jvm/java",                        // rhel6
      "/usr/lib/jvm",                             // centos6
      "/usr/lib64/jvm",                           // opensuse 13
      "/usr/local/lib/jvm/default-java",          // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java",                  // alt rhel6
      "/usr/local/lib/jvm",                       // alt centos6
      "/usr/local/lib64/jvm",                     // alt opensuse 13
      "/usr/local/lib/jvm/java-7-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-7-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/local/lib/jvm/java-6-openjdk-amd64",  // alt ubuntu / debian distros
      "/usr/lib/jvm/java-6-openjdk-amd64",        // alt ubuntu / debian distros
      "/usr/lib/jvm/java-7-oracle",               // alt ubuntu
      "/usr/lib/jvm/java-8-oracle",               // alt ubuntu
      "/usr/lib/jvm/java-6-oracle",               // alt ubuntu
      "/usr/local/lib/jvm/java-7-oracle",         // alt ubuntu
      "/usr/local/lib/jvm/java-8-oracle",         // alt ubuntu
      "/usr/local/lib/jvm/java-6-oracle",         // alt ubuntu
      "/usr/lib/jvm/default",                     // alt centos
      "/usr/java/latest",                         // alt centos
  };
  search_suffixes = {"/jre/lib/amd64/server"};
  file_name = "libjvm.so";
#endif
  // From direct environment variable
  char* env_value = NULL;
  if ((env_value = getenv("JAVA_HOME")) != NULL) {
    // logstream(LOG_INFO) << "Found environment variable " << env_name << ": " <<
    // env_value << std::endl;
    search_prefixes.insert(search_prefixes.begin(), env_value);
  }

  // Generate cross product between search_prefixes, search_suffixes, and file_name
  for (auto& prefix : search_prefixes) {
    for (auto& suffix : search_suffixes) {
      auto path = (fs::path(prefix) / fs::path(suffix) / fs::path(file_name));
      libjvm_potential_paths.push_back(path);
    }
  }

  return libjvm_potential_paths;
}

#ifndef _WIN32
static arrow::Status try_dlopen(
    std::vector<fs::path> potential_paths, const char* name, void*& out_handle) {
  std::vector<std::string> error_messages;

  for (auto& i : potential_paths) {
    i.make_preferred();
    // logstream(LOG_INFO) << "Trying " << i.string().c_str() << std::endl;
    out_handle = dlopen(i.native().c_str(), RTLD_NOW | RTLD_LOCAL);

    if (out_handle != NULL) {
      // logstream(LOG_INFO) << "Success!" << std::endl;
      break;
    } else {
      const char* err_msg = dlerror();
      if (err_msg != NULL) {
        error_messages.push_back(std::string(err_msg));
      } else {
        error_messages.push_back(std::string(" returned NULL"));
      }
    }
  }

  if (out_handle == NULL) {
    std::stringstream ss;
    ss << "Unable to load " << name;
    return arrow::Status::IOError(ss.str());
  }

  return arrow::Status::OK();
}

#else
static arrow::Status try_dlopen(
    std::vector<fs::path> potential_paths, const char* name, HINSTANCE& out_handle) {
  std::vector<std::string> error_messages;

  for (auto& i : potential_paths) {
    i.make_preferred();
    // logstream(LOG_INFO) << "Trying " << i.string().c_str() << std::endl;

    out_handle = LoadLibrary(i.string().c_str());

    if (out_handle != NULL) {
      // logstream(LOG_INFO) << "Success!" << std::endl;
      break;
    } else {
      // error_messages.push_back(get_last_err_str(GetLastError()));
    }
  }

  if (out_handle == NULL) {
    std::stringstream ss;
    ss << "Unable to load " << name;
    return arrow::Status::IOError(ss.str());
  }

  return arrow::Status::OK();
}
#endif  // _WIN32

}  // extern "C"

#define GET_SYMBOL_REQUIRED(SYMBOL_NAME)                                           \
  do {                                                                             \
    if (!ptr_##SYMBOL_NAME) {                                                      \
      *reinterpret_cast<void**>(&ptr_##SYMBOL_NAME) = get_symbol("" #SYMBOL_NAME); \
    }                                                                              \
    if (!ptr_##SYMBOL_NAME)                                                        \
      return Status::IOError("Getting symbol " #SYMBOL_NAME "failed");             \
  } while (0)

namespace arrow {
namespace io {

Status ARROW_EXPORT ConnectLibHdfs() {
  static std::mutex lock;
  std::lock_guard<std::mutex> guard(lock);

  static bool shim_attempted = false;
  if (!shim_attempted) {
    shim_attempted = true;

    std::vector<fs::path> libjvm_potential_paths = get_potential_libjvm_paths();
    RETURN_NOT_OK(try_dlopen(libjvm_potential_paths, "libjvm", libjvm_handle));

    std::vector<fs::path> libhdfs_potential_paths = get_potential_libhdfs_paths();
    RETURN_NOT_OK(try_dlopen(libhdfs_potential_paths, "libhdfs", libhdfs_handle));
  } else if (libhdfs_handle == nullptr) {
    return Status::IOError("Prior attempt to load libhdfs failed");
  }

  GET_SYMBOL_REQUIRED(hdfsConnect);
  GET_SYMBOL_REQUIRED(hdfsConnectAsUser);
  GET_SYMBOL_REQUIRED(hdfsCreateDirectory);
  GET_SYMBOL_REQUIRED(hdfsDelete);
  GET_SYMBOL_REQUIRED(hdfsDisconnect);
  GET_SYMBOL_REQUIRED(hdfsExists);
  GET_SYMBOL_REQUIRED(hdfsFreeFileInfo);
  GET_SYMBOL_REQUIRED(hdfsGetCapacity);
  GET_SYMBOL_REQUIRED(hdfsGetUsed);
  GET_SYMBOL_REQUIRED(hdfsGetPathInfo);
  GET_SYMBOL_REQUIRED(hdfsListDirectory);

  // File methods
  GET_SYMBOL_REQUIRED(hdfsCloseFile);
  GET_SYMBOL_REQUIRED(hdfsFlush);
  GET_SYMBOL_REQUIRED(hdfsOpenFile);
  GET_SYMBOL_REQUIRED(hdfsRead);
  GET_SYMBOL_REQUIRED(hdfsPread);
  GET_SYMBOL_REQUIRED(hdfsSeek);
  GET_SYMBOL_REQUIRED(hdfsTell);
  GET_SYMBOL_REQUIRED(hdfsWrite);

  return Status::OK();
}

}  // namespace io
}  // namespace arrow

#endif  // HAS_HADOOP
