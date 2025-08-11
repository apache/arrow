# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file LICENSE.rst or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

function(check_file_hash has_hash hash_is_good)
  if("${has_hash}" STREQUAL "")
    message(FATAL_ERROR "has_hash Can't be empty")
  endif()

  if("${hash_is_good}" STREQUAL "")
    message(FATAL_ERROR "hash_is_good Can't be empty")
  endif()

  if("SHA256" STREQUAL "")
    # No check
    set("${has_hash}" FALSE PARENT_SCOPE)
    set("${hash_is_good}" FALSE PARENT_SCOPE)
    return()
  endif()

  set("${has_hash}" TRUE PARENT_SCOPE)

  message(VERBOSE "verifying file...
       file='/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz'")

  file("SHA256" "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz" actual_value)

  if(NOT "${actual_value}" STREQUAL "f989a862f694e7dbb695925ddb7c4ce06aa6c51aca945105c075139aed7e55a2")
    set("${hash_is_good}" FALSE PARENT_SCOPE)
    message(VERBOSE "SHA256 hash of
    /tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz
  does not match expected value
    expected: 'f989a862f694e7dbb695925ddb7c4ce06aa6c51aca945105c075139aed7e55a2'
      actual: '${actual_value}'")
  else()
    set("${hash_is_good}" TRUE PARENT_SCOPE)
  endif()
endfunction()

function(sleep_before_download attempt)
  if(attempt EQUAL 0)
    return()
  endif()

  if(attempt EQUAL 1)
    message(VERBOSE "Retrying...")
    return()
  endif()

  set(sleep_seconds 0)

  if(attempt EQUAL 2)
    set(sleep_seconds 5)
  elseif(attempt EQUAL 3)
    set(sleep_seconds 5)
  elseif(attempt EQUAL 4)
    set(sleep_seconds 15)
  elseif(attempt EQUAL 5)
    set(sleep_seconds 60)
  elseif(attempt EQUAL 6)
    set(sleep_seconds 90)
  elseif(attempt EQUAL 7)
    set(sleep_seconds 300)
  else()
    set(sleep_seconds 1200)
  endif()

  message(VERBOSE "Retry after ${sleep_seconds} seconds (attempt #${attempt}) ...")

  execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep "${sleep_seconds}")
endfunction()

if(EXISTS "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz")
  check_file_hash(has_hash hash_is_good)
  if(has_hash)
    if(hash_is_good)
      message(VERBOSE "File already exists and hash match (skip download):
  file='/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz'
  SHA256='f989a862f694e7dbb695925ddb7c4ce06aa6c51aca945105c075139aed7e55a2'"
      )
      return()
    else()
      message(VERBOSE "File already exists but hash mismatch. Removing...")
      file(REMOVE "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz")
    endif()
  else()
    message(VERBOSE "File already exists but no hash specified (use URL_HASH):
  file='/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz'
Old file will be removed and new file downloaded from URL."
    )
    file(REMOVE "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz")
  endif()
endif()

set(retry_number 5)

message(VERBOSE "Downloading...
   dst='/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz'
   timeout='none'
   inactivity timeout='none'"
)
set(download_retry_codes 7 6 8 15 28 35)
set(skip_url_list)
set(status_code)
foreach(i RANGE ${retry_number})
  if(status_code IN_LIST download_retry_codes)
    sleep_before_download(${i})
  endif()
  foreach(url IN ITEMS [====[https://github.com/substrait-io/substrait/archive/v0.44.0.tar.gz]====])
    if(NOT url IN_LIST skip_url_list)
      message(VERBOSE "Using src='${url}'")

      
      
      
      
      

      file(
        DOWNLOAD
        "${url}" "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz"
        SHOW_PROGRESS
        # no TIMEOUT
        # no INACTIVITY_TIMEOUT
        STATUS status
        LOG log
        
        
        )

      list(GET status 0 status_code)
      list(GET status 1 status_string)

      if(status_code EQUAL 0)
        check_file_hash(has_hash hash_is_good)
        if(has_hash AND NOT hash_is_good)
          message(VERBOSE "Hash mismatch, removing...")
          file(REMOVE "/tmp/eugo/arrow/cpp/eugo_build/substrait_ep-prefix/src/v0.44.0.tar.gz")
        else()
          message(VERBOSE "Downloading... done")
          return()
        endif()
      else()
        string(APPEND logFailedURLs "error: downloading '${url}' failed
        status_code: ${status_code}
        status_string: ${status_string}
        log:
        --- LOG BEGIN ---
        ${log}
        --- LOG END ---
        "
        )
      if(NOT status_code IN_LIST download_retry_codes)
        list(APPEND skip_url_list "${url}")
        break()
      endif()
    endif()
  endif()
  endforeach()
endforeach()

message(FATAL_ERROR "Each download failed!
  ${logFailedURLs}
  "
)
