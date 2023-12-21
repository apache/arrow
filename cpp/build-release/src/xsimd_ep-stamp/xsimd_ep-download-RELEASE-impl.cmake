set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-P;/Users/simon/Desktop/arrow/arrowSF/cpp/build-release/src/xsimd_ep-stamp/download-xsimd_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-P;/Users/simon/Desktop/arrow/arrowSF/cpp/build-release/src/xsimd_ep-stamp/verify-xsimd_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-P;/Users/simon/Desktop/arrow/arrowSF/cpp/build-release/src/xsimd_ep-stamp/extract-xsimd_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
