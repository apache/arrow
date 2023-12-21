############################################################################
# Copyright (c) Johan Mabille, Sylvain Corlay, Wolf Vollprecht and         #
# Martin Renou                                                             #
# Copyright (c) QuantStack                                                 #
#                                                                          #
# Distributed under the terms of the BSD 3-Clause License.                 #
#                                                                          #
# The full license is in the file LICENSE, distributed with this software. #
############################################################################

# xsimd cmake module
# This module sets the following variables in your project::
#
#   xsimd_FOUND - true if xsimd found on the system
#   xsimd_INCLUDE_DIRS - the directory containing xsimd headers
#   xsimd_LIBRARY - empty


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was xsimdConfig.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

if(NOT TARGET xsimd)
    set(xsimd_ENABLE_XTL_COMPLEX OFF)
    if(xsimd_ENABLE_XTL_COMPLEX)
        include(CMakeFindDependencyMacro)
        find_dependency(xtl)
    endif()

    include("${CMAKE_CURRENT_LIST_DIR}/xsimdTargets.cmake")
    get_target_property(xsimd_INCLUDE_DIRS xsimd INTERFACE_INCLUDE_DIRECTORIES)
endif()
