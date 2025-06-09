if(ARROW_S3)
  find_package(AWSSDK REQUIRED)
  # Fix issue where scripts expect a variable called "AWSSDK_LINK_LIBRARIES"
  # which is not defined by the generated AWSSDKConfig.cmake
  if(NOT DEFINED AWSSDK_LINK_LIBRARIES)
    set(AWSSDK_LINK_LIBRARIES "${AWSSDK_LIBRARIES}")
  endif()

  # Causes logic used for generated .pc file to not run
  # avoiding instropection of target `aws-cpp-sdk::aws-cpp-sdk`
  # This is fine because the generated .pc file is not of use
  set(AWSSDK_SOURCE "conan")
endif()
