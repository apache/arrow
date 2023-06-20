set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(tools /home/gcc-user/x-tools/armeb-unknown-linux-gnueabi/)
set(CMAKE_C_COMPILER ${tools}/bin/armeb-unknown-linux-gnueabi-gcc)
set(CMAKE_CXX_COMPILER ${tools}/bin/armeb-unknown-linux-gnueabi-g++)

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)