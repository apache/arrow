add_custom_target(cls_arrow)
arrow_install_all_headers("arrow/adapters/skyhookdm-ceph-cls")

# cls_arrow
set(ARROW_CLS_SRCS cls_arrow.cc)

set(ARROW_CLS_LINK_STATIC arrow_dataset_static)
set(ARROW_CLS_LINK_SHARED arrow_dataset_shared)

find_package(librados REQUIRED)
include_directories(${LIBRADOS_INCLUDE_DIR})

add_arrow_lib(cls_arrow
              CMAKE_PACKAGE_NAME
              ArrowCls
              PKG_CONFIG_NAME
              arrow-cls
              OUTPUTS
              ARROW_CLS_LIBRARIES
              SOURCES
              ${ARROW_CLS_SRCS}
              PRIVATE_INCLUDES
              ${ARROW_CLS_PRIVATE_INCLUDES}
              SHARED_LINK_LIBS
              ${ARROW_CLS_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_CLS_LINK_STATIC})


if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

foreach(LIB_TARGET ${ARROW_DATASET_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_DS_EXPORTING)
endforeach()

# cls_arrow_test
add_arrow_test(cls_arrow_test
               SOURCES
               cls_arrow_test.cc
               cls_arrow_test_utils.cc
               EXTRA_LINK_LIBS
               ${ARROW_DATASET_TEST_LINK_LIBS}
               PREFIX
               "arrow-cls")
