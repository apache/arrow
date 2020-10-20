set(cls_dir ${CMAKE_INSTALL_LIBDIR}/rados-classes)

# cls_arrow
add_library(cls_arrow SHARED cls_arrow.cc cls_arrow_utils.cc)
set_target_properties(cls_arrow PROPERTIES
        VERSION "1.0.0"
        SOVERSION "1"
        INSTALL_RPATH ""
        CXX_VISIBILITY_PRESET hidden
        )
target_link_libraries(cls_arrow
        arrow
        arrow_dataset
        )

install(TARGETS cls_arrow DESTINATION ${cls_dir})

# cls_arrow_test
add_executable(cls_arrow_test
        cls_arrow_test.cc
        test_utils.cc
        cls_arrow_utils.cc
        )

target_link_libraries(cls_arrow_test
        arrow
        arrow_dataset
        ${LIBRADOS_LIBRARIES}
        ${GTEST_MAIN_LIBRARIES}
        ${GTEST_INCLUDE_DIRS}
        ${GTEST_LIBRARIES}
        )

install(TARGETS
        cls_arrow_test
        RUNTIME DESTINATION bin
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        )
