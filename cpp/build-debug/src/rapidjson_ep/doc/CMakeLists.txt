find_package(Doxygen)

IF(NOT DOXYGEN_FOUND)
    MESSAGE(STATUS "No Doxygen found. Documentation won't be built")
ELSE()
    file(GLOB SOURCES ${CMAKE_CURRENT_LIST_DIR}/../include/*)
    file(GLOB MARKDOWN_DOC ${CMAKE_CURRENT_LIST_DIR}/../doc/*.md)
    list(APPEND MARKDOWN_DOC ${CMAKE_CURRENT_LIST_DIR}/../readme.md)

    CONFIGURE_FILE(Doxyfile.in Doxyfile @ONLY)
    CONFIGURE_FILE(Doxyfile.zh-cn.in Doxyfile.zh-cn @ONLY)

    file(GLOB DOXYFILES ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile*)
    
    add_custom_command(OUTPUT html
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile.zh-cn
        COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/html
        DEPENDS ${MARKDOWN_DOC} ${SOURCES} ${DOXYFILES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/../
        )

    add_custom_target(doc ALL DEPENDS html)
    install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html
        DESTINATION ${DOC_INSTALL_DIR}
        COMPONENT doc)
ENDIF()
