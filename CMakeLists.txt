cmake_minimum_required(VERSION 2.8)

include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckIncludeFiles)
include(CheckCSourceCompiles)

project(pgstat C)

set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH "devel")
set(PROJECT_VERSION
    ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

set(CMAKE_C_FLAGS "-Wall")

# Get PostgreSQL include and library directories.

execute_process(
    COMMAND pg_config --includedir
    OUTPUT_VARIABLE PGINCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message("-- pg_config --includedir - ${PGINCLUDEDIR}")
if(PGINCLUDEDIR)
    set(PGINCLUDE "-I${PGINCLUDEDIR}")
endif(PGINCLUDEDIR)

execute_process(
    COMMAND pg_config --libdir
    OUTPUT_VARIABLE PGLIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message("-- pg_config --libdir - ${PGLIBDIR}")

# Check for include files.

check_include_files("stdlib.h;stdarg.h;string.h" STDC_HEADERS)

check_include_files("string.h" HAVE_STRING_H)
check_include_files("unistd.h" HAVE_UNISTD_H)

# Check for library functions.

check_function_exists(getopt HAVE_GETOPT)
check_function_exists(snprintf HAVE_SNPRINTF)
check_function_exists(strchr HAVE_STRCHR)

# Set appropriate compile flags.

set_source_files_properties(
    dbxact.c
    engine.c
    main.c
    pg.c
    sprompt.c
    PROPERTIES COMPILE_FLAGS "${PGINCLUDE}"
)

add_executable(
    ${PROJECT_NAME}
    dbxact.c
    engine.c
    main.c
    pg.c
    sprompt.c
)

# Determine appropriate linker flags.

find_library(LIBPQ pq PATHS ${PGLIBDIR})
if(LIBPQ)
    target_link_libraries(${PROJECT_NAME} ${LIBPQ})
endif(LIBPQ)

find_library(LIBM m)
if(LIBM)
    target_link_libraries(${PROJECT_NAME} ${LIBM})
endif(LIBM)

find_library(LIBTERMCAP termcap)
if(LIBTERMCAP)
    target_link_libraries(${PROJECT_NAME} ${LIBTERMCAP})
else(LIBTERMCAP)
    find_library(LIBCURSES curses)
    if(LIBCURSES)
        target_link_libraries(${PROJECT_NAME} ${LIBCURSES})
    else(LIBCURSES)
        find_library(LIBNCURSES ncurses)
        if(LIBNCURSES)
            target_link_libraries(${PROJECT_NAME} ${LIBNCURSES})
        endif(LIBNCURSES)
    endif(LIBCURSES)
endif(LIBTERMCAP)

find_library(LIBBSD bsd)
if(LIBBSD)
    target_link_libraries(${PROJECT_NAME} ${LIBBSD})
endif(LIBBSD)

install(
    PROGRAMS
    ${CMAKE_BINARY_DIR}/${PROJECT_NAME}
    DESTINATION "bin"
)
install(
    FILES
    ${CMAKE_SOURCE_DIR}/pgstat.1
    DESTINATION "share/man/man1"
)

# CPack rules for creating distribution files.

include(InstallRequiredSystemLibraries)

set(CPACK_SOURCE_GENERATOR "TBZ2;TXZ;ZIP")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "pgstat is 'systat' for PostgreSQL")
set(CPACK_PACKAGE_VENDOR "pgstat")
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${PROJECT_NAME}-${PROJECT_VERSION}")
set(
    CPACK_SOURCE_IGNORE_FILES
    "\\\\.bz2\\$;\\\\.gz\\$;\\\\.xz\\$;\\\\.Z\\$;\\\\.zip\\$"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/CMakeCache\\\\.txt\\$"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/CMakeFiles/"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/cmake_install\\\\.cmake\\$"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/CPackConfig\\\\.cmake\\$"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/\\\\.gitignore\\$"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/\\\\_CPack\\\\_Packages/"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/CPackSourceConfig\\\\.cmake\\$"
)
set(
    CPACK_SOURCE_IGNORE_FILES
    "${CPACK_SOURCE_IGNORE_FILES};/README\\\\.git\\$"
)

include(CPack)
