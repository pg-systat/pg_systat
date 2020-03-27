cmake_minimum_required(VERSION 2.8)

include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckIncludeFiles)
include(CheckCSourceCompiles)

project(pg_systat C)

set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH "0-alpha2")
set(PROJECT_VERSION
    ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

set(CMAKE_C_FLAGS "-Wall")
set(CMAKE_INCLUDE_CURRENT_DIR ON)

execute_process(
    COMMAND uname -m
    OUTPUT_VARIABLE ARCH
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message("-- arch - ${ARCH}")

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
    dbblk.c
    dbconfl.c
    dbfs.c
    dbtup.c
    dbxact.c
    engine.c
    index.c
    indexio.c
    main.c
    pg.c
    sprompt.c
    tableanalyze.c
    tableioheap.c
    tableioidx.c
    tableiotidx.c
    tableiotoast.c
    tablescan.c
    tabletup.c
    tablevac.c
    vacuum.c
    PROPERTIES COMPILE_FLAGS "${PGINCLUDE}"
)

add_executable(
    ${PROJECT_NAME}
    dbblk.c
    dbconfl.c
    dbfs.c
    dbtup.c
    dbxact.c
    engine.c
    index.c
    indexio.c
    main.c
    pg.c
    sprompt.c
    tableanalyze.c
    tableioheap.c
    tableioidx.c
    tableiotidx.c
    tableiotoast.c
    tablescan.c
    tabletup.c
    tablevac.c
    vacuum.c
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

find_library(LIBTINFO tinfo)
if(LIBTINFO)
	target_link_libraries(${PROJECT_NAME} ${LIBTINFO})
endif(LIBTINFO)

find_library(LIBCURSES curses)
if(LIBCURSES)
    target_link_libraries(${PROJECT_NAME} ${LIBCURSES})
else(LIBCURSES)
    find_library(LIBNCURSES ncurses)
    if(LIBNCURSES)
        target_link_libraries(${PROJECT_NAME} ${LIBNCURSES})
    endif(LIBNCURSES)
endif(LIBCURSES)

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
    ${CMAKE_SOURCE_DIR}/pg_systat.1
    DESTINATION "share/man/man1"
)

# CPack rules for creating distribution files.

include(InstallRequiredSystemLibraries)

set(CPACK_SOURCE_GENERATOR "TBZ2;TXZ;ZIP")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "pg_systat is 'systat' for PostgreSQL")
set(CPACK_PACKAGE_VENDOR "pg_systat")
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
    "${CPACK_SOURCE_IGNORE_FILES};/Makefile\\$"
)

include(CPack)

set(OUTPUT "${PROJECT_VERSION}")

add_custom_target(appimage
    COMMAND cp -aL ${CMAKE_SOURCE_DIR}/AppRun AppDir
    COMMAND cp -aL ${CMAKE_SOURCE_DIR}/pg_systat.desktop AppDir
    COMMAND cp -aL ${CMAKE_SOURCE_DIR}/pg_systat.png AppDir
    COMMAND mkdir -p AppDir/usr/share/metainfo
    COMMAND cp -aL ${CMAKE_SOURCE_DIR}/pg_systat.appdata.xml AppDir/usr/share/metainfo
    COMMAND mkdir -p AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libpq.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libtinfo.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libncurses.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libbsd.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libssl.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libcrypto.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libgssapi_krb5.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libkrb5.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libk5crypto.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libkrb5support.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libkeyutils.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND cp -aL
            `ldd pg_systat | grep libselinux.so | cut -d \" \" -f 3` AppDir/usr/lib
    COMMAND VERSION=${PROJECT_VERSION} appimagetool-${ARCH}.AppImage AppDir
)
