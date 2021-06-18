cmake_minimum_required(VERSION 2.8)

project(pg_systat NONE)

find_program(RST2MAN NAMES rst2man rst2man.py REQUIRED)

add_custom_target(manpage
    COMMAND ${RST2MAN} ${CMAKE_SOURCE_DIR}/pg_systat.1.rst
            ${CMAKE_BINARY_DIR}/pg_systat.1
)

install(
    FILES
    ${CMAKE_BINARY_DIR}/pg_systat.1
    DESTINATION "share/man/man1"
)
