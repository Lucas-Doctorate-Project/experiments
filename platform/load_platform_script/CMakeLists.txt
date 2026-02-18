cmake_minimum_required(VERSION 3.16)
project(platform_loader CXX)

# Path to the SimGrid installation
set(SIMGRID_INSTALL_PATH "$ENV{HOME}/.local" CACHE PATH "Path to SimGrid installation")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compilation flags 
set(WARNING_FLAGS
    "-Wshadow -Wcast-align -Waggregate-return -Wmissing-prototypes \
     -Wmissing-declarations -Wstrict-prototypes -Wmissing-noreturn \
     -Wredundant-decls -Wnested-externs -Wpointer-arith \
     -Wwrite-strings -finline-functions"
)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O2 ${WARNING_FLAGS}")

# --- Shared library: libplatform.so ---
add_library(platform SHARED platform.cpp)

target_include_directories(platform PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${SIMGRID_INSTALL_PATH}/include
)

# If SimGrid is installed in lib64 
target_link_directories(platform PRIVATE
    ${SIMGRID_INSTALL_PATH}/lib64
)

target_link_libraries(platform PRIVATE simgrid)

# Ensure the output name is libplatform.so
set_target_properties(platform PROPERTIES
    OUTPUT_NAME "platform"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)

# --- Test executable ---
add_executable(example example.cpp)

target_include_directories(example PRIVATE
    ${SIMGRID_INSTALL_PATH}/include
)

target_link_directories(example PRIVATE
    ${SIMGRID_INSTALL_PATH}/lib64
)

target_link_libraries(example PRIVATE simgrid)

# Place the executable in the project root directory
set_target_properties(example PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)
