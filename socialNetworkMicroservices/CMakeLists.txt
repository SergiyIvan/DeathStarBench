cmake_minimum_required(VERSION 3.5)
project(social_network_microservices)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "-O3 -g")
set(CMAKE_INSTALL_PREFIX /usr/local/bin)
set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_subdirectory(src)
#add_subdirectory(test)
#enable_testing()


