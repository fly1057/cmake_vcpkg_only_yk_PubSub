cmake_minimum_required(VERSION 3.20)

# 生成 compile_commands.json 供 clangd 使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# vcpkg 工具链设置
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "D:/vcpkg/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "Vcpkg toolchain file")
endif()

project(RedisYkPubSub VERSION 1.0.0 LANGUAGES C CXX)

# C 标准
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Windows 设置
if(WIN32)
    add_compile_definitions(_WIN32_WINNT=0x0601 WIN32_LEAN_AND_MEAN)
    # MSVC 专用选项
    if(MSVC)
        add_compile_options(/utf-8)  # 解决 MSVC 编译 UTF-8 源文件时的编码问题
    endif()
    # GCC/MinGW 使用 -fexec-charset=GBK 或默认 UTF-8
endif()

# 输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 查找依赖
find_package(hiredis CONFIG REQUIRED)
find_package(Libevent CONFIG REQUIRED COMPONENTS core extra)

# 可执行文件
add_executable(${PROJECT_NAME}
    src/redis_client.c
    src/redis_client_data.c
    src/only_yk_PubSub.c
)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/inc
)

# 添加vcpkg安装的头文件路径（确保clangd能找到）
if(EXISTS "${CMAKE_BINARY_DIR}/vcpkg_installed/x64-windows/include")
    target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_BINARY_DIR}/vcpkg_installed/x64-windows/include
    )
endif()

target_link_libraries(${PROJECT_NAME} PRIVATE
    hiredis::hiredis
    libevent::core
    libevent::extra
    ws2_32  # Winsock
)
