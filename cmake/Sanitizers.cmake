include_guard(GLOBAL)

function(swarmnet_enable_sanitizers target_name)
    if(NOT SWARMNET_ENABLE_ASAN AND NOT SWARMNET_ENABLE_UBSAN AND NOT SWARMNET_ENABLE_TSAN)
        return()
    endif()

    if(MSVC)
        return()
    endif()

    if(WIN32)
        if(NOT SWARMNET_SANITIZER_WINDOWS_NOTICE_EMITTED)
            message(STATUS "Sanitizers requested but disabled on Windows for this toolchain.")
            set(SWARMNET_SANITIZER_WINDOWS_NOTICE_EMITTED
                TRUE
                CACHE INTERNAL "One-time Windows sanitizer notice")
        endif()
        return()
    endif()

    set(_flags "")
    if(SWARMNET_ENABLE_ASAN)
        list(APPEND _flags -fsanitize=address)
    endif()
    if(SWARMNET_ENABLE_UBSAN)
        list(APPEND _flags -fsanitize=undefined)
    endif()
    if(SWARMNET_ENABLE_TSAN)
        list(APPEND _flags -fsanitize=thread)
    endif()

    if(_flags)
        target_compile_options(${target_name} PRIVATE ${_flags} -fno-omit-frame-pointer)
        target_link_options(${target_name} PRIVATE ${_flags})
    endif()
endfunction()
