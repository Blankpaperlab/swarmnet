include_guard(GLOBAL)

function(swarmnet_enable_warnings target_name)
    if(MSVC)
        target_compile_options(${target_name}
            PRIVATE
                /W4
                /WX
                /permissive-
                /utf-8
        )
    else()
        target_compile_options(${target_name}
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
                -Werror
                -Wconversion
                -Wsign-conversion
                -Wshadow
        )
    endif()
endfunction()
