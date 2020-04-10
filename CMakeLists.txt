# Usage: git clone git://github.com/thomasmoelhave/tpie && mkdir build && cd build && cmake .. && make main && ./main
add_subdirectory(tpie)
add_executable(main main.cc)
target_link_libraries(main tpie)
