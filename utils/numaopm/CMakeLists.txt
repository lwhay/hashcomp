add_executable(numatest numa-test.cpp)
target_link_libraries(numatest -lpthread -lnuma -fopenmp)

add_executable(numa-naivetest numa-naivetest.cpp)
target_link_libraries(numa-naivetest -lpthread -lnuma)