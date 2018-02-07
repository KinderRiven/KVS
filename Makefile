CC = g++
# main.cpp
MAIN     = main.cpp #city/city-test.cc

# include resources path
INC_PATH = include

# g++ flags
CC_FLAG  = -std=c++11 -O3

# test resources
TEST_SRC = test/testFactory.cc

# google city hash resources
CITY_SRC = city/city.cc

#
CC_SRC = src/halgorithm.cc src/hhash.cc

# libraries
TBB_LIB  = lib/tbb/

# output file
OUTPUT   = run

all:
	$(CC) $(MAIN) $(CC_SRC) $(TEST_SRC) $(CITY_SRC) -L $(TBB_LIB) -I $(INC_PATH) -o $(OUTPUT)
