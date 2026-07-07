# cc = g++
# EXEC = LogStore
# SRCS = main.cpp sample.cpp
# OBJS = $(SRCS:.cpp=.o)

# .cpp.o:
# 	$(cc) -std=c++11 -o $@ -c $<

# start:$(OBJS)
# 	$(cc) -o $(EXEC) $(OBJS)

# clean:
# 	rm -rf $(OBJS) $(EXEC)

CXX = g++
CXXFLAGS = -O2 -g -Wall -std=c++11
EXEC = DeCompressor
SRCS = main.cpp ../Coffer.cpp 
OBJS = $(SRCS:.cpp=.o)

start:$(OBJS)
	$(CXX) $(OBJS) -o $(EXEC) -lzstd
.cpp.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<
clean:
	rm -f $(OBJS) $(EXEC)
