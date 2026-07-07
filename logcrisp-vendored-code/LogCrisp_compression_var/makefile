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
CXXFLAGS = -O2 -g -Wall -std=c++11 -fPIC
AR = ar
ARFLAGS = rcs
EXEC = Compressor
SRCS = main.cpp RuntimePattern.cpp StaticPattern.cpp util.cpp Coffer.cpp timer.cpp
OBJS = $(SRCS:.cpp=.o)
LIBS = -lzstd

all: $(EXEC)

$(EXEC).a: $(OBJS)
	$(AR) $(ARFLAGS) $@ $^

$(EXEC): compressor_cli.o $(EXEC).a
	$(CXX) -o $@ compressor_cli.o $(EXEC).a $(LIBS)

start: $(EXEC)

.cpp.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -f $(OBJS) compressor_cli.o $(EXEC) $(EXEC).a
