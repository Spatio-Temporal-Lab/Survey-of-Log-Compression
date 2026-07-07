CXX = g++
CXXFLAGS = -O2 -g -std=c++11 -fPIC
AR = ar
ARFLAGS = rcs
EXEC = Trainer
SRCS = main.cpp util.cpp LengthParser.cpp template.cpp Group.cpp timer.cpp
OBJS = $(SRCS:.cpp=.o)

all: $(EXEC)

$(EXEC).a: $(OBJS)
	$(AR) $(ARFLAGS) $@ $^

$(EXEC): trainer_cli.o $(EXEC).a
	$(CXX) -o $@ trainer_cli.o $(EXEC).a

start: $(EXEC)

.cpp.o:
	$(CXX) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -f $(OBJS) trainer_cli.o $(EXEC) $(EXEC).a
