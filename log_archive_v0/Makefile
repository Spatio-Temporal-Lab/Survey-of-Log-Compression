CXX ?= g++
CPPFLAGS ?= -Iinclude
CXXFLAGS ?= -O3 -std=c++11
LDLIBS ?= -lbz2 -pthread -lssl -lcrypto

TARGET := bin/Archiver
SOURCES := $(filter-out src/LogDistributerAnalyzer_JaccardDynamic.cpp,$(wildcard src/*.cpp))
OBJECTS := $(patsubst src/%.cpp,build/%.o,$(SOURCES))

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJECTS) | bin
	$(CXX) $(OBJECTS) $(LDLIBS) -o $@

build/%.o: src/%.cpp | build
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

bin build:
	mkdir -p $@

clean:
	rm -rf build $(TARGET)
