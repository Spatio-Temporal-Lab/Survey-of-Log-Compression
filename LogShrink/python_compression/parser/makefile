# cc = g++
# EXEC = THULR
# SRCS = main.cpp template.cpp LengthSearch.cpp
# OBJS = $(SRCS:.cpp=.o)
#
# start:$(OBJS)
# 	$(cc) -o $(EXEC) $(OBJS)
# 	$(cc) -std=c++11 elastic.cpp -o Elastic
#
# .cpp.o:
# 	$(cc) -std=c++11 -o $@ -c $<
# clean:
# 	rm -rf $(OBJS) $(EXEC)
# 	rm -rf Elastic
#
cc = g++
EXEC = THULR
SRCS = main.cpp template.cpp LengthSearch.cpp
OBJS = $(SRCS:.cpp=.o)

# 编译和链接选项
CFLAGS = -std=c++11 -g -O0  # -g 添加调试信息, -O0 关闭优化
# ASan内存检测工具
#CFLAGS = -std=c++11 -g -O0 -fsanitize=address -fno-omit-frame-pointer

start: $(OBJS)
	$(cc) $(CFLAGS) -o $(EXEC) $(OBJS)
	$(cc) $(CFLAGS) elastic.cpp -o Elastic

.cpp.o:
	$(cc) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJS) $(EXEC)
	rm -rf Elastic
