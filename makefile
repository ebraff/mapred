COMPILER = gcc

all: program

program: MapReduce.c

	$(COMPILER) -pthread -g -o mapred MapReduce.h hash.h MapReduce.c

run: mapred
	./mapred -a sort -i threads -m 1 -r 1 intsmall.txt output.txt

debug: mapred
	gdb mapred

clean:
	rm -f mapred
	rm -f head