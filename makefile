COMPILER = gcc

all: program

program: MapReduce.c

	$(COMPILER) -g -o mapred MapReduce.h hash.h MapReduce.c

run: mapred
	./mapred -a wordcount -i threads -m 10 -r 10 input.txt output.txt

debug: mapred
	gdb mapred

clean:
	rm -f mapred
	rm -f head
