COMPILER = gcc

all: program

program: MapReduce.c
	$(COMPILER) -pthread -g -o mapred MapReduce.h MapReduce.c

header: MapReduce.h
	$(COMPILER) -g -o head MapReduce.h

run: mapred
	./mapred -a wordcount -i threads -m 10 -r 10 input.txt output.txt

debug: mapred
	gdb mapred

clean:
	rm -f mapred
	rm -f head
