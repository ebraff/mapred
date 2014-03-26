COMPILER = gcc

all: program

program: MapReduce.c
	$(COMPILER) -g -o MapReduce MapReduce.c

header: shell.h
	$(COMPILER) -g -o head shell.h

run: MapReduce
	./MapReduce

debug: MapReduce
	gdb MapReduce

clean:
	rm -f MapReduce
	rm -f head
