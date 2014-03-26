#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "uthash.h"

int aFlag; /*wordcount = 0, sort = 1*/
int iFlag; /*threads = 0, procs = 1*/
int numMapThreads;
int numReduceThreads;
char *infile, *outfile;

void printUsage();
int isNumber(char *string);
int parseArgs(int argc, char *argv[]);
