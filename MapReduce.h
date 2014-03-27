#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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

typedef struct hashNode 
{
    void* key;                    /* key */
    struct value valueHead;
    UT_hash_handle hh;         /* makes this structure hashable */
} hashNode;

typedef struct valueNode
{
	int value;
	struct valueNode next;
} valueNode;

hashNode map(void (*mapFunction)());

