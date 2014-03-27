#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include "uthash.h"

/* GLOBAL VARIABLES */

int aFlag; /*wordcount = 0, sort = 1*/
int iFlag; /*threads = 0, procs = 1*/
int numMapThreads;
int numReduceThreads;
char *infile, *outfile;
struct hashNode *keyMap;

/* STRUCTS */

typedef struct valueNode
{
	int value;
	struct valueNode *next;
} valueNode;

typedef struct hashNode 
{
    void* key;                    /* key */
    valueNode *valueHead;
    UT_hash_handle hh;         /* makes this structure hashable */
} hashNode;

/* FUNCTION HEADERS */

void printUsage();
int isNumber(char *string);
int parseArgs(int argc, char *argv[]);

hashNode* map(void *(*func_ptr)(void *shard));
void reduce(void (*func_ptr)(void *key, void *head));

void *mapWord(void *voidshard);
void *mapInt(void *shard);

void reduceWord(hashNode* wordHash);
void reduceInt(hashNode* intHash);

void cleanShardFiles();
