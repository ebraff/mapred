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
struct hashNode *keyMap;

void printUsage();
int isNumber(char *string);
int parseArgs(int argc, char *argv[]);
void cleanShardFiles();

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

hashNode* map(hashNode* (*func_ptr)(char *shard));
void reduce(void (*func_ptr)(void *key, void *head));

hashNode* mapWord(char* shard);
hashNode* mapInt(char* shard);

void reduceWord(hashNode* wordHash);
void reduceInt(hashNode* intHash);

