#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include "uthash.h"
#include "hash.h"

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
    char* key;                    /* key */
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


void addWord(char* word, int value) {
    hashNode *hn;
    valueNode *vn;

    hn = malloc(sizeof(struct hashNode));
    hn->key = strdup( word);
    
    vn = malloc(sizeof(struct valueNode));
    vn->value = 1;
    hn->valueHead = vn;
    
    /* Does not work with hashNode passes as parameter, only with global keyMap */
    HASH_ADD_STR( keyMap, key, hn );  /* id: name of key field */
}

hashNode *findWord(const char* word) {
    hashNode *hn;
    
    /* Does not work with hashNode passes as parameter, only with global keyMap */
    HASH_FIND_STR( keyMap, "hi", hn );  /* hn: output pointer */
    return hn;
}

