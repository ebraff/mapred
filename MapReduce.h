#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <semaphore.h>
#include "uthash.h"
#include "hash.h"

/* GLOBAL VARIABLES */

int aFlag; /*wordcount = 0, sort = 1*/
int iFlag; /*threads = 0, procs = 1*/
int numMapThreads;
int numReduceThreads;
char *infile, *outfile;
sem_t semLock;
/* Array of hashtables, one for each reduce thread*/
struct hashNode ** hashArray;

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
    struct hashNode *next;
    UT_hash_handle hh;         /* makes this structure hashable */
} hashNode;

typedef struct arg
{
	int index;
	FILE *file;
} arg;

/* FUNCTION HEADERS */

void printUsage();
int isNumber(char *string);
int hasNext(int len);
int parseArgs(int argc, char *argv[]);

void map(void *(*func_ptr)(void *shard));
void partition(void *(*func_ptr)(void *argstruct));

void *mapWord(void *voidshard);
void *mapCount(void *shard);

void *reduceWord(void *argstruct);
void *reduceCount(void* argstruct);
void sortHashAll();
void* sortHash(void* hashNodeVoid);


void cleanShardFiles();
void trolol();

hashNode* newHashNode(char* word, int value);
valueNode* newValNode(int value);
void addWord(char* word, int value);
hashNode *findWord(const char* word);

unsigned long hash(unsigned char *str); 


/* HASH FUNCTIONS */

/* Sort one hash table */
void* sortHash(void* hashNodeVoid){
    hashNode** hn = (hashNode**)hashNodeVoid;
    HASH_SORT(*hn, keyCompare);
    
}

int keyCompare(void *a, void *b) {
    int res = atoi((char*)a) - atoi((char*)b);
    
    if(res < 0)
        return -1; /* a < b */
    else if(res == 0)
        return 0; /* a == b */
    else
        return 1; /* a > b */
}

/* Create new hashNode */
hashNode* newHashNode(char* word, int value)
{
	hashNode *hn = (hashNode *)malloc(sizeof(hashNode));
	hn->key = strdup(word); 
    hn->valueHead = newValNode(value);
    
    return hn;
}

/* Create new hashNode */
valueNode* newValNode(int value)
{
    valueNode *vn = (valueNode *)malloc(sizeof(struct valueNode));
    vn->value = value;
    vn->next = NULL;
    
    return vn;
}

void addWord(char* word, int value) {
    hashNode *hn = findWord(word);
    
    if(hn)
    {
		valueNode* vn = newValNode(value);
		vn->next = hn->valueHead;
		hn->valueHead = vn;
	}
	else
	{
		hn = newHashNode(word, value);
		HASH_ADD_STR( hashArray[hash(word) % numReduceThreads], key, hn ); /* id: name of key field */
	}
      
}

hashNode *findWord(const char* word) {
    hashNode *hn;
    
    /* Does not work with hashNode passes as parameter, only with global keyMap */
    HASH_FIND_STR(hashArray[hash((char*) word) % numReduceThreads], word, hn );  /* hn: output pointer */
    return hn;
}

unsigned long hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;
    
    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    
    return hash;
}