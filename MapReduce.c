/*mapred -a [wordcount, sort] -i [procs, threads] -m num_maps -r num_reduces infile outfile*/
#include "MapReduce.h"

/* Name: printUsage
 * Description: Prints to command line the proper usage of this program
 * Arguments: none
 * Returns: void
 */
void printUsage()
{
	printf("mapred -a [wordcount, sort] -i [procs, threads] -m num_maps -r num_reduces infile outfile\n-a specifies the application: wordcount or sort\n-i specifies whether processes or threads are used\n-m specifies the number of map processes (or threads)\n-r specifies the number of reduce processes (or threads)\ninfile is the name of the input file\noutfile is the name of the input file\n");
}

/* Name: isNumber
 * Description: takes the given string and checks each character
 * 				to see if they are all digits. In other words it
 * 				checks the string to see if its a number usable by
 * 				atoi().
 * Arguments: string - a NULL terminated list of chars
 * Returns: 0 if the string is not a number
 * 			1 if the string is a number
 */
int isNumber(char *string)
{
	int count;
	for(count = 0; count < strlen(string); count++)
		if (!isdigit(string[count]))
			return 0;
	return 1;
}

/* Name: parseArgs
 * Description: Checks the command line input to make sure it is valid.
 * Arguments: argc - this is the number of arguments main recieves
 * 					 from command line.
 * 			  argv - this is the argument list main recieves from command
 * 					 line.
 * Returns: 0 if the arguments are not valid
 * 			1 if the arguments are valid
 */
int parseArgs(int argc, char *argv[])
{
	if (argc != 11)
	{
		printf("ERROR: Invalid arguments! Proper usage is as follows:\n");
		printUsage();
		return 0;
	}

	int argCount;
	for(argCount = 1; argCount < argc - 2; argCount++)
	{
		if (strcmp(argv[argCount], "-a") == 0)
			if (strcmp(argv[argCount + 1], "wordcount") == 0)
				aFlag = 0;
			else if (strcmp(argv[argCount + 1], "sort") == 0)
				aFlag = 1;
			else
			{
				printf("ERROR: Invalid input to -a Flag!\n");
				printUsage();
				return 0;
			}
		else if (strcmp(argv[argCount], "-i") == 0)
			if (strcmp(argv[argCount + 1], "threads") == 0)
				iFlag = 0;
			else if (strcmp(argv[argCount + 1], "procs") == 0)
			{
				printf("ERROR: Program does not support multi-processing!\n");
				return 0;
			}
			else
			{
				printf("ERROR: Invalid input to -i Flag!\n");
				printUsage();
				return 0;
			}
		else if (strcmp(argv[argCount], "-m") == 0)
			if (isNumber(argv[argCount + 1]))
				numMapThreads = atoi(argv[argCount + 1]);
			else
			{
				printf("ERROR: Invalid input to -m Flag!\n");
				printUsage();
				return 0;
			}
		else if (strcmp(argv[argCount], "-r") == 0)
			if (isNumber(argv[argCount + 1]))
				numReduceThreads = atoi(argv[argCount + 1]);
			else
			{
				printf("ERROR: Invalid input to -m Flag!\n");
				printUsage();
				return 0;
			}
	}

	infile = strdup(argv[argc - 2]);
	outfile = strdup(argv[argc - 1]);

	return 1;
}

/* Name: sortHashAll
 * Description: Goes through each hash table in array and spawns a sort thread to sort it.
 * Arguments: none
 * Returns: void
 */
void sortHashAll(){
    int i = 0;
    valueNode *vnode;
    hashNode* newHash = NULL;
    hashNode* smallest = NULL;
    hashNode* curr = NULL;
    pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * numReduceThreads);
    
	int threadID;
	
	/* Create a sort worker and have each one call sortHash for a hash table in the array */
	for(threadID = 0; threadID < numReduceThreads; threadID++)
	{
		pthread_create(&threads[threadID], NULL, sortHash, (void *)&hashArray[threadID]);
	}
    
	/* Sit and wait for everyone to finish */
	for(threadID = 0; threadID < numReduceThreads; threadID++)
	{
		pthread_join(threads[threadID], NULL);
	}
/**** Print a list to check order, testing! ***/
    for(curr = hashArray[0]; curr != NULL; curr = curr->hh.next)
	{
		int total = 0;
		
        
		/* Sum up the values for each key */
		vnode = curr->valueHead;
		while(vnode != NULL)
		{
            printf("%s \n", curr->key);
			vnode = vnode->next;
            // printf("thread %d freeing node %d for value %s\n", idx, total, hnode->key);
			//free(temp);
		}
    }
/****************************** end test *******/
        
		
	
   
    /* Merge all sorted lists into one list */
    while(hasNext(numMapThreads))
    {
        for ( i = 0 ; i < numReduceThreads; i++) {
            curr = hashArray[i];
            
            if (smallest == NULL) { /* if smallest is not initiated,set it to the first element */
                smallest = curr;
                hashArray[i] = hashArray[i]->hh.next;
            }
            else if (strcmp(curr->key,smallest->key) < 0) /* curr < smallest */
            {
                smallest = curr;
                hashArray[i] = hashArray[i]->hh.next;
            }
        }
        smallest->next = newHash;
        newHash = smallest;
        smallest = NULL;
    }
    
    hashArray[0] = newHash; /* put the full sorted hash into the array */


}

/* Name: map
 * Description: Checks if there are more elements to compare in the hashArray
 * Arguments: number of hash tables in the array
 * Returns: 0 if false, 1 if true
 */
int hasNext(int len)
{
    int i;
    for( i = 0 ; i < len ; i++ )
    {
        if (hashArray[i] != NULL);
        {
            return 1;
        }
        
    }
    
    return 0;
}

/* Name: map
 * Description: This is the framework map function for the mapreduce
 * 				structure. This function will start and control all of
 * 				map worker threads.
 * Arguments: func_ptr - a pointer to the user defined function which
 * 						 each worker will be responsible for executing.
 * Returns: void
 */
void map(void *(*func_ptr)(void *shard))
{
	sem_init(&semLock, 0, 1);
	pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * numMapThreads);
	char **shardfiles = (char **)malloc(sizeof(char *) * numMapThreads);

	int threadID;
	
	/* Create the Map worker threads and have each one
	 * call func_ptr for a different file shard */
    printf("Starting map threads\n");
	for(threadID = 0; threadID < numMapThreads; threadID++)
	{
		/* grab the shardfile that this thread is incharge of */
		shardfiles[threadID] = (char *)malloc(sizeof(char) * (strlen(infile) + 4));
		sprintf(shardfiles[threadID], "%s.%d", infile, threadID);
		pthread_create(&threads[threadID], NULL, func_ptr, (void *)shardfiles[threadID]);
	}

	/* Sit and wait for everyone to finish */
	for(threadID = 0; threadID < numMapThreads; threadID++)
	{
		pthread_join(threads[threadID], NULL);
		free(shardfiles[threadID]);
	}

	free(threads);
	sem_destroy(&semLock);
}

/* Name: partition
 * Description: This is the framework partition function for the mapreduce
 * 				structure. This function will start and control all of
 * 				reduce worker threads.
 * Arguments: func_ptr - a pointer to the user defined function which
 * 						 each worker will be responsible for executing.
 * Returns: void
 */
void partition(void *(*func_ptr)(void *argstruct))
{
	pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * numReduceThreads);
	arg *threadargs = (arg *)malloc(sizeof(arg) * numReduceThreads);

	FILE *file;
	if ((file = fopen(outfile, "w")) == NULL)
	{
		printf("ERROR: Failed to open output file %s\n", outfile);
		exit(1);
	}
    printf("Starting sort\n");
    if(aFlag == 1) /* Integer sort */
        sortHashAll();
    
	int threadID;
	/* Create the Reduce worker threads and have each one
	 * call func_ptr and be passed a key */
	for(threadID = 0; threadID < numReduceThreads; threadID++)
	{
		threadargs[threadID].index = threadID;
		threadargs[threadID].file = file;
		pthread_create(&threads[threadID], NULL, func_ptr, (void *)&threadargs[threadID]);
	}

	/* Sit and wait for everyone to finish */
	for(threadID = 0; threadID < numReduceThreads; threadID++)
		pthread_join(threads[threadID], NULL);

	free(threads);
	fclose(file);
}

/* Name: reduceWord
 * Description: Grabs the set of key, value pairs that it is responsible for counting
 * 		and counts all occurances of a single key and outputs the results
 * 		to the outfile provided by the user.
 * Arguments: argstruct - a struct containing an int index and a file handle fh
 * Returns: void
 */
void *reduceWord(void *argstruct)
{
	int idx = ((arg *)argstruct)->index;
	FILE *file = ((arg *)argstruct)->file;

	hashNode *hnode;
	/* step through all the keys */
	for(hnode = hashArray[idx]; hnode != NULL; hnode = hnode->hh.next)
	{
		int total = 0;
		valueNode *vnode;

		/* Sum up the values for each key */
		vnode = hnode->valueHead;
		while(vnode != NULL)
		{
			total++;
			valueNode *temp = vnode;
			vnode = vnode->next;
           // printf("thread %d freeing node %d for value %s\n", idx, total, hnode->key);
			free(temp);
		}

		fprintf(file, "%s %d\n", hnode->key, total);
	}
}

/* Name: mapWord
 * Description: The user defined function which pulls words out
 * 				of a shard file
 * Arguments: shard - the name of the shard file
 * Returns: void
 */
void *mapWord(void *voidshard)
{
	char *shard = (char *)voidshard;
	FILE *file;
	if ((file = fopen(shard, "r")) == NULL)
	{
		printf("ERROR: Failed to open shard file %s\n", shard);
		exit(1);
	}

	char word[1024]; /* word buffer for reading from the file */
	char chr;
	while((chr = fgetc(file)) != EOF)
	{
		if (isalpha(chr)) /* add to the current word */
			sprintf(word, "%s%c", word, tolower(chr));
		else if (strlen(word) != 0) /* if we have a completed word */
		{
			sem_wait(&semLock);
			addWord(word, 1);
			sem_post(&semLock);
			sprintf(word, "");
		}
	}

	fclose(file);
}

/* Name: mapcount
 * * Description: The user defined function which pulls numbers out
 * * of a shard file
 * * Arguments: shard - the name of the shard file
 *     * Returns: void
 *      */
void *mapCount(void *voidshard)
{
    char *shard = (char *)voidshard;
    FILE *file;
    if ((file = fopen(shard, "r")) == NULL)
    {
        printf("ERROR: Failed to open shard file %s\n", shard);
        exit(1);
    }

    char number[1024]; /* word buffer for reading from the file */
    char chr;
    
    while((chr = fgetc(file)) != EOF)
    {
        if (isdigit(chr)) /* add to the current word */
            sprintf(number, "%s%c", number, tolower(chr));
        else if (strlen(number) != 0) /* if we have a completed word */
        {
            
            sem_wait(&semLock);
            addWord(number, 1);
            sem_post(&semLock);
            sprintf(number, "");
        }
    }
    
    fclose(file);
    
    
}

/* Name: reduceCount
 * Description: print the integers from the associating hashtable to the output file.
 *              integers printed once for each value.
 * Arguments: argstruct - a struct containing an int index and a file handle fh
 * Returns: void
 */
void *reduceCount(void *argstruct)
{
    int idx = ((arg *)argstruct)->index;
	FILE *file = ((arg *)argstruct)->file;
    
	hashNode *hnode;
	/* step through all the keys */
	for(hnode = hashArray[idx]; hnode != NULL; hnode = hnode->hh.next)
	{
		int total = 0;
		valueNode *vnode;
        
		/* Sum up the values for each key */
		vnode = hnode->valueHead;
		while(vnode != NULL)
		{
            fprintf(file, "%s\n", hnode->key);
			total++;
			valueNode *temp = vnode;
			vnode = vnode->next;
            printf("thread %d freeing node %d for value %s\n", idx, total, hnode->key);
			free(temp);
		}
        
		
	}
}

/* Name: cleanShardFiles
 * Description: Removes all of the shard files created at
 *              the beginning of the program.
 * Arguments: none
 * Returns: void
 */
void cleanShardFiles()
{
	int shardNum;
	for(shardNum = 0; shardNum < numMapThreads; shardNum++)
	{
		char *cmd = (char *)malloc(sizeof(char) * (10 + strlen(infile)));
		sprintf(cmd, "rm -f %s.%d", infile, shardNum);
		system(cmd);
		free(cmd);
	}
}

/* \giggle */
void trolol()
{
	time_t t;
	srand((unsigned) time(&t));
	int funNum = rand();

	if (funNum % 7 == 0)
	{
		printf("         ___\n");
		printf("    ~ _>/O O\\<_ ~~ ~~ ~  ~ ~\n");
		printf("  ~~ ( o     o )  ~~~~ ~~~ ~~~~~\n");
		printf("  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
		printf("    _/         \\_\n");
		printf("   (__/(     )\\__)\n");
		printf("      _/     \\\n");
		printf("     (__/'.'\\__)\n");
        
    }
}

/* Yeah no im not gona give main a pretty function comment! */
int main(int argc, char *argv[])
{
	int i;
    
	/* Validate the arguments before doing anything */
	if (!parseArgs(argc, argv))
		return 1;
    
	char cwd[1024]; /* this will hold the command to separate infile into shards */
	/* grab the full path of the current directory */
	if (getcwd(cwd, sizeof(cwd)) == NULL)
	{
		printf("ERROR: There are some problems with your current directory bitch go fix them!\n");
		return 1;
	}
    
	/* perform some C black magic to concatinate strings together */
	sprintf(cwd, "%s/split.sh %s %d", cwd, infile, numMapThreads);
	system(cwd); /*Split the input file*/
    
	/* Initialize and malloc array of hash tables for storage */
	hashArray = (hashNode **)malloc(sizeof(hashNode*) * numReduceThreads);
	for( i = 0 ; i < numReduceThreads ; i++)
	{
		hashArray[i] = NULL;
	}
    
    printf("aflag: %d\n", aFlag);
	/* Start the Mapping process */
	if (aFlag) /* sort */
	{
        printf("Starting map count\n");
		map(&mapCount);
		partition(&reduceCount);
	}
	else /* wordcount */
	{
        printf("Starting map word\n");
		map(&mapWord);
		partition(&reduceWord);
	}
    
	cleanShardFiles();
	trolol();
    
	return 0;
}
	