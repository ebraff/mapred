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
				aFlag = 0;
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
	pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * numMapThreads);
	char **shardfiles = (char **)malloc(sizeof(char *) * numMapThreads);

	int threadID;
	/* Create the Map worker threads and have each one
	 * call func_ptr for a different file shard */
	for(threadID = 0; threadID < numMapThreads; threadID++)
	{
		/* grab the shardfile that this thread is incharge of */
		shardfiles[threadID] = (char *)malloc(sizeof(char) * (strlen(infile) + 4));
		sprintf(shardfiles[threadID], "%s.%d", infile, threadID);
		printf("shardfile = %s\n", shardfiles[threadID]);
		pthread_create(&threads[threadID], NULL, func_ptr, (void *)shardfiles[threadID]);
	}

	/* Sit and wait for everyone to finish */
	for(threadID = 0; threadID < numMapThreads; threadID++)
	{
		pthread_join(threads[threadID], NULL);
		free(shardfiles[threadID]);
	}

	free(threads);
}

/* Name: reduce
 * Description: This is the framework reduce function for the mapreduce
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

	int threadID;
	/* Create the Map worker threads and have each one
	 * call func_ptr for a different file shard */
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
 * Description:
 * Arguments: wordHash -
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
	printf("mapWord(%s)\n", shard);
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
			addWord(word, 1);
			sprintf(word, "");
		}
	}

	fclose(file);
}

/* Name: cleanShardFiles
 * Description: Removes all of the shard files created at
 * 				the beginning of the program.
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
	printf("%d\n",funNum);
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

	keyMap = NULL;

	/* Start the Mapping process */
	if (aFlag) /* sort */
		printf("this needs to be replaced with a map call");
	else /* wordcount */
		map(&mapWord);

	//keyMap = map(mapWord);
	
	//addWord("hi", 1, keyMap);
	//printf("Added \"hi\"\n");
	
	//hashNode* word = findWord("hi" , keyMap);
	/*
	if(word) 
	{
		printf("Found word: %s with value: %i\n", (char*)word->key, word->valueHead->value);
	}
	else
	{
		printf("No word found\n");
	}*/

	cleanShardFiles();
	trolol();

	return 0;
}
