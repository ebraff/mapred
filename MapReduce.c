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
 * Returns:
 */
hashNode* map(hashNode* (*func_ptr)(char *shard))
{
	int threadID, status, pid;
	/* Create the Map worker threads and have each one
	 * call func_ptr for a different file shard */
	for(threadID = 0; threadID < numMapThreads; threadID++)
	{
		pid = fork();
		if (pid == -1)
		{
			/* fork failed */
			printf("ERROR: There was a problem forking the map workers!\n");
			exit(1);
		}
		else if (pid == 0)
		{
			/* child process */
			char *shardfile = (char *)malloc(sizeof(char) * (strlen(infile) + 4));
			sprintf(shardfile, "%s.%d", infile, threadID);


			/* Free all allocated memory and exit */
			free(shardfile);
			exit(0);
		}
	}

	while((pid = wait(&status)) != -1);
}

/* Name: reduceWord
 * Description:
 * Arguments: wordHash -
 * Returns: void
 */
void reduceWord(hashNode* wordHash)
{
	
}

/* Name: mapWord
 * Description:
 * Arguments: shard -
 * Returns:
 */
hashNode* mapWord(char* shard)
{

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
	map(NULL);

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

	return 0;
}
