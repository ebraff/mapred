/*mapred -a [wordcount, sort] -i [procs, threads] -m num_maps -r num_reduces infile outfile*/
#include "MapReduce.h"

void printUsage()
{
	printf("mapred -a [wordcount, sort] -i [procs, threads] -m num_maps -r num_reduces infile outfile\n-a specifies the application: wordcount or sort\n-i specifies whether processes or threads are used\n-m specifies the number of map processes (or threads)\n-r specifies the number of reduce processes (or threads)\ninfile is the name of the input file\noutfile is the name of the input file\n");
}

int isNumber(char *string)
{
	int count;
	for(count = 0; count < strlen(string); count++)
		if (!isdigit(string[count]))
			return 1;
	return 0;
}

int parseArgs(int argc, char *argv[])
{
	if (argc != 11)
	{
		printf("ERROR: Invalid arguments! Proper usage is as follows:\n");
		printUsage();
		return 1;
	}

	int argCount;
	for(argCount = 1; argCount < argc - 2; argCount++)
	{
		if (strcmp(argv[argCount], "-a"))
			if (strcmp(argv[argCount + 1], "wordcount"))
				aFlag = 0;
			else if (strcmp(argv[argCount + 1], "sort"))
				aFlag = 1;
			else
			{
				printf("ERROR: Invalid input to -a Flag!\n");
				printUsage();
			}
		else if (strcmp(argv[argCount], "-i"))
			if (strcmp(argv[argCount + 1], "threads"))
				aFlag = 0;
			else if (strcmp(argv[argCount + 1], "procs"))
			{
				printf("ERROR: Program does not support multi-processing!\n");
				return 1;
			}
			else
			{
				printf("ERROR: Invalid input to -i Flag!\n");
				printUsage();
			}
		else if (strcmp(argv[argCount], "-m"))
			if (isNumber(argv[argCount + 1]))
				numMapThreads = atoi(argv[argCount + 1]);
			else
			{
				printf("ERROR: Invalid input to -m Flag!\n");
				printUsage();
			}
		else if (strcmp(argv[argCount], "-r"))
			if (isNumber(argv[argCount + 1]))
				numReduceThreads = atoi(argv[argCount + 1]);
			else
			{
				printf("ERROR: Invalid input to -m Flag!\n");
				printUsage();
			}
	}

	infile = strdup(argv[argc - 2]);
	outfile = strdup(argv[argc - 1]);
}

hashNode* map(hashNode* (*func_ptr)(char *shard))
{
	int count;
	for(count = 0; count < numMapThreads; count++)
	{
		
	}
}

void reduceWord(hashNode* wordHash)
{
	
}

hashNode* mapWord(char* shard)
{

}

int main(int argc, char *argv[])
{
	keyMap = NULL;
	
	parseArgs(argc, argv);
	
	char cwd[1024];
	if (getcwd(cwd, sizeof(cwd)) == NULL)
	{
		printf("ERROR: There are some problems with your current directory bitch go fix them!\n");
		return 1;
	}

	/*Because C is stupid!*/
	strcat(cwd, "/split.sh ");
	strcat(cwd, infile);
	strcat(cwd, " ");
	strcat(cwd, argv[6]); /*change this to itoa at some point when were less lazy*/
	//system(cwd); /*Split the input file*/
	

	//keyMap = map(mapWord);
	
	addWord("hi", 1, keyMap);
	printf("Added \"hi\"\n");
	
	hashNode* word = findWord("hi" , keyMap);
	
	if(word) 
	{
		printf("Found word: %s with value: %i\n", word->key, word->valueHead->value);
	}
	else
	{
		printf("No word found\n");
	}
	/*remove extra split files*/
	return 0;
}
