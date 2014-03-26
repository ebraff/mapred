#include <stdio.h>
#include <string.h>

void printUsage()
{
	printf("mapred -a [wordcount, sort] -i [procs, threads] -m num_maps -r num_reduces infile outfile\n-a specifies the application: wordcount or sort\n-i specifies whether processes or threads are used\n-m specifies the number of map processes (or threads)\n-r specifies the number of reduce processes (or threads)\ninfile is the name of the input file\noutfile is the name of the input file\n");
}

int main(int argc, char *argv[])
{
	int aFlag = 0; /*wordcount = 0, sort = 1*/
	int iFlag = 0; /*threads = 0, procs = 1*/
	int numMapThreads = 0;
	int numReduceThreads = 0;
	char *infile, *outfile;
	/*mapred -a [wordcount, sort] -i [procs, threads] -m num_maps -r num_reduces infile outfile*/
	if (argc != 10)
	{
		printf("ERROR: Invalid arguments! Proper usage is as follows:\n");
		printUsage();
		return 1;
	}

	int argCount;
	for(argCount = 1; argCount < argc; argCount++)
	{
		if (strcmp(argv[argCount], "-a"))
			if (strcmp(argv[argCount + 1], "wordcount"))
				aFlag = 0;
			else if (strcmp(argv[argCount + 1], "sort"))
				aFLag = 1;
			else
			{
				printf("ERROR: Invalid input to -a flag!");
				printUsage();
			}
		else if (strcmp(argv[argCount], "-i"))
			if (strcmp(argv[argCount + 1], "threads"))
				aFlag = 0;
			else if (strcmp(argv[argCount + 1], "procs"))
				aFLag = 1;
			else
			{
				printf("ERROR: Invalid input to -i flag!");
				printUsage();
			}
		/*else if (strcmp(argv[argCount], "-m"))
			if (strcmp(argv[argCount + 1], "threads"))
				aFlag = 0;
			else if (strcmp(argv[argCount + 1], "procs"))
				aFLag = 1;
			else
			{
				printf("ERROR: Invalid input to -i flag!");
				printUsage();
			}*/
	}
	
	return 0;
}
