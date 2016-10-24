#include <stdio.h>
#include "pf.h"
#include "pftypes.h"

#include <time.h>
#include <stdlib.h>


// #define FILE1	"input"
#define SIZE 1000
#define PAGE_SIZE 1024
#define INT_SIZE 4

void printPage(int* buf,int pagenum){
	int i;
	printf("new data for %d\n", pagenum);
	for(i=0;i<PF_PAGE_SIZE/INT_SIZE;i++){
		int data=*((int *)(buf+i));
		printf(" %p,",data);
	}
	printf("\n");
}

printfile(fd)
int fd;
{
int error;
int *buf;
int pagenum;

	printf("reading file\n");
	pagenum = -1;
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
		//printf("got page %d\n",pagenum);
		printPage(buf, pagenum);
		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
	}
	if (error != PFE_EOF){
		PF_PrintError("not eof\n");
		exit(1);
	}
	printf("eof reached\n");

}


int main(int argc, char const *argv[])
{
	if(argc<1) return -1;
	char* FILE1 = argv[1]; 
	int fd1;
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open file1\n");
		exit(1);
	}
	printf("printing fd1");
	printfile(fd1);

	return 0;
}

