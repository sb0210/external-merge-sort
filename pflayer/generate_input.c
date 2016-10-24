/* testpf.c */
#include <stdio.h>
#include "pf.h"
#include "pftypes.h"

#include <time.h>
#include <stdlib.h>


#define SIZE 100000		//input intger range
#define PAGE_SIZE 1024	//page size in bytes
#define INT_SIZE 4
#define PAGE_NUM 2500  //number of pages

void printPage(int* buf,int pagenum){
	int i;
	printf("new data for %d\n", pagenum);
	for(i=0;i<PF_PAGE_SIZE/INT_SIZE-1;i++){
		int data=*((int *)(buf+i));
		printf("%d , ",data);
	}
	printf("\n");
}

int main(int argc, char* argv[])
{
int error;
int i;
int pagenum,*buf;
int *buf1,*buf2;
int fd1,fd2;
char* FILE1;
	// printf("BUF SIZE %d\n", sizeof(buf) );

	srand(time(NULL));
	if(argc<2){return -1; }
	FILE1=argv[1];

	/* create a few files */
	if ((error=PF_CreateFile(FILE1))!= PFE_OK){
		PF_PrintError("file1");
		exit(1);
	}
	// printf("file1 created\n");

	
	/* Open the files, and see how the buffer manager
	handles more insertions, and deletions */
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open file1\n");
		exit(1);
	}
	// printf("opened file1\n");
	int data=0;
	for (i=1; i <= PAGE_NUM ; i++){
		if ((error=PF_AllocPage(fd1,&pagenum,&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		// printf("\n Fillling Page %d \n",pagenum);
		// printf("BUF SIZE %d\n", sizeof(buf) );

		int t;

		for(t=0;t<PAGE_SIZE;t++){
			//input integer generated here, make change only in the next line for sorted/ unsorted input
			int data=rand()%SIZE+1;
			//data=(data+1)%SIZE;
			//written to buffer page
			*((int *)buf +t ) = data;
			printf(" %d \n",data);

		} 
		if ((error=PF_UnfixPage(fd1,pagenum,TRUE))!= PFE_OK){
			PF_PrintError("unfix file1");
			exit(1);
		}
	}

	
	// printf("printing fd1");
	// printfile(fd1);

	
	if (PF_CloseFile(fd1) != PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}
	else{
			// printf("\nclosed file1\n");
	}

	/* print the buffer */
	// printf("buffer:\n");
	// PFbufPrint();

	
}



//function for printfile
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
