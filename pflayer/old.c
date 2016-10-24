/* testpf.c */
#include <stdio.h>
#include "pf.h"
#include "pftypes.h"
#include <strings.h>
#include <time.h>
#include <stdlib.h>


#define FILE1	"input"
#define FILE2 	"output"
#define INT_SIZE 4
#define NUM_RECORDS 1024

void printPage(char** buf,int pagenum){
	int i;
	printf("new data for %d\n", pagenum);
	for(i=0;i<PF_PAGE_SIZE/INT_SIZE-1;i++){
		int data=*((int *)(buf+i)); //resolve later
		printf("alloc %d data\n",data);
	}
}

void mysort(PFbpage * pageaddr){
	// Sort the buffer denoted by pageaddr internally

	struct PFfpage* page = &(pageaddr->fpage);
	int* data = (int *)page->pagebuf;

	int* curr;
	int* next;

	int i, j, k;
	for(i=0; i<NUM_RECORDS-1; i++){//number of iterations
		short swap =0;
		for(j=0; j<NUM_RECORDS-i-1; j++){
			curr= data+ j; //* (INT_SIZE);
			next= curr++;
			int tmp;
			printf("comparing current and next %d %d\n",*curr,*next );
			if(*curr < *next){
				tmp=*curr; *curr=*next;*next=tmp;
				// bcopy(curr, &tmp, INT_SIZE); // *tmp = *curr
				// bcopy(next, curr, INT_SIZE);
				// bcopy(&tmp, next, INT_SIZE);
				swap = 1;
			}
		}
		if(swap==0) break;
	}



}


main()
{
int error,end_of_file=0;
int i,k;
int pagenum1,pagenum2,pagenum,*buf;
int *buf1,*buf2;
int fd1,fd2;
PFbpage * bpage;
// int* buf_in[PF_MAX_BUFS-1],*buf_out;
// buffer in[PF_MAX_BUFS-1],out;
int fd_out[PF_MAX_BUFS-1],pg_num[PF_MAX_BUFS-1];

	if ((error=PF_CreateFile(FILE2))!= PFE_OK){
		PF_PrintError("output");
		exit(1);
	}
	printf("output blank file created\n");	

	
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open input\n");
		exit(1);
	}
	printf("opened input\n");

	if ((fd2=PF_OpenFile(FILE2))<0){
		PF_PrintError("open output\n");
		exit(1);
	}
	printf("opened output\n");
	
	printf("reading file\n");
	pagenum = -1;

	

	//loading from input buffer
	for(k=0;k<2;k++){
		if ((error=PF_GetNextPage(fd1,&pagenum,&buf1))== PFE_OK){		//next page avl
			int data=*((int *)buf1);
			printf("old data : \n buff adrr: %p", buf1);
			printf(" page: %d data: %d\n",pagenum,data);
			if ((bpage=PFhashFind(fd1,pagenum)) != NULL){			
				mysort(bpage);
			}
			else {
				PF_PrintError("buffer addr retrieving error\n");
				exit(1);
			}
			printPage(buf1,pagenum);
			pg_num[k]=pagenum;
			fd_out[k]=fd1;
			// if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			// 	PF_PrintError("unfix");
			// 	exit(1);
			// }
		}
		else break; //eof or otherwise
	}

	if(error==PFE_EOF){
		end_of_file=1;
	}
	else if (error != PFE_OK){
		PF_PrintError("not eof\n");
		exit(1);
	}

	// printf("checking buffer size etc\n");
	// printf("%p\n", buf);
	// printf("%d\n",*((int *)buf));
	// buf+=(4096+512)/4;
	// printf("%p\n", buf);
	// printf("%d\n", *((int *)buf));

	if ((error=PF_AllocPage(fd2,&pagenum2,&buf2))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
	}



/* print the buffer */
	printf("buffer:\n");
	PFbufPrint();

	/* print the hash table */
	 

	printf("hash table:\n");
	PFhashPrint();

	
	if ((error=PF_UnfixPage(fd1,4,FALSE))!= PFE_OK){
		PF_PrintError("unfix");
		exit(1);
	}
	if ((error=PF_GetNextPage(fd1,&pagenum,&buf1))== PFE_OK){		//next page avl
			int data=*((int *)buf1);
			printf("%p\n", buf1);
			printf("got page %d data %d\n",pagenum,data);
	}

	// clearbuffer(fd1);

	if (PF_CloseFile(fd1) != PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}
	else{
			printf("closed file1\n");
	}

	
}
clearbuffer(fd)
int fd;
{
int error,pagenum;
int* buf; 	

	// if((error=PF_GetThisPage(fd,&pagenum,&buf))== PFE_OK)){
	// 	printf("%s\n", );
	// }
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
			printf("got page %d, %d\n",pagenum,*buf);
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
/************************************************************
Open the File.
allocate as many pages in the file as the buffer
manager would allow, and write the page number
into the data.
then, close file.
******************************************************************/
writefile(fname)
char *fname;
{
int i;
int fd,pagenum;
int *buf;
int error;

	/* open file1, and allocate a few pages in there */
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file1");
		exit(1);
	}
	printf("opened %s\n",fname);

	for (i=0; i < PF_MAX_BUFS; i++){
		if ((error=PF_AllocPage(fd,&pagenum,&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		*((int *)buf) = i;
		printf("allocated page %d\n",pagenum);
	}

	if ((error=PF_AllocPage(fd,&pagenum,&buf))==PFE_OK){
		printf("too many buffers, and it's still OK\n");
		exit(1);
	}

	/* unfix these pages */
	for (i=0; i < PF_MAX_BUFS; i++){
		if ((error=PF_UnfixPage(fd,i,TRUE))!= PFE_OK){
			PF_PrintError("unfix buffer\n");
			exit(1);
		}
	}

	/* close the file */
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file1\n");
		exit(1);
	}

}

/**************************************************************
print the content of file
*************************************************************/
readfile(fname)
char *fname;
{
int error;
int *buf;
int pagenum;
int fd;

	printf("opening %s\n",fname);
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file");
		exit(1);
	}
	printfile(fd);
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file");
		exit(1);
	}
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
		printf("got page %d, %d\n",pagenum,*buf);
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

