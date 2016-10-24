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
# define INT_MIN -32767
# define max_pages 1024

void printPage(char** buf,int pagenum){
	int i;
	printf("new data for %d\n", pagenum);
	for(i=0;i<PF_PAGE_SIZE/INT_SIZE-1;i++){
		int data=*((int *)(buf+i)); //resolve later
		printf("%d , ",data);
	}
	printf("\n");
}

void WritePage(char** buf_in,char** buf_out){
	int i;
	printf("buffer in to out \n");
	for(i=0;i<NUM_RECORDS;i++){
		*((int *)(buf_out+i))=*((int *)(buf_in+i)); //resolve later
	}

}

void mysort(int* data){
	// Sort the buffer denoted by pageaddr internally

	// struct PFfpage* page = &(pageaddr->fpage);
	// int* data = (int *)page->pagebuf;

	int* curr;
	int* next;

	int i, j, k;
	for(i=0; i<NUM_RECORDS-1; i++){//number of iterations
		short swap =0;
		for(j=0; j<NUM_RECORDS-i-1; j++){
			curr= data+ j; //* (INT_SIZE);
			next= curr++;
			int tmp;
			//printf("comparing current and next %d %d\n",*curr,*next );
			if(*curr < *next){
				tmp=*curr; *curr=*next;*next=tmp;
				swap = 1;
			}
		}
		if(swap==0) break;
	}
}

void merge(int* fds, int size, int outfd){
	int error;
	int i;

	int* base[size];  //base pointer of each page
	int pagenums[size]; // pg no. of each page
	int curr[size];     // record no. of current data
	short valid[size];   // if file has records to be read
	int eof_reached=0;   // no. of files ended

	for(i=0; i<size; i++){   //load first page of all files
		PF_GetFirstPage(fds[i], &pagenums[i], &base[i]);
		curr[i]=0;
		valid[i]=1;	
	}
	

	int outpagenum;   
	int* outbuf;   // base pointer of output page
	//allocate a page to output buf
	if ((error=PF_AllocPage(outfd,&outpagenum,&outbuf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}

	int count=0; // no. of record which is being written in output buf



	while(eof_reached<size){
		int min= INT_MIN;
		int loc=-1;

		for(i=0; i<size; i++){
			if(valid[i]==1){
				if(*( base[i] + curr[i]) < min){
					min = *( base[i] + curr[i]);
					loc= i;
				}
			}
		}

		if(count>=NUM_RECORDS){     //output buf full
			//fresh page
			PFbufUnfix(outfd,outpagenum,TRUE);
			if ((error=PF_AllocPage(outfd,&outpagenum,&outbuf))!= PFE_OK){
				PF_PrintError("first buffer\n");
				exit(1);
			}
			count=0;
		}
		*((int* )outbuf+count) =min;  //write to output buf

		curr[loc]++;
		if(curr[loc]>=NUM_RECORDS){      // page ended
			PFbufUnfix(fds[loc],pagenums[loc], FALSE);
			if(PF_GetNextPage(fds[loc], &pagenums[loc], &base[loc]) == PFE_EOF){   //file ended
				eof_reached++;
				valid[i]=0;
			}
			curr[loc]=0;
		}
	}
	PFbufUnfix(outfd,outpagenum,TRUE);


}

main()
{
int error,end_of_file=0;
int i,k;
int pagenum1=-1,pagenum2=-1,pagenum=-1,*buf;
int *buf1,*buf2;
int fd1,fd2;
PFbpage * bpage;
// int* buf_in[PF_MAX_BUFS-1],*buf_out;
// buffer in[PF_MAX_BUFS-1],out;
int fd_out[1024],pg_num[PF_MAX_BUFS-1];

	
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open input\n");
		exit(1);
	}
	printf("opened input\n");
	
	printf("reading input file\n");

	

	//loading from input buffer
	int run_num=0;
	while(end_of_file==0){
		for(k=0;k<PF_MAX_BUFS-1;k++){
			if ((error=PF_GetNextPage(fd1,&pagenum1,&buf1))== PFE_OK){		//next page avl
				int data=*((int *)buf1);
				printf("old data : \n buff adrr: %p", buf1);
				printf(" page: %d data: %d\n",pagenum1,data);
				if ((bpage=PFhashFind(fd1,pagenum1)) != NULL){	
					printf("YO! Before Sort\n");	
					// printPage(buf1, pagenum1);		
					mysort((int *) buf1);
					printf("YO! Sort\n");
					// printPage(buf1, pagenum1);
				}
				else {
					PF_PrintError("buffer addr retrieving error\n");
					exit(1);
				}

				printf("new data : \n buff adrr: %p", buf1);
				printf(" page: %d data: %d\n",pagenum1,data);
				//generating file name
				char f[40];
				sprintf(f, "%d", run_num);
				strcat(f,".0");
				//creating new file
				if ((error=PF_CreateFile(f))!= PFE_OK){
					PF_PrintError("run %d \n",run_num);
					exit(1);
				}
				printf("%s blank file created\n",f);	

				if ((fd2=PF_OpenFile(f))<0){
					PF_PrintError("open run file\n");
					exit(1);
				}
				if ((error=PF_AllocPage(fd2,&pagenum2,&buf2))!= PFE_OK){
						PF_PrintError("first buffer\n");
						exit(1);
				}
				//write to output file
				WritePage(buf1,buf2);
				if ((error=PF_UnfixPage(fd1,pagenum1,FALSE))!= PFE_OK){
					PF_PrintError("unfix");
					exit(1);
				}
				if ((error=PF_UnfixPage(fd2,pagenum2,TRUE))!= PFE_OK){
					PF_PrintError("unfix");
					exit(1);
				}
				fd_out[run_num]=fd2;
				run_num++;
				printf("buffer: %d\n", pagenum1);
				PFbufPrint();

			}
			else break; //eof or otherwise
		}
		if(error==PFE_EOF){
		end_of_file=1;
		}
		else if (error != PFE_OK){
			PF_PrintError("not eof\n");
			printf("buffer:\n");
			PFbufPrint();	
			exit(1);
		}
	}
	if (PF_CloseFile(fd1) != PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}
	else{
			printf("closed input file\n");
	}

/*
	int run_size=run_num,next_run=0,phase_num=1;
	int fd_in[max_pages];
	while(run_size>1){
		//create new file		
		int k,t;
		int fd_out[run_size]; //setbit later
		run_num=0,next_run=0;
		char pass[40];
		sprintf(pass, "%d", phase_num);

		//making latest run the current run
		for(t=0;t<run_size;t++){
			fd_in[t]=fd_out[t];
		}
		while(run_num<run_size){
			char f[40];
			sprintf(f, "%d", run_num);
			strcat(f,".");strcat(f,pass);

			if ((error=PF_CreateFile(f))!= PFE_OK){
			PF_PrintError("run %d \n",run_num);
			exit(1);
			}
			printf("%s blank file created\n",f);	

			if ((fd_out[next_run]=PF_OpenFile(f))<0){
				PF_PrintError("open run file\n");
				exit(1);
			}
			int fd_merge[PF_MAX_BUFS-1], siz=0,k;
			for(k=0;k<PF_MAX_BUFS-1 && run_num<run_size;k++){
				fd_merge[k]=fd_in[run_num];
				run_num++;
				siz++;
			}
			if(siz>0){
				merge(fd_merge,siz,fd_out[next_run]);
				next_run++;
			}
		}
		for(t=0;t<run_size;t++){
			if ((error=PF_CloseFile(fd_in[t]))!= PFE_OK){
			PF_PrintError("close file");
			exit(1);
			char file_name[40],tmp[40];
			sprintf(tmp, "%d", phase_num-1);
			sprintf(file_name, "%d", t);
			strcat(file_name,".");strcat(file_name,tmp);
			// PF_DestroyFile(file_name);

		}
	}

		run_size=next_run;
		phase_num++;
	}

*/	
//close the last file
	/*
	if (PF_CloseFile(fd1) != PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}
	else{
			printf("closed file1\n");
	}
*/
	
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

