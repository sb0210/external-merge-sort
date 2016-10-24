/* testpf.c */
#include <stdio.h>
#include "pf.h"
#include "pftypes.h"
#include <strings.h>
#include <time.h>
#include <stdlib.h>
#include <limits.h>

#define INT_SIZE 4
#define NUM_RECORDS 1024
# define max_pages 1024

//prints the entire page
void printPage(int* buf,int pagenum){
	int i;
	// printf("data %d\n", pagenum);
	for(i=0;i<PF_PAGE_SIZE/INT_SIZE-1;i++){
		int data=*((int *)(buf+i));
		printf("%d , ",data);
	}
	printf("\n");
}

//prints the file pagewise
void printFile(int fd){
	int error;
	int *buf;
	int pagenum;

	// printf("reading file\n");
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
	// printf("eof reached\n");

}
//helper function for generating strings of file name as run_num.pass_num
char *createname(int pass_num, int run_num){
	static char file_name[40],tmp[40];
	sprintf(tmp, "%d", pass_num);
	sprintf(file_name, "%d", run_num);
	strcat(file_name,".");strcat(file_name,tmp);
	return file_name;
}

//writes a page from input buffer to output buffer
void WritePage(int* buf_in,int* buf_out){
	int i;
	// printf("buffer in to out \n");
	for(i=0;i<NUM_RECORDS;i++){
		*(buf_out+i)=*(buf_in+i);
	}

}

void mysort(int* data){
	// Sort the buffer denoted by poniter data internally

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

//for a merge phase, takes BUF_SIZE files from fds[], merges them and writes output to outfd
//action in buffer
void merge(int* fds, int size, int outfd){
	int error;
	int i;

	int *base[size];  //base pointer of each page
	int pagenums[size]; // pg no. of each page
	int curr[size];     // record no. of current data
	short valid[size];   // if file has records to be read
	int eof_reached=0;   // no. of files ended

	for(i=0; i<size; i++){   //load first page of all files
		// printf("%d File Descriptor %d\n",i, fds[i] );
		if (( error = PF_GetFirstPage(fds[i], &pagenums[i], &base[i]) )!= PFE_OK){
			// printf(" pagenum %d\n",pagenums[i]);

			if (error == PFE_EOF){
				// PF_PrintError("eof\n");
			}
			else{
				// PF_PrintError("not eof\n");
			}
			exit(1);
		}
		
		curr[i]=0;
		valid[i]=1;	
	}
	
	int outpagenum; //page number of page of output file which is being written   
	int* outbuf;   // base pointer of output page
	//allocate a page to output buf
	if ((error=PF_AllocPage(outfd,&outpagenum,&outbuf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}

	int count=0; // no. of record which is being written in output buf
	// printf("Start Merge\n");
	while(eof_reached<size){
		// printf("%d eof_reached \n", eof_reached );
		int min= INT_MAX;
		int loc=-1;

		for(i=0; i<size; i++){
			if(valid[i]==1){
				if(*( base[i] + curr[i]) < min){
					min = *( base[i] + curr[i]);
					loc= i;
				}
			}
			// else printf("Close %d || ", i );
		}
		if(loc<0){
			printf("no page selected \n");
			exit(1);
		}

		if(count>=NUM_RECORDS){     //output buf full
			//fresh page
			// printf("page unfixed %d for output\n",outpagenum );
			PF_UnfixPage(outfd,outpagenum,TRUE);
			if ((error=PF_AllocPage(outfd,&outpagenum,&outbuf))!= PFE_OK){
				PF_PrintError("first buffer\n");
				exit(1);
			}
			count=0;
		}
		// printf("Inserting Min %d Loc %d \n", min, loc);
		*(outbuf+count) =min;  //write to output buf
		count++;
		curr[loc]++;
		if(curr[loc]>=NUM_RECORDS){      // page ended
			PF_UnfixPage(fds[loc],pagenums[loc], FALSE);
			// printf("page unfixed %d for file %d\n",pagenums[loc],loc);
			if(PF_GetNextPage(fds[loc], &pagenums[loc], &base[loc]) == PFE_EOF){   //file ended
				eof_reached++;
				valid[loc]=0;
			}
			curr[loc]=0;
		}
	}
	PF_UnfixPage(outfd,outpagenum,TRUE);
}

int main(int argc, char const *argv[])
{
	if(argc<1) return -1;
	char* FILE1= argv[1];
	int error,end_of_file=0;
	int i,k;
	int pagenum1,pagenum2,pagenum,*buf;
	int *buf1,*buf2;
	int fd1,fd2;
	struct timeval tv1, tv2;

	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open input\n");
		exit(1);
	}
	// printf("opened input\n");
	
	// printf("reading input file\n");
	//printFile(fd1);

	pagenum1=-1;	

	//SORT PHASE
	int run_num=0;
	//run_num : total out files genrated
	gettimeofday(&tv1, NULL);
	double sort_phase_time=0;

	while(end_of_file==0){
		//take max pages a buffer can contain, sort each page and write it to a new file
		for(k=0;k<PF_MAX_BUFS-1;k++){
			if ((error=PF_GetNextPage(fd1,&pagenum1,&buf1))!= PFE_OK){
				if(error==PFE_EOF) end_of_file=1;
				else {
					printf("buffer:\n");
					PFbufPrint();
					PF_PrintError("not eof\n");
					exit(1);
				}
				break;
			}

			//next page avl
			int data=*((int *)buf1);
			//generating file name
			char* f = createname(0, run_num);
			//creating new file
			if ((error=PF_CreateFile(f))!= PFE_OK){
				PF_PrintError("run %d \n",run_num);
				exit(1);
			}
			//open a new file and write sorted data to it
			if ((fd2=PF_OpenFile(f))<0){
				printf("%s\n",f );
				PF_PrintError("open run file \n");
				exit(1);
			}
			//create new page in output file
			if ((error=PF_AllocPage(fd2,&pagenum2,&buf2))!= PFE_OK){
					PF_PrintError("first buffer\n");
					exit(1);
			}
			//write to output file
			// gettimeofday(&tv1, NULL);

			WritePage((int *)buf1,(int *)buf2);		
			mysort((int*)buf2);

			// gettimeofday(&tv2, NULL);
			double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
			sort_phase_time+=t;


			if ((error=PF_UnfixPage(fd1,pagenum1,FALSE))!= PFE_OK){
				PF_PrintError("unfix");
				exit(1);
			}
			//remove from buffer
			if ((error=PF_UnfixPage(fd2,pagenum2,TRUE))!= PFE_OK){
				PF_PrintError("unfix");
				exit(1);
			}

			if (PF_CloseFile(fd2) != PFE_OK){
				PF_PrintError("close fd2");
				exit(1);
			}
			
			run_num++;
			
		}
	}
	// double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
	// printf("sort phase time %f \n",sort_phase_time);

	if (PF_CloseFile(fd1) != PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}
	
	//SORT PHASE COMPLETED
	//All files closed

	//MERGE PHASE
	//in each phase (pass_num) , input files (=run_size) are merged and written to output files (=next_run)
	// for next phase, output files become the input files
	//naming convention: file 2.1 marks 1 pass and 2nd file 
	int run_size=run_num,next_run=0,pass_num=1;
 	// printf("run_num for first merge %d\n",run_num);
	while(run_size>1){

		run_num=0; // files read so far in this pass
		next_run=0; // files generated so far in this pass
		//iterate over each input file
		while(run_num<run_size){
			
			char* f=createname(pass_num,next_run);
			
			if ((error=PF_CreateFile(f))!= PFE_OK){
			PF_PrintError("run %d \n",run_num);
			exit(1);
			}

			if ((fd2=PF_OpenFile(f))<0){
				PF_PrintError("open run file\n");
				exit(1);
			}

			int fd_merge[PF_MAX_BUFS-1],k;
			//a max of PF_MAX_BUFS-1 files can be merged at a time
			for(k=0;k<PF_MAX_BUFS-1 && run_num<run_size;k++){
				char* input_file = createname(pass_num-1, run_num);
				// printf("files to be merged %d %s \n",k,input_file);
				if ((fd_merge[k]=PF_OpenFile(input_file))<0){
					PF_PrintError("open input\n");
					exit(1);
				}
				run_num++;
			}

			merge(fd_merge,k,fd2);
			next_run++;		//new output file created
			int j;
			for(j=0; j<k; j++){
				if ((error=PF_CloseFile(fd_merge[j]))!= PFE_OK){
					PF_PrintError("close file");
					exit(1);
				}
			}
			if ((error=PF_CloseFile(fd2))!= PFE_OK){
				PF_PrintError("close file");
				exit(1);
			}
		}
		//all input files, already sorted are destroyed
		int t;
		for(t=0;t<run_size;t++){
			char* file_name=createname(pass_num-1, t);
			PF_DestroyFile(file_name);
		}
		// printf("next_run %d for pass %d \n",next_run,pass_num);
		run_size=next_run;
		pass_num++;
	}
	gettimeofday(&tv2, NULL);
	double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
	printf("Merge time %lf \n",t);
	//last file created, is the sorted file
	return 0;
}
