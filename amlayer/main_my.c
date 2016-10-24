/* test3.c: tests deletion and scan. */
#include <stdio.h>
#include "am.h"
#include "pf.h"
#include "testam.h"
#include <time.h>
#include <sys/time.h>

#define MAXRECS	10000	/* max # of records to insert */
#define FNAME_LENGTH 80	/* file name size */
#define LEN 500
#define INT_SIZE 4

main(int argc,char* argv[])
{
char* RELNAME1;
int fd;	/* file descriptor for the index */
char fname[FNAME_LENGTH];	/* file name */
int recnum=0;	/* record number */
int sd;	/* scan descriptor */
int numrec;	/* # of records retrieved */
int testval;	
char* row[LEN];

	if (argc > 1){
        RELNAME1 = argv[1];
    }
    else {
        return 0;
    }
    struct timeval tv1, tv2;
	FILE *fp = fopen(RELNAME1, "r");
	/* init */
	printf("initializing\n");
	PF_Init();

	/* create index */
	printf("creating index\n");
	AM_CreateIndex(RELNAME1,0,INT_TYPE,sizeof(int));

	/* open the index */
	sprintf(fname,"%s.0",RELNAME1);
	printf("opening index\n");
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open input\n");
		exit(1);
	}
	double tim=0;
	int n=0;
	// AM_InsertEntry(fd,INT_TYPE,sizeof(int),(char *)&n,10);

	printf("inserting into index\n");
	int error;
	int *buf;
	int pagenum;

	printf("reading file\n");
	pagenum = -1;
	recnum=0;
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
		printf("got page %d, %d\n",pagenum,*buf);
		//traverse over page
		int i;
		for(i=0;i<PF_PAGE_SIZE/INT_SIZE;i++){
			int data=*((int *)(buf+i));
			printf("data %d ,recnum %d \n",data,recnum);
			gettimeofday(&tv1, NULL);
			AM_InsertEntry(fd,INT_TYPE,sizeof(int),(char *)&data,IntToRecId(recnum));
			gettimeofday(&tv2, NULL);
			recnum++;
			double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
			tim+=t;
		}
		
		//unfix page
		printf("unfixing page %d\n",pagenum);
		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
	}
	//error check; shudnt be incurred
	if (error != PFE_EOF){
		PF_PrintError("not eof\n");
		exit(1);
	}
	printf("eof reached\n");


	// while(fgets(row,LEN,fp))
	// {
	// 	int num=atoi(row);

	// 	AM_InsertEntry(fd,INT_TYPE,sizeof(int),(char *)&num,recnum);
	// 	gettimeofday(&tv2, NULL);
	// 	double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
	// 	tim+=t;
	// 	recnum++;
	// }
	printf("Time for insertion\n");

	printf("%lf\n", tim);

	double scan_time=0;
	AM_PrintTree(fd,0,INT_TYPE);
	numrec= 0;
	sd = AM_OpenIndexScan(fd,INT_TYPE,sizeof(int),EQ_OP,NULL);
	int value=0;
	while(1){
		gettimeofday(&tv1, NULL);
		recnum=RecIdToInt(AM_FindNextEntry(sd));
		int pgnum=recnum/(PF_PAGE_SIZE/INT_SIZE);
		int offset=recnum%(PF_PAGE_SIZE/INT_SIZE);
		if((error=PF_GetThisPage(fd,pgnum,buf))==PFE_OK){
			value=*buf+offset;
		}
		else{
			PF_PrintError("page not found \n");
			exit(1);
		}
		gettimeofday(&tv2, NULL);

		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
		double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
		scan_time+=t;
		if(recnum<0)break;
		printf("recnum %d value %d \n",recnum,value);
		numrec++;
	}
	printf("Time for scan\n");
	printf("%lf\n", scan_time);

	printf("retrieved %d records\n",numrec);
	AM_CloseIndexScan(sd);

	
	/* destroy everything */
	printf("closing down\n");
	PF_CloseFile(fd);
	AM_DestroyIndex(RELNAME1,0);

	printf("file done!\n");
}
