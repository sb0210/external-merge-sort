#include <stdio.h>
#include "am.h"
#include "pf.h"
#include "testam.h"
#include <time.h>
#include <sys/time.h>

#define MAXRECS	10000	/* max # of records to insert */
#define FNAME_LENGTH 80	/* file name size */
#define LEN 500

main(int argc,char* argv[])
{
char* RELNAME1;
int fd;	/* file descriptor for the index */
char fname[FNAME_LENGTH], buf[LEN];	/* file name */
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
	// printf("initializing\n");
	PF_Init();

	/* create index */
	// printf("creating index\n");
	AM_CreateIndex(RELNAME1,0,INT_TYPE,sizeof(int));

	/* open the index */
	sprintf(fname,"%s.0",RELNAME1);
	// printf("opening index\n");
	fd = PF_OpenFile(fname);
	double tim=0;
	/* first, make sure that simple deletions work */
	int n=6;
	AM_InsertEntry(fd,INT_TYPE,sizeof(int),(char *)&n,10);

	// printf("inserting into index\n");
	while(fgets(row,LEN,fp))
	{
		int num=atoi(row);
		sprintf(buf,"%d.%d",num,recnum);
		gettimeofday(&tv1, NULL);
		AM_InsertEntry(fd,INT_TYPE,sizeof(int),buf,IntToRecId(recnum));
		gettimeofday(&tv2, NULL);
		double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
		tim+=t;
		recnum++;
	}
	

	double scan_time=0;
	// AM_PrintTree(fd,0,INT_TYPE);
	numrec= 0;
	sd = AM_OpenIndexScan(fd,INT_TYPE,sizeof(int),EQ_OP,NULL);
	while(1){
		gettimeofday(&tv1, NULL);
		recnum=RecIdToInt(AM_FindNextEntry(sd));
		gettimeofday(&tv2, NULL);
		double t=(tv2.tv_sec-tv1.tv_sec)*1000000.0+(tv2.tv_usec) - (tv1.tv_usec);
		scan_time+=t;
		if(recnum<0)break;
		// printf("%d\n",recnum);
		numrec++;
	}
	// printf("retrieved %d records\n",numrec);
	// printf("Time for insertion %lf\n", tim);
	printf(" Time for scan %lf\n", scan_time+tim);
	
	AM_CloseIndexScan(sd);

	
	/* destroy everything */
	// printf("closing down\n");
	PF_CloseFile(fd);
		// AM_DestroyIndex(RELNAME1,0);

	// printf("file done!\n");
}
