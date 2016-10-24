#include <stdio.h>
#include "pf.h"
#include "pftypes.h"
#include <strings.h>
#define INT_SIZE 4
void sort(PFbpage * pageaddr){
	// Sort the buffer denoted by pageaddr internally

	struct PFfpage* page = &(pageaddr->fpage);
	int* data = (int *)page->pagebuf;

	int* curr;
	int* next;

	int i, j, k;
	for(i=0; i<PF_PAGE_SIZE-1; i++){//number of iterations
		short swap =0;
		for(j=0; j<PF_PAGE_SIZE-i-1; j++){
			curr= data+ j*(INT_SIZE);
			next= curr++;
			int tmp;
			if(*curr >  *next){
				bcopy(curr, &tmp, INT_SIZE); // *tmp = *curr
				bcopy(next, curr, INT_SIZE);
				bcopy(&tmp, next, INT_SIZE);
				swap = 1;
			}
		}
		if(swap==0) break;
	}

}