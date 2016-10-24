#include<stdio.h>

int main(){
	int *buf;
	printf("%p\n", buf);
	buf++;
	printf("%p\n", buf);

	int array[2];
	int x;
	printf("%lu\n", sizeof(x));
	printf("%lu\n", sizeof(buf));
	array[0]=1;
	array[1]=2;

	buf=&array[0];
	printf("%p\n", buf);
	printf("%d\n", *buf);
	buf++;
	printf("%p\n", buf);
	printf("%d\n", *buf);

	return 0;

}