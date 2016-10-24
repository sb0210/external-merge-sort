#PUBLICDIR= /usr0/cs564/public/project
SRC= buf.c hash.c pf.c
OBJ= buf.o hash.o pf.o
HDR = pftypes.h pf.h 

pflayer.o: $(OBJ)
	ld -r -o pflayer.o $(OBJ)

tests: testhash mytestpf

mytestpf: mytestpf.o pflayer.o
	cc -o mytestpf mytestpf.o pflayer.o

testhash: testhash.o pflayer.o
	cc -o testhash testhash.o pflayer.o

$(OBJ): $(HDR)

testhash.o: $(HDR)
	gcc -c testhash.c

mytestpf.o: $(HDR)
	gcc -c mytestpf.c

lint: 
	lint $(SRC)

install: pflayer.o 

clean:
	rm -f *.o testhash mytestpf tests output 

cleaninp: 
	rm -f input generate_input generate_input.o

inp: generate_input.o pflayer.o
	cc -o generate_input generate_input.o pflayer.o
	

generate_input.o: $(HDR)
	gcc -c generate_input.c

