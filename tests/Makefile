MPI_ROOT	= /usr/local

CC	= mpicc -g -O0 -Wall
CLIBS	= -L${MPI_ROOT}/lib -L../src
CINC	= -I${MPI_ROOT}/include -IMlog/ -Iuthash -I../src -I../src/uthash

CFLAGS	= -lmpi -lmdhim -lm

all:	test1

test1: test1.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

clean:
	rm -rf test1
