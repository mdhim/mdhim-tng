MPI_ROOT	= /usr/local

CC	= mpicc -g -O0 -Wall
CLIBS	= -L${MPI_ROOT}/lib -L../src
CINC	= -I${MPI_ROOT}/include -IMlog/ -Iuthash -I../src -I../src/uthash

CFLAGS	= -lmpi -lmdhim -lm

all:	get-put bget-bput

get-put: get-put.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bget-bput: bget-bput.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

clean:
	rm -rf get-put bget-bput
