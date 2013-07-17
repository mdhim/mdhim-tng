MPI_ROOT	= /usr/local
LEVEL_DB_ROOT   = /turquoise/users/hng/leveldb-1.12.0

CC	= mpicc -g -O0 -Wall
CLIBS	= -L${MPI_ROOT}/lib -L../src -L${LEVEL_DB_ROOT}
CINC	= -I${MPI_ROOT}/include -I${LEVEL_DB_ROOT}/include -IMlog/ -Iuthash -I../src -I../src/uthash

CFLAGS	= -lmpi -lmdhim -lm -lpthread -lstdc++ ${LEVEL_DB_ROOT}/libleveldb.a

all:	get-put bget-bput put-del bput-bdel sort-bget-bput mdhimtst put-getn put-getp

get-put: get-put.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getn: put-getn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getp: put-getp.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-del: put-del.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bget-bput: bget-bput.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bdel: bput-bdel.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

sort-bget-bput: sort-bget-bput.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

mdhimtst: mdhimtst.c
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

clean:
	rm -rf get-put bget-bput put-del bput-bdel sort-bget-bput mdhimtst put-getn put-getp
