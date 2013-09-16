MPI_ROOT	= /usr/local
LEVEL_DB_ROOT   = /users/hng/leveldb-1.12.0

CC	= mpicc -g -O0 -Wall
CLIBS	= -L${MPI_ROOT}/lib -L../src -L${LEVEL_DB_ROOT}
CINC	= -I${MPI_ROOT}/include -I${LEVEL_DB_ROOT}/include -IMlog/ -Iuthash -I../src -I../src/uthash
SINGLE = single_tests
TESTER = tester
CFLAGS	= -lmpich -lmdhim -lm -lpthread -lstdc++ ${LEVEL_DB_ROOT}/libleveldb.a

all:	put-get bput-bget put-del bput-bdel mdhimtst put-getn \
	put-getp puts-gets bput-bgetn

put-get: $(SINGLE)/put-get.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

puts-gets: $(SINGLE)/puts-gets.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

put-getn: $(SINGLE)/put-getn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

put-getp: $(SINGLE)/put-getp.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

put-del: $(SINGLE)/put-del.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

bput-bget: $(SINGLE)/bput-bget.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

bput-bgetn: $(SINGLE)/bput-bgetn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

bput-bdel: $(SINGLE)/bput-bdel.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(SINGLE)/$@

mdhimtst: $(TESTER)/mdhimtst.c
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $(TESTER)/$@

clean:
	rm -rf $(SINGLE)/put-get $(SINGLE)/bput-bget $(SINGLE)/put-del $(SINGLE)/bput-bdel\
               $(TESTER)/mdhimtst $(SINGLE)/put-getn \
	       $(SINGLE)/put-getp $(SINGLE)/puts-gets \
               $(SINGLE)/bput-bgetn
