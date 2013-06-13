MPI_ROOT	= /usr/local

CC	= mpicc -g -O0 -Wall
CLIBS	= -L${MPI_ROOT}/lib
CINC	= -I${MPI_ROOT}/include -IMlog/ -Iuthash
CFLAGS	= 

OBJS    = mlog.o unqlite.o client.o data_store.o ds_unqlite.o partitioner.o messages.o range_server.o
all:	mdhim.o lib

mdhim.o: mdhim.c $(OBJS)
	$(CC) -c $< $(CINC) $(CLIBS)

mlog.o: Mlog/mlog.c
	$(CC) -c $^ $(CINC) $(CLIBS)

unqlite.o: unqlite.c
	$(CC) -c $^ $(CINC) $(CLIBS)

client.o: client.c
	$(CC) -c $^ $(CINC) $(CLIBS)

messages.o: messages.c
	$(CC) -c $^ $(CINC) $(CLIBS)

partitioner.o: partitioner.c
	$(CC) -c $^ $(CINC) $(CLIBS)

data_store.o: data_store.c
	$(CC) -c $^ $(CINC) $(CLIBS)

ds_unqlite.o: ds_unqlite.c
	$(CC) -c $^ $(CINC) $(CLIBS)

range_server.o: range_server.c
	$(CC) -c $^ $(CINC) $(CLIBS)

lib:	
	ar rvs libmdhim.a mdhim.o $(OBJS)

clean:
	rm -rf *.o libmdhim.a
