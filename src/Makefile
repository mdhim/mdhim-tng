include ../Makefile.cfg
OBJS    = mlog.o client.o local_client.o data_store.o ds_leveldb.o partitioner.o messages.o range_server.o mdhim_options.o
all:	mdhim.o lib

mdhim.o: mdhim.c $(OBJS)
	$(CC) -c $< $(CINC) $(CLIBS)

mlog.o: Mlog/mlog.c
	$(CC) -c $^ $(CINC) $(CLIBS)

client.o: client.c
	$(CC) -c $^ $(CINC) $(CLIBS)

local_client.o: local_client.c
	$(CC) -c $^ $(CINC) $(CLIBS)

messages.o: messages.c
	$(CC) -c $^ $(CINC) $(CLIBS)

partitioner.o: partitioner.c
	$(CC) -c $^ $(CINC) $(CLIBS)

data_store.o: data_store.c
	$(CC) -c $^ $(CINC) $(CLIBS)

ds_leveldb.o: ds_leveldb.c
	$(CC) -c $^ $(CINC) $(CLIBS)

range_server.o: range_server.c
	$(CC) -c $^ $(CINC) $(CLIBS)
	
mdhim_options.o : mdhim_options.c
	$(CC) -c $^ $(CINC) $(CLIBS)

lib:	
	ar rvs libmdhim.a mdhim.o $(OBJS)

clean:
	rm -rf *.o libmdhim.a
