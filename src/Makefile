include ../Makefile.cfg
OBJS    = mlog2.o client.o local_client.o data_store.o partitioner.o messages.o range_server.o mdhim_options.o mdhim-abridge.o


ifeq ($(LEVELDB),1)
	OBJS += ds_leveldb.o
else
	ifeq ($(ROCKSDB),1)
		OBJS += ds_leveldb.o
	endif
endif

ifeq ($(MYSQLDB),1)
	OBJS += ds_mysql.o
endif


all:	mdhim.o lib dylib 

mdhim.o: mdhim.c $(OBJS)
	$(CC) -c $< $(CINC) $(CLIBS) -lleveldb

mlog2.o: Mlog2/mlog2.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

client.o: client.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

local_client.o: local_client.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

messages.o: messages.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

partitioner.o: partitioner.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

data_store.o: data_store.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

ds_mysql.o: ds_mysql.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

ds_leveldb.o: ds_leveldb.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

ds_sophia.o: ds_sophia.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

range_server.o: range_server.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

mdhim_options.o : mdhim_options.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

mdhim-abridge.o : mdhim-abridge.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

lib:	
	ar rvs libmdhim.a mdhim.o $(OBJS)

dylib:
	$(CC) -shared -o libmdhim.so.1 mdhim.o $(OBJS) -L/users/acaldwell/projects/sources/leveldb/leveldb-1.15.0/ -lleveldb

clean:
	rm -rf *.o libmdhim.a libmdhim.so.1
