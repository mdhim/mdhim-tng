include ../Makefile.cfg
OBJS    = mlog.o client.o local_client.o data_store.o partitioner.o messages.o range_server.o mdhim_options.o mdhim_private.o indexes.o


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

mlog2.o: Mlog/mlog.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

mdhim_private.o: mdhim_private.c
	$(CC) -c $< $(CINC) $(CLIBS)

client.o: client.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

local_client.o: local_client.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

messages.o: messages.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

partitioner.o: partitioner.c
	$(CC) -c $^ $(CINC) $(CLIBS) -lleveldb

indexes.o: indexes.c
	$(CC) -c $^ $(CINC) $(CLIBS)

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

lib:	
	ar rvs libmdhim.a mdhim.o $(OBJS)

dylib:
	$(CC) -shared -o libmdhim.so.1 mdhim.o $(OBJS) -L$(LEVEL_DB_ROOT)/ -lleveldb

clean:
	rm -rf *.o libmdhim.a libmdhim.so.1
