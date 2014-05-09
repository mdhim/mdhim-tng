include ../../Makefile.cfg
CLIBS += -L../../src
CINC += -I../../src -I../../src/uthash
all:	put-get bput-bget put-del bput-bdel put-getn \
	put-getp puts-gets bput-bgetn bput-bgetp plfs-put-get \
	plfs-put-get-abstract plfs-put plfs-get 

put-get: put-get.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

plfs-put-get: plfs-put-get.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

plfs-put: plfs-put.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

plfs-get: plfs-get.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

plfs-put-get-abstract: plfs-put-get-abstract.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

puts-gets: puts-gets.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getn: put-getn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getp: put-getp.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-del: put-del.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bget: bput-bget.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bgetn: bput-bgetn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bgetp: bput-bgetp.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bdel: bput-bdel.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

clean:
	rm -rf put-get bput-bget put-del bput-bdel\
               put-getn put-getp \
               puts-gets bput-bgetn \
               bput-bgetp plfs-put-get plfs-put-get-abstract \
               plfs-put plfs-get
