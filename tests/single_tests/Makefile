include ../../Makefile.cfg
CLIBS += -L../../src
CINC += -I../../src -I../../src/uthash
all:	put-get bput-bget put-del bput-bdel put-getn \
<<<<<<< HEAD
	put-getp puts-gets bput-bgetn bput-bgetp plfs-put-get
=======
	put-getp puts-gets bput-bgetn bput-bgetp \
	bput-bget_secondary put-get_secondary \
	put-get_secondary_local bput-bget_secondary_local \
	put-getn_secondary put-getn_secondary_local \
	put-del_secondary put-getp_secondary put-get_2secondary_local \
	put-del_secondary_local
>>>>>>> origin/secondary_indexes

put-get: put-get.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

<<<<<<< HEAD
plfs-put-get: plfs-put-get.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@
=======
put-get_secondary: put-get_secondary.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-get_secondary_local: put-get_secondary_local.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-get_2secondary_local: put-get_2secondary_local.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

>>>>>>> origin/secondary_indexes
puts-gets: puts-gets.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getn: put-getn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getn_secondary: put-getn_secondary.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getn_secondary_local: put-getn_secondary_local.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-getp_secondary: put-getp_secondary.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@


put-getp: put-getp.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-del: put-del.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-del_secondary: put-del_secondary.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

put-del_secondary_local: put-del_secondary_local.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bget: bput-bget.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bget_secondary: bput-bget_secondary.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bget_secondary_local: bput-bget_secondary_local.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bgetn: bput-bgetn.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bgetp: bput-bgetp.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

bput-bdel: bput-bdel.c 
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

clean:
	rm -rf put-get bput-bget put-del bput-bdel\
<<<<<<< HEAD
               put-getn put-getp \
               puts-gets bput-bgetn \
               bput-bgetp
=======
		put-getn put-getp \
		puts-gets bput-bgetn \
		bput-bgetp bput-bget_secondary \
		put-get_secondary put-get_secondary_local \
		bput-bget_secondary_local put-getn_secondary_local \
		put-getn_secondary put-del_secondary put-getp_secondary \
		put-get_2secondary_local put-del_secondary_local
>>>>>>> origin/secondary_indexes
