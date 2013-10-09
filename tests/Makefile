include ../Makefile.cfg
SINGLE = single_tests
TESTER = tester
TLIBS = $(CLIBS) -L../src
TINC = $(CINC) -I../src -I../src/uthash
all:	put-get bput-bget put-del bput-bdel mdhimtst put-getn \
	put-getp puts-gets bput-bgetn

put-get: $(SINGLE)/put-get.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

puts-gets: $(SINGLE)/puts-gets.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

put-getn: $(SINGLE)/put-getn.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

put-getp: $(SINGLE)/put-getp.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

put-del: $(SINGLE)/put-del.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

bput-bget: $(SINGLE)/bput-bget.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

bput-bgetn: $(SINGLE)/bput-bgetn.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

bput-bdel: $(SINGLE)/bput-bdel.c 
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(SINGLE)/$@

mdhimtst: $(TESTER)/mdhimtst.c
	$(CC) $< $(TINC) $(TLIBS) $(CFLAGS) -o $(TESTER)/$@

clean:
	rm -rf $(SINGLE)/put-get $(SINGLE)/bput-bget $(SINGLE)/put-del $(SINGLE)/bput-bdel\
               $(TESTER)/mdhimtst $(SINGLE)/put-getn \
	       $(SINGLE)/put-getp $(SINGLE)/puts-gets \
               $(SINGLE)/bput-bgetn
