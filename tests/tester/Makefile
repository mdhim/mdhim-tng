include ../../Makefile.cfg
CLIBS += -L../../src
CINC += -I../../src -I../../src/uthash
all:	mdhimtst

mdhimtst: mdhimtst.c
	$(CC) $< $(CINC) $(CLIBS) $(CFLAGS) -o $@

clean:
	rm -rf mdhimtst