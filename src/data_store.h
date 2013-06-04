/*
 * MDHIM TNG
 * 
 * Data store abstraction
 */

#ifndef      __STORE_H
#define      __STORE_H

#include "unqlite.h"

/* Storage Methods */
#define UNQLITE 1 //UNQLITE storage method

/* mdhim_store_t flags */
#define MDHIM_CREATE 1 //Implies read/write 
#define MDHIM_RDONLY 2
#define MDHIM_RDWR 3

/* Generic mdhim storage object */
struct mdhim_store_t {
	int type;
	void *db_handle;
	int flags;
};

/* Generic mdhim cursor object */
struct mdhim_store_cur_t {
	void *db_cursor;
	int flags;
};


/* mdhim storage options passed to direct storage access functions i.e.: get, put, open */
struct mdhim_store_opts_t {
  int mem_only; //Specific to unqlite - this creates a temporary in memory database on MDHIM_CREATE
};


/* mdhim cursor options passed to storage access functions that require a cursor 
 * i.e.: get_next, get_prev */
struct mdhim_store_cur_opts_t {
};

#endif
