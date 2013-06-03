/*
 * MDHIM TNG
 * 
 * Data store abstraction
 */

#include "unqlite.h"

/* Storage Methods */
#define UNQLITE 1 //UNQLITE storage method

/* mdhim_store_t flags */
#define MDHIM_CREATE 1 //Implies read/write 
#define MDHIM_RDONLY 2
#define MDHIM_RDWR 3

/* Generic mdhim storage object */
typedef struct mdhim_store_t {
	int type;
	void *db_handle;
	int flags;
} mdhim_store_t;

/* Generic mdhim cursor object */
typedef struct mdhim_store_cur_t {
	void *db_cursor;
	int flags;
} mdhim_store_cur_t;


/* mdhim storage options passed to direct storage access functions i.e.: get, put, open */
typedef struct mdhim_store_opts_t {
  int mem_only; //Specific to unqlite - this creates a temporary in memory database on MDHIM_CREATE
} mdhim_store_opts_t;


/* mdhim cursor options passed to storage access functions that require a cursor 
 * i.e.: get_next, get_prev */
typedef struct mdhim_store_cur_opts_t {
} mdhim_store_cur_opts_t;
