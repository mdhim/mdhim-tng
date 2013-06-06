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

struct mdhim_store_t *mdhim_db_open(char *path, int type, struct mdhim_store_opts_t *mstore_opts);
int mdhim_db_put( struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
		  struct mdhim_store_opts_t *mstore_opts);
int mdhim_db_bput( struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
		   struct mdhim_store_opts_t *mstore_opts);
int mdhim_db_get(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
		 struct mdhim_store_opts_t *mstore_opts);
int mdhim_db_bget( struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
		   struct mdhim_store_opts_t *mstore_opts);
int mdhim_db_get_next( struct mdhim_store_cur_t *mcur, void *key, int *key_len, void *data, int *data_len, 
		       struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_db_get_prev( struct mdhim_store_cur_t *mcur, void *key, int *key_len, void *data, int *data_len, 
		       struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_db_close( struct mdhim_store_t *mstore, struct mdhim_store_opts_t *mstore_opts);

#endif
