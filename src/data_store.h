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

struct mdhim_store_t;
struct mdhim_store_cur_t;
struct mdhim_store_opts_t;
struct mdhim_store_cur_opts_t;

/* Function pointers for abstracting data stores */
typedef int (*mdhim_store_open_fn_t)(char *path, int flags, struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_put_fn_t)(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
				    struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_bput_fn_t)(struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
				     struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_get_fn_t)(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
				    struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_bget_fn_t)(struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
				     struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_get_next_fn_t)(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
					 struct mdhim_store_cur_opts_t *mstore_cur_opts);
typedef int (*mdhim_store_get_prev_fn_t)(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
					 struct mdhim_store_cur_opts_t *mstore_cur_opts);
typedef int (*mdhim_store_close_fn_t)(struct mdhim_store_t *mstore, struct mdhim_store_opts_t *mstore_opts);

/* Generic mdhim storage object */
struct mdhim_store_t {
	int type;
	void *db_handle;
	//Pointers to functions based on data store
	mdhim_store_open_fn_t open;
	mdhim_store_put_fn_t put;
	mdhim_store_bput_fn_t bput;
	mdhim_store_get_fn_t get;
	mdhim_store_bget_fn_t bget;
	mdhim_store_get_next_fn_t get_next;
	mdhim_store_get_prev_fn_t get_prev;
	mdhim_store_close_fn_t close;
};

/* Generic mdhim cursor object */
struct mdhim_store_cur_t {
	void *db_cursor;
	int flags;
};

/* mdhim storage options passed to direct storage access functions i.e.: get, put, open */
struct mdhim_store_opts_t {
	int mem_only; //This creates a temporary in memory database on MDHIM_CREATE
};

/* mdhim cursor options passed to storage access functions that require a cursor 
 * i.e.: get_next, get_prev */
struct mdhim_store_cur_opts_t {
};

//Initializes the data store based on the type given (i.e., UNQLITE, LEVELDB, etc...)
struct mdhim_store_t *mdhim_db_init(int db_type);

#endif
