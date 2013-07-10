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
#define LEVELDB 2 //LEVELDB storage method

/* mdhim_store_t flags */
#define MDHIM_CREATE 1 //Implies read/write 
#define MDHIM_RDONLY 2
#define MDHIM_RDWR 3

struct mdhim_store_t;
struct mdhim_store_cur_t;
struct mdhim_store_opts_t;
struct mdhim_store_cur_opts_t;


/* Function pointers for abstracting data stores */
typedef int (*mdhim_store_open_fn_t)(void **db_handle, char *path, int flags, struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_put_fn_t)(void *db_handle, void *key, int key_len, void *data, int32_t data_len, 
				    struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_get_fn_t)(void *db_handle, void *key, int key_len, void **data, int32_t *data_len, 
				    struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_get_next_fn_t)(void *db_handle, void *curh, void **key, int *key_len,
					 void **data, int32_t *data_len, 
					 struct mdhim_store_cur_opts_t *mstore_cur_opts);
typedef int (*mdhim_store_get_prev_fn_t)(void *db_handle, void *curh, void **key, int *key_len,
					 void **data, int32_t *data_len, 
					 struct mdhim_store_cur_opts_t *mstore_cur_opts);
typedef void *(*mdhim_store_cursor_init_fn_t)(void *db_handle);
typedef int (*mdhim_store_cursor_release_fn_t)(void *db_handle, void *cursor);
typedef int (*mdhim_store_del_fn_t)(void *db_handle, void *key, int key_len,
				    struct mdhim_store_opts_t *mstore_opts);
typedef int (*mdhim_store_commit_fn_t)(void *db_handle);
typedef int (*mdhim_store_close_fn_t)(void *db_handle, struct mdhim_store_opts_t *mstore_opts);

/* Generic mdhim storage object */
struct mdhim_store_t {
	int type;
	void *db_handle;
	//Pointers to functions based on data store
	mdhim_store_open_fn_t open;
	mdhim_store_put_fn_t put;
	mdhim_store_get_fn_t get;
	mdhim_store_get_next_fn_t get_next;
	mdhim_store_get_prev_fn_t get_prev;
	mdhim_store_cursor_init_fn_t cursor_init;
	mdhim_store_cursor_release_fn_t cursor_release;
	mdhim_store_del_fn_t del;
	mdhim_store_commit_fn_t commit;
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
//Initializes a db cursor
struct mdhim_store_cur_t *mdhim_cursor_init(struct mdhim_store_t *mds, int type);
#endif
