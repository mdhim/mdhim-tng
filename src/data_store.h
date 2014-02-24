/*
 * MDHIM TNG
 * 
 * Data store abstraction
 */

#ifndef      __STORE_H
#define      __STORE_H

#include "uthash.h"

/* Storage Methods */
#define LEVELDB 1 //LEVELDB storage method
#define ROCKSDB 4 //RocksDB
/* mdhim_store_t flags */
#define MDHIM_CREATE 1 //Implies read/write 
#define MDHIM_RDONLY 2
#define MDHIM_RDWR 3

/* Keys for stats database */
#define MDHIM_MAX_STAT 1
#define MDHIM_MIN_STAT 2
#define MDHIM_NUM_STAT 3

struct mdhim_store_t;

/* Function pointers for abstracting data stores */
typedef int (*mdhim_store_open_fn_t)(void **db_handle, void **db_stats, char *path, int flags, 
				     int key_type);
typedef int (*mdhim_store_put_fn_t)(void *db_handle, void *key, int32_t key_len, 
				    void *data, int32_t data_len);
typedef int (*mdhim_store_batch_put_fn_t)(void *db_handle, void **keys, int32_t *key_lens, 
					  void **data, int32_t *data_lens, int num_records);
typedef int (*mdhim_store_get_fn_t)(void *db_handle, void *key, int key_len, void **data, int32_t *data_len);
typedef int (*mdhim_store_get_next_fn_t)(void *db_handle, void **key, 
					 int *key_len, void **data, 
					 int32_t *data_len);
typedef int (*mdhim_store_get_prev_fn_t)(void *db_handle, void **key, 
					 int *key_len, void **data, 
					 int32_t *data_len);
typedef int (*mdhim_store_del_fn_t)(void *db_handle, void *key, int key_len);
typedef int (*mdhim_store_commit_fn_t)(void *db_handle);
typedef int (*mdhim_store_close_fn_t)(void *db_handle, void *db_stats);

//Used for storing stats in a hash table
struct mdhim_stat {
	int key;                   //Key (slice number)
	void *max;                 //Max key
	void *min;                 //Min key
	uint64_t num;              //Number of keys in this slice
	UT_hash_handle hh;         /* makes this structure hashable */
};


//Used for storing stats in the database
struct mdhim_db_stat {
	int slice;                   
	uint64_t imax;
	uint64_t imin;
	long double dmax;
	long double dmin;
	uint64_t num;
};

//Used for transmitting integer stats to all nodes
struct mdhim_db_istat {
	int slice;                   
	uint64_t num;
	uint64_t imax;
	uint64_t imin;
};

//Used for transmitting float stats to all nodes
struct mdhim_db_fstat {
	int slice;                   
	uint64_t num;
	long double dmax;
	long double dmin;
};

/* Generic mdhim storage object */
struct mdhim_store_t {
	int type;
	//handle to db
	void *db_handle;
	//Handle to db for stats
	void *db_stats;
	//Pointers to functions based on data store
	mdhim_store_open_fn_t open;
	mdhim_store_put_fn_t put;
	mdhim_store_batch_put_fn_t batch_put;
	mdhim_store_get_fn_t get;
	mdhim_store_get_next_fn_t get_next;
	mdhim_store_get_prev_fn_t get_prev;
	mdhim_store_del_fn_t del;
	mdhim_store_commit_fn_t commit;
	mdhim_store_close_fn_t close;

        //Hashtable for stats
	struct mdhim_stat *mdhim_store_stats;

	//Lock to allow concurrent readers and a single writer to the mdhim_store_stats
	pthread_rwlock_t *mdhim_store_stats_lock;
};

//Initializes the data store based on the type given (i.e., LEVELDB, etc...)
struct mdhim_store_t *mdhim_db_init(int db_type);
#endif
