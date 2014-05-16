#ifndef      __LEVELDB_H
#define      __LEVELDB_H

#ifndef      LEVELDB_SUPPORT
#include <rocksdb/c.h>
#else
#include <leveldb/c.h>
#endif

#include "mdhim.h"
#include "partitioner.h"
#include "data_store.h"

/* Function pointer for comparator in C */
typedef int (*mdhim_store_cmp_fn_t)(void* arg, const char* a, size_t alen,
				    const char* b, size_t blen);

struct mdhim_leveldb_t {
	leveldb_t *db;
	leveldb_options_t *options;
	leveldb_comparator_t* cmp;
	leveldb_filterpolicy_t *filter;
	leveldb_cache_t *cache;
	leveldb_env_t *env;
	leveldb_writeoptions_t *write_options;
	leveldb_readoptions_t *read_options;
	mdhim_store_cmp_fn_t compare;
};

int mdhim_leveldb_open(void **dbh, void **dbs, char *path, int flags, int key_type);
int mdhim_leveldb_put(void *dbh, void *key, int key_len, void *data, int32_t data_len);
int mdhim_leveldb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len);
int mdhim_leveldb_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len);
int mdhim_leveldb_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len);
int mdhim_leveldb_close(void *dbh, void *dbs);
int mdhim_leveldb_del(void *dbh, void *key, int key_len);
int mdhim_leveldb_commit(void *dbh);
int mdhim_leveldb_batch_put(void *dbh, void **key, int32_t *key_lens, 
			    void **data, int32_t *data_lens, int num_records);
#endif
