#include "mdhim.h"
#include "data_store.h"

int mdhim_leveldb_open(void **dbh, char *path, int flags, struct mdhim_store_opts_t *mstore_opts);
int mdhim_leveldb_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_leveldb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_leveldb_get_next(void *dbh, void *curh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_leveldb_get_prev(void *dbh, void *curh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts);
void *mdhim_leveldb_cursor_init(void *dbh);
int mdhim_leveldb_cursor_release(void *dbh, void *curh);
int mdhim_leveldb_close(void *dbh, struct mdhim_store_opts_t *mstore_opts);
int mdhim_leveldb_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_leveldb_commit(void *dbh);
