#include "mdhim.h"
#include "data_store.h"

int mdhim_unqlite_open(void **dbh, void **dbs, char *path, int flags, 
		       struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			   struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_del(void *dbh, void *key, int key_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_commit(void *dbh);
