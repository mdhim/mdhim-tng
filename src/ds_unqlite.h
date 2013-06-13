#include "mdhim.h"
#include "data_store.h"

int mdhim_unqlite_open(void *dbh, char *path, int flags, struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_put(void *dbh, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get(void *dbh, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get_next(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_unqlite_get_prev(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_unqlite_close(void *dbh, struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_del(void *dbh, void *keys, int key_lens, 
		      struct mdhim_store_opts_t *mstore_opts);
