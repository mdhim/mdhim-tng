#include "data_store.h"

int mdhim_unqlite_open(struct mdhim_store_t *mstore, char *path, int flags, struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_put(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_bput(struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
		       struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get(struct mdhim_store_t *mstore, void *key, int key_len, void *data, int data_len, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_bget(struct mdhim_store_t *mstore, void **keys, int *key_lens, void **data, int *data_lens, 
		       struct mdhim_store_opts_t *mstore_opts);
int mdhim_unqlite_get_next(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_unqlite_get_prev(struct mdhim_store_cur_t *mcur, void *data, int *data_len, 
			   struct mdhim_store_cur_opts_t *mstore_cur_opts);
int mdhim_unqlite_close(struct mdhim_store_t *mstore, struct mdhim_store_opts_t *mstore_opts);
