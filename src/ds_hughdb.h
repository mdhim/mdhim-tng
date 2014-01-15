#include <stdio.h>
#include "mdhim.h"
#include "data_store.h"

int mdhim_hughdb_open(void **dbh, void **dbs, char *path, int flags, 
		      struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		     struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_batch_put(void *dbh, void **keys, int32_t *key_lens, 
			    void **data, int32_t *data_lens, int num_records,
			   struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		     struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_get_next(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			  struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_get_prev(void *dbh, void **key, int *key_len, 
			   void **data, int32_t *data_len, 
			  struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_del(void *dbh, void *key, int key_len, 
		     struct mdhim_store_opts_t *mstore_opts);
int mdhim_hughdb_commit(void *dbh);

#define HUGHDB_BUF_SIZE 52428800

typedef struct hughdb_t hughdb_t;
struct hughdb_t {
	FILE *file;
	char buf[HUGHDB_BUF_SIZE];
};

typedef struct hughdb_rec_t hughdb_rec_t;
struct hughdb_rec_t {
	int key_len;	
	int data_len;
};
