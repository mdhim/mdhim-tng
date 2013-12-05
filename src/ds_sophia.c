#include <stdlib.h>
#include <string.h>
#include <sophia.h>
#include <stdio.h>
#include <linux/limits.h>
#include <sys/time.h>
#include "ds_leveldb.h"

void open_db(void **env, void **db, char *path, int flags) {
	int rc;

	*db = NULL;
	*env = NULL;

	//Create the environment
	*env = sp_env();
	if (*env == NULL) {
		/* memory allocation error */
		mlog(MDHIM_SERVER_CRIT, "Error creating environment while opening sophia database");
		return;
	}
	
	rc = sp_ctl(*env, SPDIR, SPO_CREAT|SPO_RDWR, path);
	if (rc == -1) {
		mlog(MDHIM_SERVER_CRIT, "Error setting sophia environment: %s", sp_error(*env));
		sp_destroy(*env);
		return;
	}
	
	//Open the database
	*db = sp_open(*env);
	if (*db == NULL) {
		mlog(MDHIM_SERVER_CRIT, "Error opening sophia database: %s", sp_error(*env));
		sp_destroy(*env);
		return;
	}

	return;
}

int mdhim_sophia_open(void **dbh, void **dbs, char *path, int flags, 
		      struct mdhim_store_opts_t *mstore_opts) {
	char stats_path[PATH_MAX];
	void *env, *db;

	//Open the main database
	open_db(&env, &db, path, flags);
	if (!env || !db) {
		return MDHIM_DB_ERROR;
	}

	//Set a pointer to the environment
	mstore_opts->db_ptr1 = env;
	//Set the database handle
	*dbh = db;
	
	//Open the stats database
	sprintf(stats_path, "%s_stats", path);
	open_db(&env, &db, stats_path, flags);
	if (!env || !db) {
		return MDHIM_DB_ERROR;
	}

	//Set a pointer to the environment
	mstore_opts->db_ptr2 = env;
	//Set the database handle
	*dbs = db;

	return MDHIM_SUCCESS;
}

int mdhim_sophia_put(void *dbh, void *key, int key_len, void *data, int32_t data_len, 
		     struct mdhim_store_opts_t *mstore_opts) {
	int rc;

	rc = sp_set(dbh, key, key_len, data, data_len);
	if (rc == -1) {
		mlog(MDHIM_SERVER_CRIT, "Error putting key/value in sophiadb");
		return MDHIM_DB_ERROR;
	}

	return MDHIM_SUCCESS;

}
int mdhim_sophia_get(void *dbh, void *key, int key_len, void **data, int32_t *data_len, 
		     struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}
int mdhim_sophia_get_next(void *dbh, void **key, int *key_len, 
			  void **data, int32_t *data_len,
			  struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_get_prev(void *dbh, void **key, int *key_len, 
			  void **data, int32_t *data_len,
			  struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_del(void *dbh, void *key, int key_len, 
		     struct mdhim_store_opts_t *mstore_opts) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_commit(void *dbh) {
	return MDHIM_SUCCESS;
}

int mdhim_sophia_batch_put(void *dbh, void **key, int32_t *key_lens, 
			   void **data, int32_t *data_lens, int num_records,
			   struct mdhim_store_opts_t *mstore_opts) {
	int i;
	int rc;

	for (i = 0; i < num_records; i++) {
		rc = mdhim_sophia_put(dbh, key[i], key_lens[i], data[i], data_lens[i], 
				      mstore_opts);
		if (rc != MDHIM_SUCCESS) {
			return rc;
		}
	}

	return MDHIM_SUCCESS;
}

int mdhim_sophia_close(void *dbh, void *dbs, struct mdhim_store_opts_t *mstore_opts) {
	sp_destroy(dbh);
	sp_destroy(dbs);
	sp_destroy(mstore_opts->db_ptr1);
	sp_destroy(mstore_opts->db_ptr2);

	return MDHIM_SUCCESS;
}
